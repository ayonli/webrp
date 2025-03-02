import { asyncTask, AsyncTask, sleep } from "@ayonli/jsext/async"
import { try_ } from "@ayonli/jsext/result"
import { unrefTimer } from "@ayonli/jsext/runtime"
import { toWebSocketStream, WebSocket } from "@ayonli/jsext/ws"
import { pack, unpack } from "msgpackr"
import {
    ProxyRequestAbortFrame,
    ProxyRequestBodyFrame,
    ProxyRequestFrame,
    ProxyRequestHeaderFrame,
    ProxyResponseBodyFrame,
    ProxyResponseHeaderFrame,
} from "../header.ts"

export interface ProxyClientOptions {
    clientId: string
    remoteUrl: string
    localUrl: string
    connToken?: string
    pingInterval?: number
    logPrefix?: string
}

export default class ProxyClient {
    private clientId: string
    private remoteUrl: string
    private localUrl: string
    readonly connToken: string | undefined
    private logPrefix: string
    private socket: WebSocket | null = null
    private connectedBefore = false
    private lastActive = 0
    private requestWriters = new Map<string, WritableStreamDefaultWriter>()
    private requestControllers = new Map<string, AbortController>()
    private pingTask: AsyncTask<void> | null = null
    private pingInterval: number
    private healthChecker: number | NodeJS.Timeout

    constructor(options: ProxyClientOptions) {
        const { clientId, remoteUrl, localUrl, connToken, pingInterval, logPrefix } = options
        this.clientId = clientId
        this.remoteUrl = remoteUrl
        this.localUrl = localUrl
        this.connToken = connToken
        this.logPrefix = logPrefix ?? ""
        this.pingInterval = pingInterval ?? 30_000
        this.healthChecker = setInterval(() => {
            if (this.socket &&
                !this.pingTask &&
                this.lastActive && Date.now() - this.lastActive >= this.pingInterval
            ) {
                // long time without any activity, send ping.
                this.ping().catch(console.error)
            }
        }, 1_000)

        unrefTimer(this.healthChecker)
    }

    private log(msg: string, ...args: unknown[]) {
        console.log(this.logPrefix + msg, ...args)
    }

    private logError(msg: string, ...args: unknown[]) {
        console.error(this.logPrefix + msg, ...args)
    }

    private async ping(): Promise<void> {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN)
            return

        this.socket.send("ping")
        this.pingTask = asyncTask()
        const timeout = await Promise.any([
            this.pingTask.then(() => false),
            sleep(5_000).then(() => true),
        ])
        this.pingTask = null

        if (timeout) {
            // The server is not responding to the ping, close the connection
            // for reconnection.
            this.socket.close()
            return
        }

        const url = new URL("__ping__", this.remoteUrl)
        url.searchParams.set("clientId", this.clientId)

        const result = await try_(fetch(url, {
            signal: AbortSignal.timeout(5_000),
        }))
        if (!result.ok) {
            // The server is not responding to the ping, close the connection
            // for reconnection.
            this.socket.close()
            return
        }

        const response = result.value
        if (!response.ok) {
            return // The server doesn't support the ping endpoint.
        }

        const { ok, code } = await result.value.json() as {
            ok: boolean
            code: number
            message: string
        }

        if (!ok && code === 404) {
            // The server has lost the client, usually because of a redeployment,
            // close the connection for reconnection.
            this.socket.close()
        }
    }

    connect() {
        const url = new URL("/__connect__", this.remoteUrl)
        url.searchParams.set("clientId", this.clientId)

        if (this.connToken) {
            url.searchParams.set("token", this.connToken)
        }

        const socket = this.socket = new WebSocket(url)

        socket.binaryType = "arraybuffer"
        socket.addEventListener("open", () => {
            this.log("Connected to the server")
            this.connectedBefore = true
            this.lastActive = Date.now()
        })

        socket.addEventListener("error", (ev) => {
            if ((ev as ErrorEvent).message?.includes("401")) {
                this.logError("Failed to connect to the server, unauthorized")
                socket.close()
            } else if (socket.readyState === WebSocket.CONNECTING ||
                (socket.readyState === WebSocket.CLOSED && !this.connectedBefore)
            ) {
                this.log("Failed to connect to the server, will retry in 5 seconds")
                setTimeout(() => {
                    this.log("Reconnecting...")
                    this.connectedBefore = false
                    this.connect()
                }, 5_000)
            }
        })

        socket.addEventListener("close", () => {
            if (this.connectedBefore) {
                this.log("Disconnected from the server")
                setTimeout(() => {
                    this.log("Reconnecting...")
                    this.connectedBefore = false
                    this.connect()
                }, 0)
            }
        })

        socket.addEventListener("message", event => {
            this.lastActive = Date.now()

            if (event.data === "pong" ||
                // BUG report: Node.js WebSocket sometimes receives null when
                // Deno/Bun sends "pong", usually after a long time without any
                // activity.
                (event.data === null && this.pingTask)
            ) {
                this.log("Pong from the server")
                this.pingTask?.resolve()
                return
            } else if (typeof event.data === "string") {
                return
            }

            const frame = unpack(new Uint8Array(event.data as ArrayBuffer))

            if (typeof frame !== "object" &&
                (!frame || typeof frame.type !== "string" || typeof frame.requestId !== "string")
            ) {
                return
            }

            this.processRequestMessage(frame)
        })
    }

    private async processRequestMessage(
        frame: ProxyRequestFrame | ProxyRequestHeaderFrame | ProxyRequestBodyFrame | ProxyRequestAbortFrame
    ) {
        if (frame.type === "request" || frame.type === "header") {
            const { requestId, method, path, headers: _headers } = frame
            const url = new URL(path, this.localUrl)
            const headers = new Headers(_headers)

            if (headers.get("x-forwarded-host")) {
                // The original host is set in the `x-forwarded-host` header, we
                // should set the `host` header to the target host.
                headers.set("host", url.host)
            }

            if (method === "GET" &&
                headers.get("connection") === "Upgrade" &&
                headers.get("upgrade") === "websocket"
            ) {
                return this.processWebSocketRequest(requestId, url, headers)
            }

            const controller = new AbortController()
            this.requestControllers.set(requestId, controller)
            controller.signal.addEventListener("abort", () => {
                this.requestControllers.delete(requestId)
            })

            const reqInit: RequestInit = {
                method,
                headers,
                signal: controller.signal,
                // @ts-ignore for Node.js
                duplex: "half",
            }

            if (frame.type === "request" && frame.body) {
                reqInit.body = frame.body
            } else if (frame.type === "header" && !frame.eof) {
                const { readable, writable } = new TransformStream()
                const writer = writable.getWriter()
                this.requestWriters.set(requestId, writer)
                reqInit.body = readable
            }

            const req = new Request(url, reqInit)
            const result = await try_(fetch(req))

            if (!result.ok) {
                this.requestControllers.delete(requestId)
                return this.respond({
                    requestId,
                    type: "header",
                    status: 502,
                    statusText: "Bad Gateway",
                    headers: [],
                    eof: true,
                })
            }

            const res = result.value

            this.respond({
                requestId,
                type: "header",
                status: res.status,
                statusText: res.statusText,
                headers: Array.from(res.headers.entries()).filter(([field]) => {
                    // The stream of response body is always decompressed.
                    // We need to remove the `content-encoding` header if present.
                    return field !== "content-encoding"
                }),
                eof: !res.body,
            })

            if (res.body) {
                const reader = res.body.getReader()
                while (true) {
                    try {
                        const { done, value } = await reader.read()
                        this.respond({
                            requestId,
                            type: "body",
                            data: value,
                            eof: done,
                        })

                        if (done) {
                            break
                        }
                    } catch {
                        // Happens when the connection is closed prematurely.
                        this.respond({
                            requestId,
                            type: "body",
                            data: undefined,
                            eof: true,
                        })
                        break
                    }
                }
            }

            this.requestControllers.delete(requestId)
        } else if (frame.type === "body") {
            const { requestId, data, eof } = frame
            const writer = this.requestWriters.get(requestId)

            if (!writer) {
                return
            }

            if (eof) {
                this.requestWriters.delete(requestId)
                writer.close().catch(console.error)
            } else if (data !== undefined) {
                writer.write(data).then(() => {
                    this.lastActive = Date.now()
                }).catch(console.error)
            }
        } else if (frame.type === "abort") {
            const controller = this.requestControllers.get(frame.requestId)
            if (controller) {
                controller.abort()
            }
        }
    }

    private async processWebSocketRequest(requestId: string, url: URL, headers: Headers) {
        const protocols = headers.get("sec-websocket-protocol") ?? undefined
        const upstreamPort = toWebSocketStream(new WebSocket(url, protocols))

        const proxyUrl = new URL("/__ws__", this.remoteUrl)
        proxyUrl.searchParams.set("clientId", this.clientId)
        proxyUrl.searchParams.set("requestId", requestId)
        if (this.connToken) {
            proxyUrl.searchParams.set("token", this.connToken)
        }

        const downstreamPort = toWebSocketStream(new WebSocket(proxyUrl))

        const {
            readable: upstreamIncoming,
            writable: upstreamOutgoing,
        } = await upstreamPort.opened
        const {
            readable: downstreamIncoming,
            writable: downstreamOutgoing,
        } = await downstreamPort.opened

        upstreamIncoming.pipeTo(downstreamOutgoing)
        downstreamIncoming.pipeTo(upstreamOutgoing)

        upstreamPort.closed.then(() => {
            // deno-lint-ignore no-empty
            try { downstreamPort.close() } catch { }
        })
        downstreamPort.closed.then(() => {
            // deno-lint-ignore no-empty
            try { upstreamPort.close() } catch { }
        })
    }

    private respond(frame: ProxyResponseHeaderFrame | ProxyResponseBodyFrame) {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            return
        }

        this.socket.send(pack(frame))
        this.lastActive = Date.now()
    }
}
