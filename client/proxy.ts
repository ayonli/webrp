import { asyncTask, AsyncTask, sleep } from "@ayonli/jsext/async"
import { Result } from "@ayonli/jsext/result"
import { env, unrefTimer } from "@ayonli/jsext/runtime"
import { toWebSocketStream, WebSocket } from "@ayonli/jsext/ws"
import { pack, unpack } from "msgpackr"
import {
    ProxyRequestBodyFrame,
    ProxyRequestHeaderFrame,
    ProxyResponseBodyFrame,
    ProxyResponseHeaderFrame,
} from "../header.ts"

export function getConfig() {
    const clientId = env("CLIENT_ID")
    const remoteUrl = env("REMOTE_URL")
    const localUrl = env("LOCAL_URL")
    const connToken = env("CONN_TOKEN")
    const pingInterval = Math.max(Number(env("PING_INTERVAL") || "30"), 5) * 1_000

    if (!clientId || !remoteUrl || !localUrl) {
        throw new Error("CLIENT_ID, REMOTE_URL and LOCAL_URL must be set")
    }

    return { clientId, remoteUrl, localUrl, connToken, pingInterval }
}

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
    private logPrefix: string | undefined
    private socket: WebSocket | null = null
    private connectedBefore = false
    private lastActive = 0
    private pingTask: AsyncTask<void> | null = null
    private pingInterval: number
    private healthChecker: number | NodeJS.Timeout
    private requests = new Map<string, WritableStreamDefaultWriter>()

    constructor(options: ProxyClientOptions) {
        const { clientId, remoteUrl, localUrl, connToken, pingInterval, logPrefix } = options
        this.clientId = clientId
        this.remoteUrl = remoteUrl
        this.localUrl = localUrl
        this.connToken = connToken
        this.logPrefix = logPrefix
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

        const result = await Result.try(fetch(url, {
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

    private async processRequestMessage(frame: ProxyRequestHeaderFrame | ProxyRequestBodyFrame) {
        if (frame.type === "header") {
            const { requestId, method, path, headers: _headers, eof } = frame
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

            const reqInit: RequestInit = {
                method,
                headers,
                // @ts-ignore for Node.js
                duplex: "half",
            }

            if (!eof) {
                const { readable, writable } = new TransformStream()
                const writer = writable.getWriter()
                this.requests.set(requestId, writer)
                reqInit.body = readable
            }

            const req = new Request(url, reqInit)
            const result = await Result.try(fetch(req))

            if (!result.ok) {
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
                        break
                    }
                }
            }
        } else if (frame.type === "body") {
            const { requestId, data, eof } = frame
            const writer = this.requests.get(requestId)

            if (!writer) {
                return
            }

            if (eof) {
                this.requests.delete(requestId)
                writer.close().catch(console.error)
            } else if (data !== undefined) {
                writer.write(new Uint8Array(data)).catch(console.error)
            }
        }
    }

    private async processWebSocketRequest(requestId: string, url: URL, headers: Headers) {
        const protocols = headers.get("sec-websocket-protocol") ?? undefined
        const socket = new WebSocket(url, protocols)

        const server = toWebSocketStream(socket)
        const proxyUrl = new URL("/__ws__", this.remoteUrl)
        proxyUrl.searchParams.set("clientId", this.clientId)
        proxyUrl.searchParams.set("requestId", requestId)

        if (this.connToken) {
            proxyUrl.searchParams.set("token", this.connToken)
        }

        const client = toWebSocketStream(new WebSocket(proxyUrl))

        const {
            readable: serverReadable,
            writable: serverWritable,
        } = await server.opened
        const {
            readable: clientReadable,
            writable: clientWritable,
        } = await client.opened

        serverReadable.pipeTo(clientWritable)
        clientReadable.pipeTo(serverWritable)

        server.closed.then(() => {
            // deno-lint-ignore no-empty
            try { client.close() } catch { }
        })
        client.closed.then(() => {
            // deno-lint-ignore no-empty
            try { server.close() } catch { }
        })
    }

    private respond(frame: ProxyResponseHeaderFrame | ProxyResponseBodyFrame) {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            return
        }

        const buf = pack(frame)
        this.socket.send(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength))
    }
}
