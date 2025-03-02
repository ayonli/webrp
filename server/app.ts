import { asyncTask, AsyncTask, sleep } from "@ayonli/jsext/async"
import { crc32 } from "@ayonli/jsext/hash"
import { RequestContext } from "@ayonli/jsext/http"
import { serial } from "@ayonli/jsext/number"
import runtime, { addUnhandledRejectionListener, env } from "@ayonli/jsext/runtime"
import { stripStart } from "@ayonli/jsext/string"
import {
    toWebSocketStream,
    WebSocketConnection,
    WebSocketServer,
    WebSocketStream,
} from "@ayonli/jsext/ws"
import { Hono } from "hono"
import { pack, unpack } from "msgpackr"
import {
    ProxyRequestAbortFrame,
    ProxyRequestBodyFrame,
    ProxyRequestFrame,
    ProxyRequestHeaderFrame,
    ProxyResponseBodyFrame,
    ProxyResponseHeaderFrame,
} from "../header.ts"

addUnhandledRejectionListener(ev => {
    ev.preventDefault()
    console.error("Unhandled rejection:", ev.reason)
})

let authRule: RegExp | null = null

function createAuthRule(AUTH_RULE: string) {
    if (AUTH_RULE.startsWith("/")) {
        const lastIndex = AUTH_RULE.lastIndexOf("/")
        if (lastIndex > 1) {
            const pattern = AUTH_RULE.slice(1, lastIndex)
            let flags = AUTH_RULE.slice(lastIndex + 1)

            if (flags && flags !== "i") {
                console.warn("Only 'i' flag is supported in AUTH_RULE.")
                if (flags.includes("i")) {
                    flags = "i"
                } else {
                    flags = ""
                }
            }

            return new RegExp(pattern, flags || undefined)
        }

        return new RegExp(AUTH_RULE)
    } else {
        return new RegExp(AUTH_RULE)
    }
}

function passAuth(path: string) {
    return authRule ? !authRule.test(path) : false
}

const idPool = serial(true)

function nextId() {
    return idPool.next().value!.toString(32)
}

const wsServer = new WebSocketServer()

type ClientRecord = {
    socket: WebSocketConnection
    requests: Set<string>
    responses: Set<string>
}
const clients: Record<string, ClientRecord | null> = {}

const requestTasks = new Map<string, AsyncTask<Response | WebSocketStream>>()
const responseWriters = new Map<string, WritableStreamDefaultWriter<Uint8Array>>()

function processResponseMessage(
    frame: ProxyResponseHeaderFrame | ProxyResponseBodyFrame,
    clientId: string
) {
    if (frame.type === "header") {
        const { requestId, status, statusText, headers, eof } = frame
        const task = requestTasks.get(requestId)

        if (!task) {
            return
        }

        if (eof) {
            const res = new Response(null, {
                status,
                statusText,
                headers: new Headers(headers),
            })
            task.resolve(res)
        } else {
            const { readable, writable } = new TransformStream()
            const res = new Response(readable, {
                status,
                statusText,
                headers: new Headers(headers),
            })

            const writer = writable.getWriter()
            responseWriters.set(requestId, writer)

            const client = clients[clientId]
            client?.responses.add(requestId)

            task.resolve(res)
        }
    } else if (frame.type === "body") {
        const { requestId, data, eof } = frame
        const writer = responseWriters.get(requestId)

        if (!writer) {
            return
        }

        if (eof) {
            responseWriters.delete(requestId)
            writer.close().catch(() => { })

            const client = clients[clientId]
            client?.responses.delete(requestId)
        } else if (data !== undefined) {
            writer.write(new Uint8Array(data)).catch(console.error)
        }
    }
}

const app = new Hono<{ Bindings: any }>()
    // An endpoint is for the client to connect to the server using WebSocket.
    .get("/__connect__", ctx => {
        if (runtime().identity === "workerd") {
            env(ctx.env) // initialize the environment for the worker
        }

        const { AUTH_RULE } = env()
        if (AUTH_RULE) {
            authRule ??= createAuthRule(AUTH_RULE)
        }

        const { searchParams } = new URL(ctx.req.url)
        const clientId = searchParams.get("clientId")
        if (!clientId) {
            return ctx.text("Client ID is missing.", { status: 400 })
        }

        const auth = ctx.req.query("token") || ""
        const { CONN_TOKEN } = env()
        if (CONN_TOKEN && auth !== CONN_TOKEN) {
            return new Response("Unauthorized", {
                status: 401,
                statusText: "Unauthorized",
            })
        }

        const { response, socket } = wsServer.upgrade(ctx.req.raw)

        socket.addEventListener("open", () => {
            clients[clientId] = {
                socket,
                requests: new Set(),
                responses: new Set(),
            }
            console.log("Client connected:", clientId)
        })
        socket.addEventListener("message", event => {
            if (event.data === "ping") {
                console.log("Ping from client:", clientId)
                socket.send("pong")
                return
            } else if (typeof event.data === "string") {
                return
            }

            const frame = unpack(event.data)
            if (typeof frame !== "object" &&
                (!frame || typeof frame.type !== "string" || typeof frame.requestId !== "string")
            ) {
                return
            }

            processResponseMessage(frame, clientId)
        })
        socket.addEventListener("close", () => {
            console.log("Client disconnected:", clientId)
            const { requests, responses } = clients[clientId]!

            // Respond to all pending requests with 500 Internal Server Error.
            requests.forEach(requestId => {
                const task = requestTasks.get(requestId)
                task?.resolve(new Response(null, {
                    status: 500,
                    statusText: "Internal Server Error",
                }))
            })

            // Close all ongoing responses.
            responses.forEach(requestId => {
                const writer = responseWriters.get(requestId)
                writer?.close().catch(() => { })
            })

            // Set the client record to null. This will remove the client from
            // the list of available clients, but keep the client ID untouched,
            // so that when the client reconnects, it will be assigned to the
            // same position.
            clients[clientId] = null
        })

        return response
    })

    // An endpoint is for the client to check the server's availability.
    .get("/__ping__", ctx => {
        const { searchParams } = new URL(ctx.req.url)
        const clientId = searchParams.get("clientId")
        if (!clientId) {
            return ctx.json({
                ok: false,
                code: 400,
                message: "Client ID is missing.",
            })
        }

        const socket = clients[clientId]
        if (!socket) {
            return ctx.json({
                ok: false,
                code: 404,
                message: "Client not found.",
            })
        }

        return ctx.json({ ok: true, code: 200, message: "pong" })
    })

    // An endpoint is for WebSocket proxy.
    .get("/__ws__", ctx => {
        const { searchParams } = new URL(ctx.req.url)
        const clientId = searchParams.get("clientId")
        if (!clientId) {
            return ctx.text("Client ID is missing.", { status: 400 })
        }

        const requestId = searchParams.get("requestId")
        if (!requestId) {
            return ctx.text("Request ID is missing.", { status: 400 })
        }

        const auth = ctx.req.query("token") || ""
        const { CONN_TOKEN } = env()
        if (CONN_TOKEN && auth !== CONN_TOKEN) {
            return ctx.text("Unauthorized", {
                status: 401,
                statusText: "Unauthorized",
            })
        }

        const task = requestTasks.get(requestId)
        if (!task) {
            return ctx.text("Request not found.", { status: 404 })
        }

        const { response, socket } = wsServer.upgrade(ctx.req.raw)
        const stream = toWebSocketStream(socket)
        task.resolve(stream)

        return response
    })

    // Proxy all requests to the client.
    .all("/*", async ctx => {
        const respondBody = !["HEAD", "OPTIONS"].includes(ctx.req.method)

        let auth = ctx.req.header("x-auth-token")
            || ctx.req.header("authorization")
        auth &&= stripStart(auth, "Bearer ")

        const { AUTH_TOKEN } = env()
        if (AUTH_TOKEN && auth !== AUTH_TOKEN && !passAuth(ctx.req.path)) {
            return new Response(respondBody ? "Unauthorized" : null, {
                status: 401,
                statusText: "Unauthorized",
            })
        }

        const _clients = Object.values(clients).filter(Boolean) as ClientRecord[]

        if (!_clients.length) {
            return new Response(respondBody ? "No proxy client" : null, {
                status: 503,
                statusText: "Service Unavailable",
            })
        }

        let ip = ctx.req.header("x-forwarded-for")
        if (!ip) {
            if ("remoteAddress" in ctx.env) {
                ip = (ctx.env as RequestContext)?.remoteAddress?.hostname
            } else if ("remoteAddr" in ctx.env) {
                ip = (ctx.env as Deno.ServeHandlerInfo<Deno.NetAddr>)?.remoteAddr?.hostname
            }

            ip ||= ""
        }
        const modId = crc32(ip) % _clients.length
        const client = _clients[modId]
        const requestId = nextId()
        const req = ctx.req.raw
        const { protocol, host, pathname, search } = new URL(req.url)
        const headers = new Headers(req.headers.entries())

        if (ip && !headers.has("x-forwarded-for")) {
            headers.set("x-forwarded-for", ip)
        }

        if (!headers.has("x-forwarded-proto")) {
            headers.set("x-forwarded-proto", protocol.slice(0, -1))
        }

        const { FORWARD_HOST } = env()
        if (!(FORWARD_HOST?.toLowerCase().match(/^(true|on|1)$/)) &&
            !headers.has("x-forwarded-host")
        ) {
            headers.set("x-forwarded-host", host)
        } else if (!headers.has("host")) {
            headers.set("host", host)
        }

        const task = asyncTask<Response | WebSocketStream>()

        requestTasks.set(requestId, task)
        client.requests.add(requestId)

        const { BUFFER_REQUEST } = env()
        if (BUFFER_REQUEST?.toLowerCase().match(/^(true|on|1)$/)) {
            // Read all the request body before sending the request to the proxy
            // client.
            // This is not recommended as it can cause high memory usage and
            // will prevent the HTTP transaction from supporting full duplex
            // communication.
            const request: ProxyRequestFrame = {
                requestId,
                type: "request",
                method: req.method,
                path: pathname + search,
                headers: [...headers.entries()],
                body: req.body ? new Uint8Array(await req.arrayBuffer()) : undefined,
            }
            client.socket.send(pack(request))
        } else {
            const header: ProxyRequestHeaderFrame = {
                requestId,
                type: "header",
                method: req.method,
                path: pathname + search,
                headers: [...headers.entries()],
                eof: !req.body,
            }
            client.socket.send(pack(header))

            if (req.body) {
                // Transfer the request body asynchronously, so that the response
                // can be processed in parallel.
                (async () => {
                    const reader = req.body!.getReader()
                    while (true) {
                        try {
                            const { done, value } = await reader.read()
                            const body: ProxyRequestBodyFrame = {
                                requestId,
                                type: "body",
                                data: value,
                                eof: done,
                            }

                            client.socket.send(pack(body))

                            if (done) {
                                break
                            }
                        } catch { // request aborted or stream error
                            break
                        }
                    }
                })().catch(console.error)
            }
        }

        req.signal.addEventListener("abort", () => {
            client.socket.send(pack({
                requestId,
                type: "abort",
            } satisfies ProxyRequestAbortFrame))
        })

        const res = await Promise.any([task, sleep(30_000)])
        requestTasks.delete(requestId)
        client.requests.delete(requestId)

        if (res instanceof Response) {
            return res
        }

        if (res instanceof WebSocketStream) {
            const upstreamPort = res
            const { response, socket } = wsServer.upgrade(ctx.req.raw)
            const downstreamPort = toWebSocketStream(socket);

            (async () => {
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
            })()

            return response
        }

        return new Response(respondBody ? "Proxy client timeout" : null, {
            status: 504,
            statusText: "Gateway Timeout",
        })
    })

export default app
