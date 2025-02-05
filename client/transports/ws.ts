import { WebSocket } from "@ayonli/jsext/ws"
import { parseProxyRequest } from "../../common.ts"
import { getConfig } from "../util.ts"
import chat from "../chat.ts"

let connectedBefore = false

export default function wsConnect() {
    const { serverUrl, agentId } = getConfig()
    const ws = new WebSocket(serverUrl + "/ws?agentId=" + agentId)

    ws.addEventListener("open", () => {
        connectedBefore = true
        console.log("Connected to the server")
    })

    ws.addEventListener("error", () => {
        if (ws.readyState === WebSocket.CONNECTING ||
            (ws.readyState === WebSocket.CLOSED && !connectedBefore)
        ) {
            console.log("Failed to connect to the server, will retry in 5 seconds")
            setTimeout(() => {
                connectedBefore = false
                console.log("Reconnecting...")
                wsConnect()
            }, 5000)
        }
    })

    ws.addEventListener("close", () => {
        if (connectedBefore) {
            console.log("Disconnected from the server")
            setTimeout(() => {
                connectedBefore = false
                console.log("Reconnecting...")
                wsConnect()
            }, 0)
        }
    })

    ws.addEventListener("message", event => {
        const result = parseProxyRequest(event.data)
        if (!result.ok) {
            console.error(result.error)
            return
        }

        chat(result.value, response => {
            ws.send(JSON.stringify(response))
        }).catch(console.error)
    })
}
