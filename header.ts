export interface ProxyFrame {
    requestId: string
    eof: boolean
}

export interface ProxyRequestHeaderFrame extends ProxyFrame {
    type: "header"
    method: string
    path: string
    headers: [string, string][]
}

export interface ProxyRequestBodyFrame extends ProxyFrame {
    type: "body"
    data?: Uint8Array
}

export interface ProxyRequestFrame {
    requestId: string
    type: "request"
    method: string
    path: string
    headers: [string, string][]
    body?: Uint8Array
}

export interface ProxyRequestAbortFrame {
    requestId: string
    type: "abort"
}

export interface ProxyResponseHeaderFrame extends ProxyFrame {
    type: "header"
    status: number
    statusText: string
    headers: [string, string][]
}

export interface ProxyResponseBodyFrame extends ProxyFrame {
    type: "body"
    data?: Uint8Array
}
