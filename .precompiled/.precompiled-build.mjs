// Stable, Precompiled File (Will not be updated as reguarly, but will remain stable)
// Developed By: Sriail (Simple, Modern Wisp Server Implamentation)
var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/server/connectionHandler.mjs
import { connect } from "cloudflare:sockets";

// src/server/protocol.js
var packet_types = {
  CONNECT: 1,
  DATA: 2,
  CONTINUE: 3,
  CLOSE: 4
};
var stream_types = {
  TCP: 1,
  UDP: 2
};

// src/server/connectionHandler.mjs
function findHeaderEnd(buf) {
  for (let i = 0; i <= buf.length - 4; i++) {
    if (buf[i] === 13 && buf[i + 1] === 10 && buf[i + 2] === 13 && buf[i + 3] === 10) {
      return i;
    }
  }
  return -1;
}
__name(findHeaderEnd, "findHeaderEnd");
var ConnectionHandler = class {
  static {
    __name(this, "ConnectionHandler");
  }
  constructor(ws) {
    this.ws = ws;
    this.streams = /* @__PURE__ */ new Map();
    this.init();
  }
  async init() {
    this.sendContinue(0, 8);
    this.ws.addEventListener("message", (event) => {
      if (typeof event.data === "string") return;
      const buf = new Uint8Array(event.data);
      this.onMessage(buf).catch((err) => console.error("Handler error:", err));
    });
    this.ws.addEventListener("close", () => this.onClose());
    this.ws.addEventListener("error", () => this.onClose());
  }
  async onMessage(buf) {
    if (buf.length < 5) return;
    const view = new DataView(buf.buffer);
    const type = buf[0];
    const streamId = view.getUint32(1, true);
    const payload = buf.subarray(5);
    switch (type) {
      case packet_types.CONNECT:
        await this.handleConnect(streamId, payload);
        break;
      case packet_types.DATA:
        await this.handleData(streamId, payload);
        break;
      case packet_types.CLOSE:
        this.handleClose(streamId);
        break;
    }
  }
  async handleConnect(streamId, payload) {
    const streamType = payload[0];
    const port = payload[1] | payload[2] << 8;
    const hostname = new TextDecoder().decode(payload.subarray(3));
    if (streamType === stream_types.UDP) {
      this.sendClose(streamId, 65);
      return;
    }
    if (port === 80 || port === 443) {
      this.setupHttpStream(streamId, hostname, port);
      return;
    }
    try {
      const tcpSocket = connect({ hostname, port });
      await tcpSocket.opened;
      const writer = tcpSocket.writable.getWriter();
      const reader = tcpSocket.readable.getReader();
      const stream = {
        type: "tcp",
        socket: tcpSocket,
        writer,
        reader,
        closed: false
      };
      this.streams.set(streamId, stream);
      this.startTcpReadLoop(streamId, stream);
      this.sendContinue(streamId, 8);
    } catch (e) {
      console.error("TCP Connect error:", e.message || e);
      this.sendClose(streamId, 68);
    }
  }
  setupHttpStream(streamId, hostname, port) {
    const stream = {
      type: "http",
      hostname,
      port,
      fetchInitiated: false,
      headerBuffer: new Uint8Array(0),
      bodyController: null,
      bodyStream: null,
      closed: false
    };
    stream.bodyStream = new ReadableStream({
      start(controller) {
        stream.bodyController = controller;
      }
    });
    this.streams.set(streamId, stream);
    this.sendContinue(streamId, 8);
  }
  async handleData(streamId, payload) {
    const stream = this.streams.get(streamId);
    if (!stream) return;
    if (stream.type === "http") {
      await this.handleHttpData(streamId, stream, payload);
      return;
    }
    try {
      await stream.writer.ready;
      await stream.writer.write(payload);
      this.sendContinue(streamId, 8);
    } catch (e) {
      console.error("TCP Data write error:", e.message || e);
      this.sendClose(streamId, 3);
      this.cleanupStream(streamId);
    }
  }
  async handleHttpData(streamId, stream, payload) {
    if (stream.closed) return;
    if (stream.fetchInitiated) {
      stream.bodyController.enqueue(payload);
      this.sendContinue(streamId, 8);
      return;
    }
    const newBuffer = new Uint8Array(stream.headerBuffer.length + payload.length);
    newBuffer.set(stream.headerBuffer, 0);
    newBuffer.set(payload, stream.headerBuffer.length);
    stream.headerBuffer = newBuffer;
    const headerEnd = findHeaderEnd(stream.headerBuffer);
    if (headerEnd !== -1) {
      stream.fetchInitiated = true;
      const bodyStart = stream.headerBuffer.subarray(headerEnd + 4);
      if (bodyStart.length > 0) {
        stream.bodyController.enqueue(bodyStart);
      }
      stream.headerBuffer = stream.headerBuffer.subarray(0, headerEnd);
      await this.processHttpRequest(streamId, stream);
    }
  }
  async processHttpRequest(streamId, stream) {
    const rawRequest = new TextDecoder().decode(stream.headerBuffer);
    const lines = rawRequest.split("\r\n");
    const [method, path] = lines[0].split(" ");
    const headers = new Headers();
    for (let i = 1; i < lines.length; i++) {
      const colonIndex = lines[i].indexOf(":");
      if (colonIndex > 0) {
        const key = lines[i].substring(0, colonIndex).trim();
        const value = lines[i].substring(colonIndex + 1).trim();
        headers.set(key, value);
      }
    }
    if (!headers.get("Host")) {
      headers.set("Host", stream.hostname);
    }
    const protocol = stream.port === 443 ? "https:" : "http:";
    const url = `${protocol}//${stream.hostname}${path}`;
    try {
      const fetchOptions = {
        method,
        headers,
        redirect: "manual"
        // Pass redirects back to the client raw
      };
      if (method !== "GET" && method !== "HEAD") {
        fetchOptions.body = stream.bodyStream;
      } else {
        stream.bodyController.close();
      }
      const response = await fetch(url, fetchOptions);
      let rawResponse = `HTTP/1.1 ${response.status} ${response.statusText}\r
`;
      response.headers.forEach((value, key) => {
        rawResponse += `${key}: ${value}\r
`;
      });
      rawResponse += "\r\n";
      const headerBytes = new TextEncoder().encode(rawResponse);
      this.sendDataPacket(streamId, headerBytes);
      if (response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          this.sendDataPacket(streamId, value);
        }
      }
      stream.bodyController.close();
      this.sendClose(streamId, 2);
      this.cleanupStream(streamId);
    } catch (e) {
      console.error("Fetch error:", e.message);
      try {
        stream.bodyController.error(e);
      } catch (err) {
      }
      this.sendClose(streamId, 68);
      this.cleanupStream(streamId);
    }
  }
  async startTcpReadLoop(streamId, stream) {
    try {
      while (!stream.closed) {
        const { done, value } = await stream.reader.read();
        if (done || !value) break;
        const header = new Uint8Array(5);
        const headerView = new DataView(header.buffer);
        header[0] = packet_types.DATA;
        headerView.setUint32(1, streamId, true);
        const out = new Uint8Array(header.length + value.length);
        out.set(header, 0);
        out.set(value, header.length);
        this.ws.send(out.buffer);
      }
    } catch (e) {
      console.error("TCP Read loop error:", e.message || e);
      this.sendClose(streamId, 3);
      this.cleanupStream(streamId);
      return;
    }
    this.sendClose(streamId, 2);
    this.cleanupStream(streamId);
  }
  handleClose(streamId) {
    this.cleanupStream(streamId);
  }
  cleanupStream(streamId) {
    const stream = this.streams.get(streamId);
    if (stream) {
      stream.closed = true;
      if (stream.type === "tcp") {
        stream.writer.close().catch(() => {
        });
      } else if (stream.type === "http") {
        try {
          stream.bodyController.close();
        } catch (e) {
        }
      }
      this.streams.delete(streamId);
    }
  }
  sendDataPacket(streamId, data) {
    const header = new Uint8Array(5);
    const headerView = new DataView(header.buffer);
    header[0] = packet_types.DATA;
    headerView.setUint32(1, streamId, true);
    const out = new Uint8Array(header.length + data.length);
    out.set(header, 0);
    out.set(data, header.length);
    try {
      this.ws.send(out.buffer);
    } catch (e) {
    }
  }
  sendClose(streamId, reason) {
    const buf = new Uint8Array(6);
    const view = new DataView(buf.buffer);
    buf[0] = packet_types.CLOSE;
    view.setUint32(1, streamId, true);
    buf[5] = reason;
    try {
      this.ws.send(buf.buffer);
    } catch (e) {
    }
  }
  sendContinue(streamId, remaining) {
    const buf = new Uint8Array(9);
    const view = new DataView(buf.buffer);
    buf[0] = packet_types.CONTINUE;
    view.setUint32(1, streamId, true);
    view.setUint32(5, remaining, true);
    try {
      this.ws.send(buf.buffer);
    } catch (e) {
    }
  }
  onClose() {
    for (const id of this.streams.keys()) {
      this.cleanupStream(id);
    }
    this.streams.clear();
  }
};

// src/server/connections.mjs
var WispHandler = class {
  static {
    __name(this, "WispHandler");
  }
  static async handle(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    new ConnectionHandler(server);
    const responseHeaders = new Headers();
    const protocol = request.headers.get("Sec-WebSocket-Protocol");
    if (protocol) {
      const firstProtocol = protocol.split(",")[0].trim();
      responseHeaders.set("Sec-WebSocket-Protocol", firstProtocol);
    }
    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: responseHeaders
    });
  }
};

// src/server/wsproxy.mjs
import { connect as connect2 } from "cloudflare:sockets";
var WSProxyConnection = class {
  static {
    __name(this, "WSProxyConnection");
  }
  static async handle(request, path) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const target = path.split("/").pop().split(":");
    const hostname = target[0];
    const port = parseInt(target[1]);
    server.accept();
    (async () => {
      let writer, reader;
      try {
        const tcpSocket = connect2({ hostname, port });
        await tcpSocket.opened;
        writer = tcpSocket.writable.getWriter();
        reader = tcpSocket.readable.getReader();
        server.addEventListener("message", async (event) => {
          try {
            if (typeof event.data === "string") return;
            const buf = new Uint8Array(event.data);
            await writer.ready;
            await writer.write(buf);
          } catch (e) {
          }
        });
        server.addEventListener("close", async () => {
          try {
            if (writer) await writer.close();
          } catch (e) {
          }
        });
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          server.send(value.buffer);
        }
      } catch (e) {
      } finally {
        try {
          server.close();
        } catch (e) {
        }
      }
    })();
    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
};

// src/server/server.mjs
var HTML_PAGE = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <script>
  document.title = window.location.hostname.replace('www.', '');
<\/script>
</head>
<body>
  <h1>Wisp Server (V1 Protocol)</h1>
<div>
  <label>Wisp Server URL (Default of the Current Server): 
    <input type="text" id="serverUrl" value="" size="50">
  </label>
</div>
  <br>
  <div>
    <label>Target Web Address: <input type="text" id="targetUrl" value="http://example.com" size="50"></label>
  </div>
  <br>
  <button id="testBtn">Send Request</button>
  <hr>
  <h3>Response:</h3>
  <pre id="output" style="white-space: pre-wrap; word-wrap: break-word;"></pre>

<script type="module">
  import { ClientConnection } from "https://cdn.jsdelivr.net/gh/sriail/Bare-Js-Wisp-Server@refactor-proxy-for-wisp-js-client/src/client/index.js";

  // --- Dynamic Server URL Setup ---
  const serverInput = document.getElementById('serverUrl');
  if (serverInput) {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    serverInput.value = \`\${protocol}//\${window.location.host}/wisp/\`;
  }
  // ---------------------------------

  document.getElementById('testBtn').addEventListener('click', () => {
    const serverUrl = document.getElementById('serverUrl').value;
    const targetUrl = document.getElementById('targetUrl').value;
    const output = document.getElementById('output');
    
    if (!serverUrl.endsWith('/')) {
      output.textContent = "Error: Wisp Server URL must end with a trailing slash (/)";
      return;
    }

    output.textContent = "Connecting to " + serverUrl + "...\\n";
    
    try {
      const parsedTarget = new URL(targetUrl);
      const host = parsedTarget.hostname;
      const port = parsedTarget.port || (parsedTarget.protocol === 'https:' ? '443' : '80');
      const path = (parsedTarget.pathname + parsedTarget.search) || '/';
      
      // Explicitly use Wisp V1 to match our server implementation
      const conn = new ClientConnection(serverUrl, { wisp_version: 1 });
      
      conn.onopen = () => {
        output.textContent += "Connected! Creating stream to " + host + ":" + port + "\\n";
        const stream = conn.create_stream(host, parseInt(port));
        
        stream.onmessage = (raw_data) => {
          const text = new TextDecoder().decode(raw_data);
          output.textContent += text;
        };
        
        stream.onclose = (reason) => {
          output.textContent += "\\n--- Stream Closed (Reason: " + reason + ") ---\\n";
        };

        const httpRequest = "GET " + path + " HTTP/1.1\\r\\nHost: " + host + "\\r\\nConnection: close\\r\\nUser-Agent: WispTester/1.0\\r\\nAccept: */*\\r\\n\\r\\n";
        
        setTimeout(() => {
          stream.send(new TextEncoder().encode(httpRequest));
          output.textContent += "--- Request Sent ---\\n\\n";
        }, 100);
      };
      
      conn.onerror = (e) => {
        output.textContent += "\\n[ERROR] Connection error.\\n";
        console.error(e);
      };
      
      conn.onclose = () => {
        output.textContent += "\\n--- Wisp Connection Closed ---\\n";
      };

    } catch (err) {
      output.textContent += "\\n[ERROR] " + err.message + "\\n";
      console.error(err);
    }
  });
<\/script>
</body>
</html>`;
var WispServer = class {
  static {
    __name(this, "WispServer");
  }
  static async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get("Upgrade");
    const url = new URL(request.url);
    const path = url.pathname;
    if (upgradeHeader && upgradeHeader === "websocket") {
      if (path.endsWith("/")) {
        return WispHandler.handle(request);
      } else {
        return WSProxyConnection.handle(request, path);
      }
    }
    if (path === "/" || path === "/test" || path === "/index.html") {
      return new Response(HTML_PAGE, {
        headers: { "Content-Type": "text/html; charset=utf-8" }
      });
    }
    return new Response("Not Found", { status: 404 });
  }
};

// src/server/index.js
var index_default = {
  async fetch(request, env, ctx) {
    return WispServer.fetch(request, env, ctx);
  }
};
export {
  index_default as default
};
//# sourceMappingURL=index.js.map
