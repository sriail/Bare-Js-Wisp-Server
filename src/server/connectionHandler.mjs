import { connect } from "cloudflare:sockets";
import { packet_types, stream_types } from "./protocol.js";

// Helper to find the end of HTTP headers (\r\n\r\n)
function findHeaderEnd(buf) {
  for (let i = 0; i <= buf.length - 4; i++) {
    if (buf[i] === 13 && buf[i+1] === 10 && buf[i+2] === 13 && buf[i+3] === 10) {
      return i;
    }
  }
  return -1;
}

export class ConnectionHandler {
  constructor(ws) {
    this.ws = ws;
    this.streams = new Map();
    this.init();
  }

  async init() {
    // Send initial CONTINUE packet (Stream ID: 0, Buffer Remaining: 8)
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
    const port = payload[1] | (payload[2] << 8);
    const hostname = new TextDecoder().decode(payload.subarray(3));

    if (streamType === stream_types.UDP) {
      this.sendClose(streamId, 0x41);
      return;
    }

    // Cloudflare Workers block TCP connect() on ports 80 and 443.
    // We must intercept these and use fetch() instead.
    if (port === 80 || port === 443) {
      this.setupHttpStream(streamId, hostname, port);
      return;
    }

    // Standard TCP logic for non-HTTP ports
    try {
      const tcpSocket = connect({ hostname, port });
      await tcpSocket.opened;
      
      const writer = tcpSocket.writable.getWriter();
      const reader = tcpSocket.readable.getReader();

      const stream = {
        type: 'tcp',
        socket: tcpSocket,
        writer,
        reader,
        closed: false,
      };
      this.streams.set(streamId, stream);

      this.startTcpReadLoop(streamId, stream);
      this.sendContinue(streamId, 8);
    } catch (e) {
      console.error("TCP Connect error:", e.message || e);
      this.sendClose(streamId, 0x44);
    }
  }

  setupHttpStream(streamId, hostname, port) {
    const stream = {
      type: 'http',
      hostname: hostname,
      port: port,
      fetchInitiated: false,
      headerBuffer: new Uint8Array(0),
      bodyController: null,
      bodyStream: null,
      closed: false,
    };
    
    // Create ReadableStream AFTER stream is defined to prevent null reference
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

    if (stream.type === 'http') {
      await this.handleHttpData(streamId, stream, payload);
      return;
    }

    // Standard TCP Data
    try {
      await stream.writer.ready;
      await stream.writer.write(payload);
      this.sendContinue(streamId, 8);
    } catch (e) {
      console.error("TCP Data write error:", e.message || e);
      this.sendClose(streamId, 0x03);
      this.cleanupStream(streamId);
    }
  }

  async handleHttpData(streamId, stream, payload) {
    if (stream.closed) return;

    // If fetch already started, pipe data into the fetch body stream
    if (stream.fetchInitiated) {
      stream.bodyController.enqueue(payload);
      this.sendContinue(streamId, 8);
      return;
    }

    // Otherwise, buffer until we find the end of HTTP headers
    const newBuffer = new Uint8Array(stream.headerBuffer.length + payload.length);
    newBuffer.set(stream.headerBuffer, 0);
    newBuffer.set(payload, stream.headerBuffer.length);
    stream.headerBuffer = newBuffer;

    const headerEnd = findHeaderEnd(stream.headerBuffer);
    if (headerEnd !== -1) {
      stream.fetchInitiated = true;
      
      // Enqueue any body data that came with the headers
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
        method: method,
        headers: headers,
        redirect: 'manual' // Pass redirects back to the client raw
      };

      if (method !== "GET" && method !== "HEAD") {
        fetchOptions.body = stream.bodyStream;
      } else {
        stream.bodyController.close(); // Close unused body stream
      }

      const response = await fetch(url, fetchOptions);

      // Construct raw HTTP response
      let rawResponse = `HTTP/1.1 ${response.status} ${response.statusText}\r\n`;
      response.headers.forEach((value, key) => {
        rawResponse += `${key}: ${value}\r\n`;
      });
      rawResponse += "\r\n";

      // Send headers back to client
      const headerBytes = new TextEncoder().encode(rawResponse);
      this.sendDataPacket(streamId, headerBytes);

      // Stream the body back to client
      if (response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          this.sendDataPacket(streamId, value);
        }
      }

      stream.bodyController.close();
      this.sendClose(streamId, 0x02); // Voluntary closure
      this.cleanupStream(streamId);

    } catch (e) {
      console.error("Fetch error:", e.message);
      try { stream.bodyController.error(e); } catch(err) {}
      this.sendClose(streamId, 0x44); // Connection refused
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
      this.sendClose(streamId, 0x03); 
      this.cleanupStream(streamId);
      return;
    }
    this.sendClose(streamId, 0x02); 
    this.cleanupStream(streamId);
  }

  handleClose(streamId) {
    this.cleanupStream(streamId);
  }

  cleanupStream(streamId) {
    const stream = this.streams.get(streamId);
    if (stream) {
      stream.closed = true;
      if (stream.type === 'tcp') {
        stream.writer.close().catch(() => {});
      } else if (stream.type === 'http') {
        try { stream.bodyController.close(); } catch(e) {}
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
    
    try { this.ws.send(out.buffer); } catch (e) {}
  }

  sendClose(streamId, reason) {
    const buf = new Uint8Array(6);
    const view = new DataView(buf.buffer);
    buf[0] = packet_types.CLOSE;
    view.setUint32(1, streamId, true);
    buf[5] = reason;
    try { this.ws.send(buf.buffer); } catch (e) {}
  }

  sendContinue(streamId, remaining) {
    const buf = new Uint8Array(9);
    const view = new DataView(buf.buffer);
    buf[0] = packet_types.CONTINUE;
    view.setUint32(1, streamId, true);
    view.setUint32(5, remaining, true);
    try { this.ws.send(buf.buffer); } catch (e) {}
  }

  onClose() {
    for (const id of this.streams.keys()) {
      this.cleanupStream(id);
    }
    this.streams.clear();
  }
}
