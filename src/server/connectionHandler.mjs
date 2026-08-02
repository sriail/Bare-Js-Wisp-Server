import { connect } from "cloudflare:sockets";
import { packet_types, stream_types } from "./protocol.js";

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
      // Cloudflare Workers expects ArrayBuffer
      let data = event.data;
      if (data instanceof ArrayBuffer) {
        // good
      } else if (ArrayBuffer.isView(data)) {
        data = new Uint8Array(data).buffer;
      } else if (typeof data === "string") {
        data = new TextEncoder().encode(data).buffer;
      }
      
      this.onMessage(data).catch((err) => console.error("Handler error:", err));
    });

    this.ws.addEventListener("close", () => this.onClose());
    this.ws.addEventListener("error", () => this.onClose());
  }

  async onMessage(data) {
    const buf = new Uint8Array(data);
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

    try {
      // Using string format for connect is more robust in CF Workers
      const tcpSocket = connect(`${hostname}:${port}`);
      
      const writer = tcpSocket.writable.getWriter();
      const reader = tcpSocket.readable.getReader();

      const stream = {
        socket: tcpSocket,
        writer,
        reader,
        closed: false,
      };
      this.streams.set(streamId, stream);

      this.startReadLoop(streamId, stream);
      this.sendContinue(streamId, 8);
    } catch (e) {
      console.error("Connect error:", e.message || e);
      this.sendClose(streamId, 0x44); // 0x44 - Connection refused
    }
  }

  async startReadLoop(streamId, stream) {
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
        
        // Strictly send ArrayBuffer to avoid CF WS TypeErrors
        this.ws.send(out.buffer);
      }
    } catch (e) {
      console.error("Read loop error:", e.message || e);
      this.sendClose(streamId, 0x03); // 0x03 - Network error
      this.cleanupStream(streamId);
      return;
    }
    this.sendClose(streamId, 0x02); // Voluntary closure
    this.cleanupStream(streamId);
  }

  async handleData(streamId, payload) {
    const stream = this.streams.get(streamId);
    if (!stream) return;

    try {
      await stream.writer.ready;
      await stream.writer.write(payload);
      this.sendContinue(streamId, 8);
    } catch (e) {
      console.error("Data write error:", e.message || e);
      this.sendClose(streamId, 0x03);
      this.cleanupStream(streamId);
    }
  }

  handleClose(streamId) {
    this.cleanupStream(streamId);
  }

  cleanupStream(streamId) {
    const stream = this.streams.get(streamId);
    if (stream) {
      stream.closed = true;
      stream.writer.close().catch(() => {});
      this.streams.delete(streamId);
    }
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
