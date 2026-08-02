import { connect } from "cloudflare:sockets";
import { Buffer } from "node:buffer";
import { packet_types, stream_types } from "./protocol.js";

export class ConnectionHandler {
  constructor(ws) {
    this.ws = ws;
    this.streams = new Map();
    this.init();
  }

  async init() {
    // Send initial CONTINUE packet (Stream ID: 0, Buffer Remaining: 8)
    const buf = Buffer.alloc(9);
    buf.writeUInt8(packet_types.CONTINUE, 0);
    buf.writeUInt32LE(0, 1);
    buf.writeUInt32LE(8, 5);
    this.ws.send(buf);

    this.ws.addEventListener("message", (event) => {
      this.onMessage(event.data).catch((err) => console.error("Handler error:", err));
    });

    this.ws.addEventListener("close", () => this.onClose());
    this.ws.addEventListener("error", () => this.onClose());
  }

  async onMessage(data) {
    const buf = Buffer.from(data);
    if (buf.length < 5) return;

    const type = buf.readUInt8(0);
    const streamId = buf.readUInt32LE(1);
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
    const streamType = payload.readUInt8(0);
    const port = payload.readUInt16LE(1);
    const hostname = payload.subarray(3).toString("utf-8");

    if (streamType === stream_types.UDP) {
      this.sendClose(streamId, 0x41);
      return;
    }

    try {
      const tcpSocket = connect({ hostname, port });
      
      // Crucial: Wait for the TCP socket to actually establish the connection
      await tcpSocket.opened;
      
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
      console.error("Connect error:", e);
      this.sendClose(streamId, 0x44); // 0x44 - Connection refused
    }
  }

  async startReadLoop(streamId, stream) {
    try {
      while (!stream.closed) {
        const { done, value } = await stream.reader.read();
        if (done) break;

        const header = Buffer.alloc(5);
        header.writeUInt8(packet_types.DATA, 0);
        header.writeUInt32LE(streamId, 1);
        this.ws.send(Buffer.concat([header, Buffer.from(value)]));
      }
    } catch (e) {
      console.error("Read loop error:", e);
    }
    this.sendClose(streamId, 0x02);
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
      console.error("Data write error:", e);
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
    const buf = Buffer.alloc(6);
    buf.writeUInt8(packet_types.CLOSE, 0);
    buf.writeUInt32LE(streamId, 1);
    buf.writeUInt8(reason, 5);
    try { this.ws.send(buf); } catch (e) {}
  }

  sendContinue(streamId, remaining) {
    const buf = Buffer.alloc(9);
    buf.writeUInt8(packet_types.CONTINUE, 0);
    buf.writeUInt32LE(streamId, 1);
    buf.writeUInt32LE(remaining, 5);
    try { this.ws.send(buf); } catch (e) {}
  }

  onClose() {
    for (const id of this.streams.keys()) {
      this.cleanupStream(id);
    }
    this.streams.clear();
  }
}
