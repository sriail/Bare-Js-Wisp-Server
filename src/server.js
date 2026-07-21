// server.js
'use strict';

import { connect } from 'cloudflare:sockets';

export const packet_types = {
  CONNECT: 0x01, DATA: 0x02, CONTINUE: 0x03, CLOSE: 0x04
};

export const stream_types = {
  TCP: 0x01, UDP: 0x02
};

export const close_reasons = {
  Unknown: 0x01, Voluntary: 0x02, NetworkError: 0x03,
  InvalidInfo: 0x41, UnreachableHost: 0x42, NoResponse: 0x43, ConnRefused: 0x44,
  TransferTimeout: 0x47, HostBlocked: 0x48, ConnThrottled: 0x49
};

// Highly optimized lightweight FIFO queue for backpressure
class AsyncQueue {
  constructor() { this.queue = []; this.waiting = null; this.closed = false; }
  put(item) {
    if (this.closed) return;
    if (this.waiting) { const w = this.waiting; this.waiting = null; w.resolve(item); } 
    else { this.queue.push(item); }
  }
  close() {
    this.closed = true;
    if (this.waiting) { const w = this.waiting; this.waiting = null; w.resolve(null); }
  }
  async get() {
    if (this.queue.length > 0) return this.queue.shift();
    if (this.closed) return null;
    return new Promise((resolve) => { this.waiting = { resolve }; });
  }
  get size() { return this.queue.length; }
}

export class ServerStream {
  static buffer_size = 128;

  constructor(stream_id, conn, hostname, port, type) {
    this.stream_id = stream_id;
    this.conn = conn;
    this.hostname = hostname;
    this.port = port;
    this.type = type;
    this.socket = null;
    this.writer = null;
    this.send_buffer = new AsyncQueue();
    this.packets_sent = 0;
    this.closed = false;
  }

  async setup() {
    if (this.type === stream_types.UDP) {
      await this.conn.close_stream(this.stream_id, close_reasons.InvalidInfo);
      return;
    }

    try {
      this.socket = connect({ hostname: this.hostname, port: Number(this.port) });
      this.writer = this.socket.writable.getWriter();
      // CRITICAL FIX: Await socket establishment to catch connection errors.
      // setup() runs in the background, so this does not block the WS event loop.
      await this.socket.opened;
    } catch (err) {
      console.error("Socket connect failed:", err);
      let reason = close_reasons.UnreachableHost;
      if (err?.message?.includes("free plan") || err?.message?.includes("not available")) {
        reason = close_reasons.InvalidInfo;
      } else if (err?.cause?.code === 'ECONNREFUSED') {
        reason = close_reasons.ConnRefused;
      }
      await this.conn.close_stream(this.stream_id, reason);
      return;
    }

    this.tcp_to_ws().catch((err) => {
      console.error("tcp_to_ws error:", err);
      this.close(close_reasons.NetworkError);
    });
    this.ws_to_tcp().catch((err) => {
      console.error("ws_to_tcp error:", err);
      this.close(close_reasons.NetworkError);
    });
  }

  async tcp_to_ws() {
    const reader = this.socket.readable.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        this.conn.send_packet(packet_types.DATA, this.stream_id, value);
      }
      await this.conn.close_stream(this.stream_id, close_reasons.Voluntary);
    } catch (err) {
      if (!this.closed) await this.conn.close_stream(this.stream_id, close_reasons.NetworkError);
    } finally {
      try { reader.releaseLock(); } catch(e) {}
    }
  }

  async ws_to_tcp() {
    while (true) {
      const data = await this.send_buffer.get();
      if (data == null) break;
      
      try {
        await this.writer.write(data);
      } catch (err) {
        if (!this.closed) await this.conn.close_stream(this.stream_id, close_reasons.NetworkError);
        break;
      }
      
      this.packets_sent++;
      if (this.packets_sent % (ServerStream.buffer_size / 2) !== 0) continue;
      
      const payload = new Uint8Array(4);
      new DataView(payload.buffer).setUint32(0, ServerStream.buffer_size - this.send_buffer.size, true);
      this.conn.send_packet(packet_types.CONTINUE, this.stream_id, payload.buffer);
    }
    await this.close();
  }

  put_data(data) {
    if (this.send_buffer.size >= ServerStream.buffer_size) {
      this.conn.close_stream(this.stream_id, close_reasons.ConnThrottled);
      return;
    }
    this.send_buffer.put(data);
  }

  async close(reason = null) {
    if (this.closed) return;
    this.closed = true;
    this.send_buffer.close();
    try { if (this.writer) await this.writer.releaseLock(); } catch(e) {}
    try { if (this.socket) this.socket.close(); } catch(e) {}
    if (reason !== null) {
      const payload = new Uint8Array(1);
      payload[0] = reason;
      this.conn.send_packet(packet_types.CLOSE, this.stream_id, payload.buffer);
    }
  }
}
