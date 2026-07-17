// server.js
'use strict';

import { connect } from 'cloudflare:sockets';

// --- Wisp Protocol Constants ---
export const packet_types = {
  CONNECT: 0x01,
  DATA: 0x02,
  CONTINUE: 0x03,
  CLOSE: 0x04,
  INFO: 0x05
};

export const stream_types = {
  TCP: 0x01,
  UDP: 0x02
};

export const close_reasons = {
  Unknown: 0x01,
  Voluntary: 0x02,
  NetworkError: 0x03,
  IncompatibleExtensions: 0x04,
  InvalidInfo: 0x41,
  UnreachableHost: 0x42,
  NoResponse: 0x43,
  ConnRefused: 0x44,
  TransferTimeout: 0x47,
  HostBlocked: 0x48,
  ConnThrottled: 0x49,
  ClientError: 0x81,
  AuthBadPassword: 0xc0,
  AuthBadSignature: 0xc1,
  AuthMissingCredentials: 0xc2
};

const text_encoder = new TextEncoder();
const text_decoder = new TextDecoder();

// --- Core Buffer & Packet Logic ---
// Strictly implements the provided buffer and packet parsing mechanism.
export class WispBuffer {
  constructor(data) {
    if (data instanceof Uint8Array) {
      this.from_array(data);
    } else if (typeof data === 'number') {
      this.from_array(new Uint8Array(data));
    } else if (typeof data === 'string') {
      this.from_array(text_encoder.encode(data));
    } else {
      throw new TypeError("Invalid data type passed to WispBuffer constructor");
    }
  }

  from_array(bytes) {
    this.size = bytes.length;
    this.bytes = bytes;
    // DataView must respect byteOffset and byteLength to avoid alignment issues
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  concat(buffer) {
    const new_bytes = new Uint8Array(this.size + buffer.size);
    new_bytes.set(this.bytes, 0);
    new_bytes.set(buffer.bytes, this.size);
    return new WispBuffer(new_bytes);
  }

  slice(index, size) {
    // Replicating the exact slice behavior from the reference implementation
    const bytes_slice = this.bytes.slice(index, size);
    return new WispBuffer(bytes_slice);
  }

  get_string() {
    return text_decoder.decode(this.bytes);
  }
}

export class WispPacket {
  static min_size = 5;
  
  constructor({ type, stream_id, payload, payload_bytes }) {
    this.type = type;
    this.stream_id = stream_id;
    this.payload_bytes = payload_bytes;
    this.payload = payload;
  }

  static parse(buffer) {
    return new WispPacket({
      type: buffer.view.getUint8(0),
      stream_id: buffer.view.getUint32(1, true), // little-endian
      payload_bytes: buffer.slice(5)
    });
  }

  static parse_all(buffer) {
    if (buffer.size < WispPacket.min_size) {
      throw new TypeError("packet too small");
    }
    const packet = WispPacket.parse(buffer);
    const payload_class = packet_classes[packet.type];
    if (typeof payload_class === 'undefined') {
      throw new TypeError("invalid packet type");
    }
    if (packet.payload_bytes.size < payload_class.min_size) {
      throw new TypeError("payload too small");
    }
    packet.payload = payload_class.parse(packet.payload_bytes);
    return packet;
  }

  serialize() {
    let buffer = new WispBuffer(5);
    buffer.view.setUint8(0, this.type);
    buffer.view.setUint32(1, this.stream_id, true);
    buffer = buffer.concat(this.payload.serialize());
    return buffer;
  }
}

export class ConnectPayload {
  static min_size = 3;
  static type = 0x01;
  constructor({ stream_type, port, hostname }) {
    this.stream_type = stream_type;
    this.port = port;
    this.hostname = hostname;
  }
  static parse(buffer) {
    return new ConnectPayload({
      stream_type: buffer.view.getUint8(0),
      port: buffer.view.getUint16(1, true),
      hostname: buffer.slice(3).get_string()
    });
  }
  serialize() {
    let buffer = new WispBuffer(3);
    buffer.view.setUint8(0, this.stream_type);
    buffer.view.setUint16(1, this.port, true);
    buffer = buffer.concat(new WispBuffer(this.hostname));
    return buffer;
  }
}

export class DataPayload {
  static min_size = 0;
  static type = 0x02;
  constructor({ data }) {
    this.data = data;
  }
  static parse(buffer) {
    return new DataPayload({ data: buffer });
  }
  serialize() {
    return this.data;
  }
}

export class ContinuePayload {
  static min_size = 4;
  static type = 0x03;
  constructor({ buffer_remaining }) {
    this.buffer_remaining = buffer_remaining;
  }
  static parse(buffer) {
    return new ContinuePayload({
      buffer_remaining: buffer.view.getUint32(0, true),
    });
  }
  serialize() {
    let buffer = new WispBuffer(4);
    buffer.view.setUint32(0, this.buffer_remaining, true);
    return buffer;
  }
}

export class ClosePayload {
  static min_size = 1;
  static type = 0x04;
  constructor({ reason }) {
    this.reason = reason;
  }
  static parse(buffer) {
    return new ClosePayload({
      reason: buffer.view.getUint8(0),
    });
  }
  serialize() {
    let buffer = new WispBuffer(1);
    buffer.view.setUint8(0, this.reason);
    return buffer;
  }
}

export class InfoPayload {
  static min_size = 2;
  static type = 0x05;
  constructor({ major_ver, minor_ver, extensions }) {
    this.major_ver = major_ver;
    this.minor_ver = minor_ver;
    this.extensions = extensions;
  }
  static parse(buffer) {
    return new InfoPayload({
      major_ver: buffer.view.getUint8(0),
      minor_ver: buffer.view.getUint8(1),
      extensions: buffer.slice(2)
    });
  }
  serialize() {
    let buffer = new WispBuffer(2);
    buffer.view.setUint8(0, this.major_ver);
    buffer.view.setUint8(1, this.minor_ver);
    return buffer.concat(this.extensions);
  }
}

const packet_classes = {
  0x01: ConnectPayload,
  0x02: DataPayload,
  0x03: ContinuePayload,
  0x04: ClosePayload,
  0x05: InfoPayload
};

// --- Async Queue for strict FIFO TCP buffering ---
class AsyncQueue {
  constructor() {
    this.queue = [];
    this.waiting = null;
    this.closed = false;
  }

  put(item) {
    if (this.closed) return;
    if (this.waiting) {
      const w = this.waiting;
      this.waiting = null;
      w.resolve(item);
    } else {
      this.queue.push(item);
    }
  }

  close() {
    this.closed = true;
    if (this.waiting) {
      const w = this.waiting;
      this.waiting = null;
      w.resolve(null);
    }
  }

  async get() {
    if (this.queue.length > 0) return this.queue.shift();
    if (this.closed) return null;
    return new Promise((resolve) => { this.waiting = { resolve }; });
  }

  get size() {
    return this.queue.length;
  }
}

// --- Stream Management ---
export class ServerStream {
  static buffer_size = 128;

  constructor(stream_id, conn) {
    this.stream_id = stream_id;
    this.conn = conn;
    this.socket = null;
    this.writer = null;
    this.send_buffer = new AsyncQueue();
    this.packets_sent = 0;
    this.closed = false;
  }

  async connect(type, port, hostname) {
    if (type === stream_types.UDP) {
      // Cloudflare Workers do not support outbound UDP
      await this.conn.close_stream(this.stream_id, close_reasons.InvalidInfo);
      return;
    }

    try {
      this.socket = connect({ hostname, port: Number(port) });
      this.writer = this.socket.writable.getWriter();
      
      // Start proxy tasks in the background
      this.tcp_to_ws().catch((error) => {
        if (!this.closed) this.close(close_reasons.NetworkError);
      });
      this.ws_to_tcp().catch((error) => {
        if (!this.closed) this.close(close_reasons.NetworkError);
      });
    } catch (err) {
      let reason = close_reasons.UnreachableHost;
      if (err?.cause?.code === 'ECONNREFUSED') reason = close_reasons.ConnRefused;
      await this.conn.close_stream(this.stream_id, reason);
    }
  }

  async tcp_to_ws() {
    const reader = this.socket.readable.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        
        const packet = new WispPacket({
          type: packet_types.DATA,
          stream_id: this.stream_id,
          payload: new DataPayload({ data: new WispBuffer(new Uint8Array(value)) })
        });
        this.conn.ws.send(packet.serialize().bytes);
      }
      await this.conn.close_stream(this.stream_id, close_reasons.Voluntary);
    } finally {
      reader.releaseLock();
    }
  }

  async ws_to_tcp() {
    while (true) {
      const data = await this.send_buffer.get();
      if (data == null) break; // stream closed
      
      await this.writer.write(data.bytes);

      this.packets_sent++;
      // Periodically send CONTINUE packets to update the client's buffer remaining
      if (this.packets_sent % (ServerStream.buffer_size / 2) !== 0) continue;
      
      const packet = new WispPacket({
        type: packet_types.CONTINUE,
        stream_id: this.stream_id,
        payload: new ContinuePayload({
          buffer_remaining: ServerStream.buffer_size - this.send_buffer.size
        })
      });
      this.conn.ws.send(packet.serialize().bytes);
    }
    await this.close();
  }

  async put_data(data) {
    if (this.send_buffer.size >= ServerStream.buffer_size) {
      // Strict congestion control: drop the stream if the client violates backpressure
      await this.conn.close_stream(this.stream_id, close_reasons.ConnThrottled);
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
      const packet = new WispPacket({
        type: packet_types.CLOSE,
        stream_id: this.stream_id,
        payload: new ClosePayload({ reason })
      });
      try { this.conn.ws.send(packet.serialize().bytes); } catch(e) {}
    }
  }
}
