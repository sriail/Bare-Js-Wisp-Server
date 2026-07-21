// client.js
'use strict';

import { ServerStream, packet_types, stream_types, close_reasons } from './server.js';

export class WispClient {
  constructor(ws) {
    this.ws = ws;
    this.streams = new Map();
  }

  async run() {
    return new Promise((resolve) => {
      this.ws.addEventListener('message', (event) => this.onMessage(event));
      this.ws.addEventListener('close', () => { this.cleanup(); resolve(); });
      this.ws.addEventListener('error', () => { this.cleanup(); resolve(); });
      
      // Spec V1.2: Immediately send a CONTINUE packet on stream ID 0 
      // containing the initial buffer size.
      const payload = new ArrayBuffer(4);
      new DataView(payload).setUint32(0, ServerStream.buffer_size, true);
      this.send_packet(packet_types.CONTINUE, 0, payload);
    });
  }

  // Highly optimized packet sender using raw ArrayBuffers
  send_packet(type, stream_id, payload_buffer) {
    if (this.ws.readyState !== 1) return; // 1 = OPEN
    const payload_len = payload_buffer ? payload_buffer.byteLength : 0;
    const buf = new ArrayBuffer(5 + payload_len);
    const view = new DataView(buf);
    const u8 = new Uint8Array(buf);
    
    view.setUint8(0, type);
    view.setUint32(1, stream_id, true); // Little-endian
    
    if (payload_len > 0) {
      u8.set(new Uint8Array(payload_buffer), 5);
    }
    
    this.ws.send(buf);
  }

  // Async to handle Blob conversion, but parsing logic is entirely synchronous
  async onMessage(event) {
    let buf;
    if (event.data instanceof ArrayBuffer) {
      buf = event.data;
    } else if (event.data instanceof Blob) {
      buf = await event.data.arrayBuffer();
    } else {
      return; 
    }

    if (buf.byteLength < 5) return;
    
    const view = new DataView(buf);
    const type = view.getUint8(0);
    const stream_id = view.getUint32(1, true);
    const payload = new Uint8Array(buf, 5);

    try {
      if (type === packet_types.CONNECT) {
        if (stream_id === 0 || this.streams.has(stream_id)) return;
        if (payload.length < 3) return;
        
        const stream_type = payload[0];
        const port = view.getUint16(5, true); // Offset 5 in the original buf
        const hostname = new TextDecoder().decode(payload.slice(3)).trim();
        
        const stream = new ServerStream(stream_id, this, hostname, port, stream_type);
        this.streams.set(stream_id, stream);
        
        // Fire and forget. setup() runs in the background, preserving CPU time.
        stream.setup();
        return;
      }

      const stream = this.streams.get(stream_id);
      if (!stream) return;

      if (type === packet_types.DATA) {
        // Synchronously push to queue. Zero await blocking.
        stream.put_data(payload);
      } else if (type === packet_types.CLOSE) {
        this.close_stream(stream_id, true);
      }
    } catch (error) {
      // Prevent malformed packets from crashing the worker
    }
  }

  async close_stream(stream_id, quiet = false) {
    const stream = this.streams.get(stream_id);
    if (!stream) return;
    await stream.close(quiet ? null : close_reasons.Unknown);
    this.streams.delete(stream_id);
  }

  cleanup() {
    for (const stream of this.streams.values()) {
      stream.close(close_reasons.NetworkError);
    }
    this.streams.clear();
  }
}
