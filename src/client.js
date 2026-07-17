// client.js
'use strict';

import { 
  ServerStream, 
  WispPacket, 
  WispBuffer,
  ConnectPayload, 
  DataPayload, 
  ClosePayload,
  ContinuePayload,
  InfoPayload,
  packet_types,
  close_reasons
} from './server.js';

export class WispClient {
  constructor(ws) {
    this.ws = ws;
    this.streams = new Map();
    this.handshakeComplete = false;
  }

  async run() {
    return new Promise((resolve) => {
      this.ws.addEventListener('message', (event) => this.onMessage(event));
      this.ws.addEventListener('close', () => { this.cleanup(); resolve(); });
      this.ws.addEventListener('error', () => { this.cleanup(); resolve(); });

      // Initiate Wisp v2 handshake
      this.send_server_info();
    });
  }

  send_packet(type, stream_id, payload) {
    if (this.ws.readyState !== 1) return; // 1 = OPEN
    const packet = new WispPacket({ type, stream_id, payload });
    this.ws.send(packet.serialize().bytes);
  }

  send_server_info() {
    // Construct Extension 0x05 (Stream Open Confirmation) metadata
    // Format: [ID (1 byte)] [Payload Length (4 bytes LE)]
    const ext_buffer = new WispBuffer(5);
    ext_buffer.view.setUint8(0, 0x05);
    ext_buffer.view.setUint32(1, 0, true);

    this.send_packet(packet_types.INFO, 0, new InfoPayload({
      major_ver: 2,
      minor_ver: 1,
      extensions: ext_buffer
    }));
  }

  async onMessage(event) {
    let buffer;
    if (event.data instanceof ArrayBuffer) {
      buffer = new WispBuffer(new Uint8Array(event.data));
    } else if (event.data instanceof Blob) {
      const ab = await event.data.arrayBuffer();
      buffer = new WispBuffer(new Uint8Array(ab));
    } else {
      return; // Unsupported data type
    }

    try {
      this.route_packet(buffer);
    } catch (error) {
      // Malformed packet; ignore to maintain connection stability
    }
  }

  route_packet(buffer) {
    if (buffer.size < WispPacket.min_size) return;
    const packet = WispPacket.parse_all(buffer);

    if (packet.type === packet_types.INFO) {
      this.handle_client_info();
      return;
    }

    if (packet.type === packet_types.CONNECT) {
      this.create_stream(
        packet.stream_id,
        packet.payload.stream_type,
        packet.payload.hostname.trim(),
        packet.payload.port
      );
      return;
    }

    const stream = this.streams.get(packet.stream_id);
    if (!stream) {
      // Stream doesn't exist or was already closed
      return;
    }

    if (packet.type === packet_types.DATA) {
      stream.put_data(packet.payload.data);
    } else if (packet.type === packet_types.CLOSE) {
      this.close_stream(packet.stream_id, null, true); // Quiet close on client request
    }
  }

  handle_client_info() {
    if (this.handshakeComplete) return;
    this.handshakeComplete = true;
    
    // Send the initial CONTINUE packet on stream ID 0 to complete handshake
    this.send_packet(packet_types.CONTINUE, 0, new ContinuePayload({
      buffer_remaining: ServerStream.buffer_size
    }));
  }

  create_stream(stream_id, type, hostname, port) {
    if (stream_id === 0 || this.streams.has(stream_id)) return;

    const stream = new ServerStream(stream_id, this, hostname, port, type);
    this.streams.set(stream_id, stream);
    stream.setup();
  }

  async close_stream(stream_id, reason = null, quiet = false) {
    const stream = this.streams.get(stream_id);
    if (!stream) return;

    await stream.close(quiet ? null : reason);
    this.streams.delete(stream_id);
  }

  cleanup() {
    for (const stream of this.streams.values()) {
      stream.close(close_reasons.NetworkError);
    }
    this.streams.clear();
  }
}
