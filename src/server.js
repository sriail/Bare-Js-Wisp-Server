// server.js
'use strict';

// Removed cloudflare:sockets to support the Free Plan.
// We use native fetch() to proxy HTTP requests instead of raw TCP.

export const packet_types = {
  CONNECT: 0x01, DATA: 0x02, CONTINUE: 0x03, CLOSE: 0x04, INFO: 0x05
};

export const stream_types = {
  TCP: 0x01, UDP: 0x02
};

export const close_reasons = {
  Unknown: 0x01, Voluntary: 0x02, NetworkError: 0x03, IncompatibleExtensions: 0x04,
  InvalidInfo: 0x41, UnreachableHost: 0x42, NoResponse: 0x43, ConnRefused: 0x44,
  TransferTimeout: 0x47, HostBlocked: 0x48, ConnThrottled: 0x49, ClientError: 0x81,
  AuthBadPassword: 0xc0, AuthBadSignature: 0xc1, AuthMissingCredentials: 0xc2
};

const text_encoder = new TextEncoder();
const text_decoder = new TextDecoder();

export class WispBuffer {
  constructor(data) {
    if (data instanceof Uint8Array) this.from_array(data);
    else if (typeof data === 'number') this.from_array(new Uint8Array(data));
    else if (typeof data === 'string') this.from_array(text_encoder.encode(data));
    else throw new TypeError("Invalid data type passed to WispBuffer constructor");
  }
  from_array(bytes) {
    this.size = bytes.length;
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }
  concat(buffer) {
    const new_bytes = new Uint8Array(this.size + buffer.size);
    new_bytes.set(this.bytes, 0);
    new_bytes.set(buffer.bytes, this.size);
    return new WispBuffer(new_bytes);
  }
  slice(index, size) {
    return new WispBuffer(this.bytes.slice(index, size));
  }
  get_string() {
    return text_decoder.decode(this.bytes);
  }
}

export class WispPacket {
  static min_size = 5;
  constructor({ type, stream_id, payload, payload_bytes }) {
    this.type = type; this.stream_id = stream_id; this.payload_bytes = payload_bytes; this.payload = payload;
  }
  static parse(buffer) {
    return new WispPacket({
      type: buffer.view.getUint8(0),
      stream_id: buffer.view.getUint32(1, true),
      payload_bytes: buffer.slice(5)
    });
  }
  static parse_all(buffer) {
    if (buffer.size < WispPacket.min_size) throw new TypeError("packet too small");
    const packet = WispPacket.parse(buffer);
    const payload_class = packet_classes[packet.type];
    if (typeof payload_class === 'undefined') throw new TypeError("invalid packet type");
    if (packet.payload_bytes.size < payload_class.min_size) throw new TypeError("payload too small");
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
  static min_size = 3; static type = 0x01;
  constructor({ stream_type, port, hostname }) { this.stream_type = stream_type; this.port = port; this.hostname = hostname; }
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
    return buffer.concat(new WispBuffer(this.hostname));
  }
}

export class DataPayload {
  static min_size = 0; static type = 0x02;
  constructor({ data }) { this.data = data; }
  static parse(buffer) { return new DataPayload({ data: buffer }); }
  serialize() { return this.data; }
}

export class ContinuePayload {
  static min_size = 4; static type = 0x03;
  constructor({ buffer_remaining }) { this.buffer_remaining = buffer_remaining; }
  static parse(buffer) { return new ContinuePayload({ buffer_remaining: buffer.view.getUint32(0, true) }); }
  serialize() {
    let buffer = new WispBuffer(4);
    buffer.view.setUint32(0, this.buffer_remaining, true);
    return buffer;
  }
}

export class ClosePayload {
  static min_size = 1; static type = 0x04;
  constructor({ reason }) { this.reason = reason; }
  static parse(buffer) { return new ClosePayload({ reason: buffer.view.getUint8(0) }); }
  serialize() {
    let buffer = new WispBuffer(1);
    buffer.view.setUint8(0, this.reason);
    return buffer;
  }
}

export class InfoPayload {
  static min_size = 2; static type = 0x05;
  constructor({ major_ver, minor_ver, extensions }) { this.major_ver = major_ver; this.minor_ver = minor_ver; this.extensions = extensions; }
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
  0x01: ConnectPayload, 0x02: DataPayload, 0x03: ContinuePayload, 0x04: ClosePayload, 0x05: InfoPayload
};

/**
 * FetchStream replaces ServerStream. 
 * It intercepts raw HTTP bytes sent over Wisp, executes a fetch(), 
 * and translates the response back into raw HTTP bytes for the Wisp client.
 */
export class FetchStream {
  static buffer_size = 128;

  constructor(stream_id, conn, hostname, port, type) {
    this.stream_id = stream_id;
    this.conn = conn;
    this.hostname = hostname;
    this.port = port;
    this.type = type;
    this.buffer = [];
    this.bufferSize = 0;
    this.headersParsed = false;
    this.closed = false;
  }

  async setup() {
    if (this.type === stream_types.UDP) {
      await this.conn.close_stream(this.stream_id, close_reasons.InvalidInfo);
      return;
    }

    // Send Stream Open Confirmation (Ext 0x05)
    this.conn.send_packet(packet_types.CONTINUE, this.stream_id, new ContinuePayload({
      buffer_remaining: FetchStream.buffer_size
    }));
  }

  async put_data(data) {
    if (this.headersParsed || this.closed) return;
    
    // Accumulate incoming chunks
    this.buffer.push(data.bytes);
    this.bufferSize += data.bytes.length;
    
    // Combine into a single Uint8Array
    let combined = new Uint8Array(this.bufferSize);
    let offset = 0;
    for (let chunk of this.buffer) {
      combined.set(chunk, offset);
      offset += chunk.length;
    }
    
    // Look for the end of HTTP headers (\r\n\r\n)
    let headerEnd = -1;
    for (let i = 0; i < combined.length - 3; i++) {
      if (combined[i] === 13 && combined[i+1] === 10 && combined[i+2] === 13 && combined[i+3] === 10) {
        headerEnd = i + 4;
        break;
      }
    }
    
    if (headerEnd !== -1) {
      this.headersParsed = true;
      let headerStr = text_decoder.decode(combined.slice(0, headerEnd));
      let body = combined.slice(headerEnd);
      
      let lines = headerStr.split('\r\n');
      let firstLine = lines[0].split(' ');
      let method = firstLine[0];
      let path = firstLine[1] || '/';
      
      let headers = {};
      for (let i = 1; i < lines.length; i++) {
        let idx = lines[i].indexOf(':');
        if (idx !== -1) {
          let key = lines[i].substring(0, idx).trim();
          let val = lines[i].substring(idx + 1).trim();
          // Filter out hop-by-hop headers
          if (key.toLowerCase() !== 'host' && key.toLowerCase() !== 'connection') {
            headers[key] = val;
          }
        }
      }
      
      let protocol = this.port === 443 ? 'https' : 'http';
      let url = `${protocol}://${this.hostname}${path}`;
      
      this.executeFetch(url, method, headers, body);
    }
  }

  async executeFetch(url, method, headers, body) {
    try {
      let fetchOptions = {
        method: method,
        headers: headers,
      };
      if (method !== 'GET' && method !== 'HEAD' && body.length > 0) {
        fetchOptions.body = body;
      }

      let response = await fetch(url, fetchOptions);
      
      // Reconstruct Raw HTTP Response
      let statusLine = `HTTP/1.1 ${response.status} ${response.statusText}\r\n`;
      let respHeaders = '';
      for (let [key, value] of response.headers.entries()) {
        let lowerKey = key.toLowerCase();
        // Strip headers that fetch() manages automatically so we don't break raw HTTP parsing
        if (lowerKey !== 'transfer-encoding' && lowerKey !== 'content-encoding' && lowerKey !== 'content-length' && lowerKey !== 'connection') {
          respHeaders += `${key}: ${value}\r\n`;
        }
      }
      respHeaders += 'Connection: close\r\n\r\n';
      
      let respHeaderBytes = text_encoder.encode(statusLine + respHeaders);
      this.conn.send_packet(packet_types.DATA, this.stream_id, new DataPayload({ data: new WispBuffer(respHeaderBytes) }));
      
      // Stream body back to client
      if (response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          this.conn.send_packet(packet_types.DATA, this.stream_id, new DataPayload({ data: new WispBuffer(new Uint8Array(value)) }));
        }
      }
      
      await this.conn.close_stream(this.stream_id, close_reasons.Voluntary);
    } catch (err) {
      console.error("Fetch proxy error:", err);
      await this.conn.close_stream(this.stream_id, close_reasons.NetworkError);
    }
  }

  async close(reason = null) {
    if (this.closed) return;
    this.closed = true;
    if (reason !== null) {
      this.conn.send_packet(packet_types.CLOSE, this.stream_id, new ClosePayload({ reason }));
    }
  }
}
