export const packet_types = {
  CONNECT: 0x01,
  DATA: 0x02,
  CONTINUE: 0x03,
  CLOSE: 0x04,
  INFO: 0x10
};

export const packet_classes = {
  0x01: { name: "CONNECT" },
  0x02: { name: "DATA" },
  0x03: { name: "CONTINUE" },
  0x04: { name: "CLOSE" },
  0x10: { name: "INFO" }
};

export const stream_types = {
  TCP: 0x01,
  UDP: 0x02
};

export class WispBuffer {
  constructor(data) {
    this.bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
    this.offset = 0;
    this.view = new DataView(this.bytes.buffer);
  }
  get size() { return this.bytes.byteLength; }
  readUint8() { return this.bytes[this.offset++]; }
  readUint16() { const v = this.view.getUint16(this.offset, true); this.offset += 2; return v; }
  readUint32() { const v = this.view.getUint32(this.offset, true); this.offset += 4; return v; }
  readBytes(n) { const v = this.bytes.slice(this.offset, this.offset + n); this.offset += n; return v; }
  writeUint8(v) { this.bytes[this.offset++] = v; }
  writeUint16(v) { this.view.setUint16(this.offset, v, true); this.offset += 2; }
  writeUint32(v) { this.view.setUint32(this.offset, v, true); this.offset += 4; }
  writeBytes(v) { this.bytes.set(v, this.offset); this.offset += v.length; }
  writeString(v) { const enc = new TextEncoder().encode(v); this.writeBytes(enc); return enc.length; }
  readString(n) { return new TextDecoder().decode(this.readBytes(n)); }
}

class Payload {
  constructor(args) { Object.assign(this, args); }
  serialize() {}
}

export class ConnectPayload extends Payload {
  serialize() {
    const enc = new TextEncoder().encode(this.hostname);
    const buf = new WispBuffer(new Uint8Array(3 + enc.length));
    buf.writeUint8(this.stream_type);
    buf.writeUint16(this.port);
    buf.writeBytes(enc);
    return buf;
  }
}

export class DataPayload extends Payload {
  serialize() { 
    return this.data instanceof WispBuffer ? this.data : new WispBuffer(this.data);
  }
}

export class ClosePayload extends Payload {
  serialize() { const b = new WispBuffer(new Uint8Array(1)); b.writeUint8(this.reason); return b; }
}

export class InfoPayload extends Payload {
  serialize() { return this.extensions; }
}

export class WispPacket {
  constructor({ type, stream_id, payload }) {
    this.type = type;
    this.stream_id = stream_id;
    this.payload = payload;
  }
  static get min_size() { return 5; }
  static parse_all(buf) {
    const type = buf.readUint8();
    const stream_id = buf.readUint32();
    const p_bytes = buf.readBytes(buf.size - 5);
    const p_buf = new WispBuffer(p_bytes);
    let payload = {};
    
    if (type === packet_types.CONNECT) {
      payload = new ConnectPayload({
        stream_type: p_buf.readUint8(),
        port: p_buf.readUint16(),
        hostname: p_buf.readString(p_bytes.length - 3)
      });
    } else if (type === packet_types.CONTINUE) {
      payload = { buffer_remaining: p_buf.readUint32() };
    } else if (type === packet_types.CLOSE) {
      payload = new ClosePayload({ reason: p_buf.readUint8() });
    } else if (type === packet_types.DATA) {
      payload = new DataPayload({ data: new WispBuffer(p_bytes) });
    }
    return { type, stream_id, payload, payload_bytes: new WispBuffer(p_bytes) };
  }
  serialize() {
    const p_buf = this.payload.serialize();
    const p_size = p_buf.size;
    const buf = new WispBuffer(new Uint8Array(5 + p_size));
    buf.writeUint8(this.type);
    buf.writeUint32(this.stream_id);
    buf.writeBytes(p_buf.bytes);
    return buf;
  }
}
