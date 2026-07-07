// wisp-protocol.js
// Core Wisp protocol implementation — packet parsing, serialization, and extension handling.
// Environment-agnostic: works in Workers, browsers, and Node.js.
// Based on Wisp protocol v2.1 specification by @ading2210.

// ─── Constants ───────────────────────────────────────────────────────────────

export const PacketType = {
  CONNECT:  0x01,
  DATA:     0x02,
  CONTINUE: 0x03,
  CLOSE:    0x04,
  INFO:     0x05,
};

export const StreamType = {
  TCP: 0x01,
  UDP: 0x02,
};

export const CloseReason = {
  Unspecified:             0x01,
  Voluntary:               0x02,
  NetworkError:            0x03,
  IncompatibleExtensions:  0x04,
  InvalidInfo:             0x41,
  UnreachableHost:         0x42,
  ConnectionTimeout:       0x43,
  ConnectionRefused:       0x44,
  DataTimeout:             0x47,
  Blocked:                 0x48,
  Throttled:               0x49,
  ClientError:             0x81,
  AuthInvalidCredentials:  0xc0,
  AuthInvalidSignature:    0xc1,
  AuthRequired:            0xc2,
};

export const ExtensionID = {
  UDP:                     0x01,
  PasswordAuth:            0x02,
  KeyAuth:                 0x03,
  MOTD:                    0x04,
  StreamOpenConfirmation:  0x05,
};

// ─── Binary Helpers ──────────────────────────────────────────────────────────

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export class BufferReader {
  constructor(data) {
    this.bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
    this.view = new DataView(this.bytes.buffer, this.bytes.byteOffset, this.bytes.byteLength);
    this.offset = 0;
  }

  get remaining() {
    return this.bytes.byteLength - this.offset;
  }

  readUint8() {
    const v = this.view.getUint8(this.offset);
    this.offset += 1;
    return v;
  }

  readUint16() {
    const v = this.view.getUint16(this.offset, true);
    this.offset += 2;
    return v;
  }

  readUint32() {
    const v = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return v;
  }

  readBytes(length) {
    const v = this.bytes.subarray(this.offset, this.offset + length);
    this.offset += length;
    return v;
  }

  readRest() {
    return this.bytes.subarray(this.offset);
  }
}

export class BufferWriter {
  constructor(size) {
    this.buffer = new ArrayBuffer(size);
    this.view = new DataView(this.buffer);
    this.bytes = new Uint8Array(this.buffer);
    this.offset = 0;
  }

  writeUint8(v) {
    this.view.setUint8(this.offset, v);
    this.offset += 1;
  }

  writeUint16(v) {
    this.view.setUint16(this.offset, v, true);
    this.offset += 2;
  }

  writeUint32(v) {
    this.view.setUint32(this.offset, v, true);
    this.offset += 4;
  }

  writeBytes(v) {
    this.bytes.set(v, this.offset);
    this.offset += v.byteLength;
  }

  writeString(s) {
    this.writeBytes(textEncoder.encode(s));
  }

  get result() {
    return this.bytes.slice(0, this.offset);
  }
}

// ─── Packet Parsing ──────────────────────────────────────────────────────────

export function parsePacket(data) {
  const r = new BufferReader(data);
  const type = r.readUint8();
  const streamId = r.readUint32();
  const payload = r.readRest();
  return { type, streamId, payload };
}

export function parseConnectPayload(payload) {
  const r = new BufferReader(payload);
  const streamType = r.readUint8();
  const port = r.readUint16();
  const hostname = textDecoder.decode(r.readRest()).trim();
  return { streamType, port, hostname };
}

export function parseContinuePayload(payload) {
  const r = new BufferReader(payload);
  return { bufferRemaining: r.readUint32() };
}

export function parseClosePayload(payload) {
  const r = new BufferReader(payload);
  return { reason: r.readUint8() };
}

export function parseInfoPayload(payload) {
  const r = new BufferReader(payload);
  const majorVer = r.readUint8();
  const minorVer = r.readUint8();
  const extensions = parseExtensions(r.readRest());
  return { majorVer, minorVer, extensions };
}

export function parseExtensions(data) {
  const r = new BufferReader(data);
  const extensions = [];
  while (r.remaining >= 5) {
    const id = r.readUint8();
    const length = r.readUint32();
    const metadata = r.readBytes(length);
    extensions.push({ id, metadata });
  }
  return extensions;
}

// ─── Packet Building ─────────────────────────────────────────────────────────

export function buildConnectPacket(streamId, streamType, port, hostname) {
  const hostBytes = textEncoder.encode(hostname);
  const w = new BufferWriter(5 + 1 + 2 + hostBytes.byteLength);
  w.writeUint8(PacketType.CONNECT);
  w.writeUint32(streamId);
  w.writeUint8(streamType);
  w.writeUint16(port);
  w.writeBytes(hostBytes);
  return w.result;
}

export function buildDataPacket(streamId, data) {
  const w = new BufferWriter(5 + data.byteLength);
  w.writeUint8(PacketType.DATA);
  w.writeUint32(streamId);
  w.writeBytes(data);
  return w.result;
}

export function buildContinuePacket(streamId, bufferRemaining) {
  const w = new BufferWriter(9);
  w.writeUint8(PacketType.CONTINUE);
  w.writeUint32(streamId);
  w.writeUint32(bufferRemaining);
  return w.result;
}

export function buildClosePacket(streamId, reason) {
  const w = new BufferWriter(6);
  w.writeUint8(PacketType.CLOSE);
  w.writeUint32(streamId);
  w.writeUint8(reason);
  return w.result;
}

export function buildInfoPacket(streamId, majorVer, minorVer, extensions) {
  const extBuf = serializeExtensions(extensions);
  const w = new BufferWriter(5 + 2 + extBuf.byteLength);
  w.writeUint8(PacketType.INFO);
  w.writeUint32(streamId);
  w.writeUint8(majorVer);
  w.writeUint8(minorVer);
  w.writeBytes(extBuf);
  return w.result;
}

// ─── Extension Serialization ─────────────────────────────────────────────────

export function serializeExtensions(extensions) {
  let total = 0;
  for (const ext of extensions) {
    total += 5 + (ext.metadata ? ext.metadata.byteLength : 0);
  }
  const w = new BufferWriter(total);
  for (const ext of extensions) {
    w.writeUint8(ext.id);
    const meta = ext.metadata || new Uint8Array(0);
    w.writeUint32(meta.byteLength);
    w.writeBytes(meta);
  }
  return w.result;
}
