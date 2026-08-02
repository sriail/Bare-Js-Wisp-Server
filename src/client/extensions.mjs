// Stubs for V2 extensions which may be expected by older V2 clients
export class UDPExtension {
  static id = 0x02;
  constructor({client_config}) { this.client_config = client_config; this.id = UDPExtension.id; this.payload = {}; }
  serialize() { return new Uint8Array(0); }
}
export class MOTDExtension {
  static id = 0x01;
  constructor({client_config}) { this.client_config = client_config; this.id = MOTDExtension.id; this.payload = {}; }
  serialize() { return new Uint8Array(0); }
}
export function serialize_extensions(exts) { return new Uint8Array(0); }
export function parse_extensions(buf, client_exts, src) { return []; }
