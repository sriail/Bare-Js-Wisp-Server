// src/client.js
'use strict';

// ========== Server-side Wisp Client Handler ==========
// This class is imported by connections.js and used by the Cloudflare Worker
// to handle incoming WebSocket connections and proxy them via ServerStream/FetchStream.
import { ServerStream, FetchStream, packet_types, stream_types, close_reasons } from './server.js';

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
      
      // Send initial CONTINUE on stream 0 (handshake)
      const payload = new ArrayBuffer(4);
      new DataView(payload).setUint32(0, 128, true); // Initial global buffer
      this.send_packet(packet_types.CONTINUE, 0, payload);
    });
  }

  send_packet(type, stream_id, payload_buffer) {
    if (this.ws.readyState !== 1) return;
    const payload_len = payload_buffer ? payload_buffer.byteLength : 0;
    const buf = new ArrayBuffer(5 + payload_len);
    const view = new DataView(buf);
    const u8 = new Uint8Array(buf);
    
    view.setUint8(0, type);
    view.setUint32(1, stream_id, true);
    
    if (payload_len > 0) {
      u8.set(new Uint8Array(payload_buffer), 5);
    }
    
    this.ws.send(buf);
  }

  async onMessage(event) {
    let buf;
    if (event.data instanceof ArrayBuffer) buf = event.data;
    else if (event.data instanceof Blob) buf = await event.data.arrayBuffer();
    else return; 

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
        const port = view.getUint16(6, true); 
        const hostname = new TextDecoder().decode(payload.slice(3)).trim();
        
        let stream;
        if (port === 80 || port === 443 || port === 8080 || port === 8443) {
          stream = new FetchStream(stream_id, this, hostname, port, stream_type);
        } else {
          stream = new ServerStream(stream_id, this, hostname, port, stream_type);
        }
        
        this.streams.set(stream_id, stream);
        stream.setup().catch(err => console.error("Stream setup failed:", err));
        return;
      }

      const stream = this.streams.get(stream_id);
      if (!stream) return;

      if (type === packet_types.DATA) {
        stream.recv_count++;
        stream.put_data(payload);
        
        // Send CONTINUE packet if we've received enough data to replenish buffer
        if (stream.recv_count >= stream.constructor.buffer_size) {
          const p = new ArrayBuffer(4);
          new DataView(p).setUint32(0, stream.constructor.buffer_size, true);
          this.send_packet(packet_types.CONTINUE, stream_id, p);
          stream.recv_count = 0;
        }
      } else if (type === packet_types.CLOSE) {
        this.close_stream(stream_id, null, true);
      }
    } catch (error) {
      console.error("onMessage error:", error);
    }
  }

  async close_stream(stream_id, reason = null, quiet = false) {
    const stream = this.streams.get(stream_id);
    if (!stream) return;
    await stream.close(quiet ? null : reason);
    this.streams.delete(stream_id);
  }

  cleanup() {
    for (const stream of this.streams.values()) stream.close(close_reasons.NetworkError);
    this.streams.clear();
  }
}

// ========== Browser-side Client Polyfill (WispWebSocket) ==========
// Provides a drop-in replacement for the native WebSocket API to easily
// route wsproxy-style connections through the Wisp protocol.

const client_packet_types = {
  CONNECT: 0x01,
  DATA: 0x02,
  CONTINUE: 0x03,
  CLOSE: 0x04
};

const client_stream_types = {
  TCP: 0x01,
  UDP: 0x02
};

export const _wisp_connections = {};

class ClientStream {
  constructor(hostname, port, websocket, buffer_size, stream_id, connection, stream_type) {
    this.hostname = hostname;
    this.port = port;
    this.ws = websocket;
    this.buffer_size = buffer_size;
    this.stream_id = stream_id;
    this.connection = connection;
    this.stream_type = stream_type;
    this.send_buffer = [];
    this.open = true;

    this.onopen = () => {};
    this.onclose = () => {};
    this.onmessage = () => {};
  }

  send(data) {
    if (this.buffer_size > 0 || !this.open || this.stream_type === client_stream_types.UDP) {
      const buf = new ArrayBuffer(5 + data.length);
      const view = new DataView(buf);
      const u8 = new Uint8Array(buf);
      view.setUint8(0, client_packet_types.DATA);
      view.setUint32(1, this.stream_id, true);
      u8.set(data, 5);
      this.ws.send(buf);
      if (this.stream_type !== client_stream_types.UDP) {
        this.buffer_size--;
      }
    } else {
      this.send_buffer.push(data);
    }
  }

  continue_received(buffer_size) {
    this.buffer_size = buffer_size;
    while (this.buffer_size > 0 && this.send_buffer.length > 0) {
      this.send(this.send_buffer.shift());
    }
  }

  close(reason = 0x01) {
    if (!this.open) return;
    const buf = new ArrayBuffer(6);
    const view = new DataView(buf);
    view.setUint8(0, client_packet_types.CLOSE);
    view.setUint32(1, this.stream_id, true);
    view.setUint8(5, reason);
    this.ws.send(buf);
    this.open = false;
    delete this.connection.active_streams[this.stream_id];
  }
}

export class ClientConnection {
  constructor(wisp_url) {
    if (!wisp_url.endsWith("/")) {
      throw new TypeError("wisp endpoints must end with a trailing forward slash");
    }
    this.wisp_url = wisp_url;
    this.max_buffer_size = 0;
    this.active_streams = {};
    this.connected = false;
    this.connecting = false;
    this.next_stream_id = 1;

    this.onopen = () => {};
    this.onclose = () => {};
    this.onerror = () => {};

    this.connect_ws();
  }

  connect_ws() {
    this.ws = new WebSocket(this.wisp_url);
    this.ws.binaryType = "arraybuffer";
    this.connecting = true;

    this.ws.onerror = () => {
      this.cleanup();
      this.onerror();
    };
    this.ws.onclose = () => {
      this.cleanup();
      this.onclose();
    };
    this.ws.onmessage = (event) => {
      this.on_ws_msg(event);
      if (this.connected && this.connecting) {
        this.connecting = false;
        this.onopen();
      }
    };
  }

  close() {
    this.ws.close();
  }

  create_stream(hostname, port, type=0x01) {
    let stream_type = type;
    if (typeof stream_type === "string") 
      stream_type = type === "udp" ? client_stream_types.UDP : client_stream_types.TCP;

    let stream_id = this.next_stream_id++;
    let stream = new ClientStream(hostname, port, this.ws, this.max_buffer_size, stream_id, this, stream_type);
    this.active_streams[stream_id] = stream;
    stream.open = this.connected;

    const hostBytes = new TextEncoder().encode(hostname);
    const buf = new ArrayBuffer(8 + hostBytes.length);
    const view = new DataView(buf);
    const u8 = new Uint8Array(buf);
    
    view.setUint8(0, client_packet_types.CONNECT);
    view.setUint32(1, stream_id, true);
    view.setUint8(5, stream_type);
    view.setUint16(6, port, true);
    u8.set(hostBytes, 8);
    
    this.ws.send(buf);
    return stream;
  }

  close_stream(stream, reason) {
    stream.onclose(reason);
    delete this.active_streams[stream.stream_id];
  }

  on_ws_msg(event) {
    const buf = new Uint8Array(event.data);
    if (buf.length < 5) return;
    
    const view = new DataView(buf.buffer);
    const type = view.getUint8(0);
    const stream_id = view.getUint32(1, true);
    const payload = buf.slice(5);

    if (stream_id === 0 && this.connecting) {
      if (type === client_packet_types.CONTINUE) {
        this.max_buffer_size = view.getUint32(5, true);
        this.connected = true;
      }
      return;
    }

    const stream = this.active_streams[stream_id];
    if (!stream) return;

    if (type === client_packet_types.DATA) {
      stream.onmessage(payload);
    } else if (type === client_packet_types.CONTINUE) {
      const buf_remaining = view.getUint32(5, true);
      stream.continue_received(buf_remaining);
    } else if (type === client_packet_types.CLOSE) {
      const reason = payload.length > 0 ? payload[0] : 0x01;
      this.close_stream(stream, reason);
    }
  }

  cleanup() {
    this.connected = false;
    this.connecting = false;
    for (const stream_id of Object.keys(this.active_streams)) {
      this.close_stream(this.active_streams[stream_id], 0x03);
    }
  }
}

export class WispWebSocket extends EventTarget {
  constructor(url, protocols=null, options = {}) {
    super();
    this.url = url;
    this.protocols = protocols;
    this.options = options;
    this.binaryType = "blob";
    this.stream = null;
    this.connection = null;

    this.onopen = () => {};
    this.onerror = () => {};
    this.onmessage = () => {};
    this.onclose = () => {};

    this.CONNECTING = 0;
    this.OPEN = 1;
    this.CLOSING = 2;
    this.CLOSED = 3;
    this._ready_state = this.CONNECTING;

    let url_split = this.url.split("/");
    let wsproxy_path = url_split.pop().split(":");
    this.host = wsproxy_path[0];
    this.port = parseInt(wsproxy_path[1]);
    this.real_url = url_split.join("/") + "/";

    this.init_connection();
  }

  on_conn_close() {
    this._ready_state = this.CLOSED;
    this.onerror(new Event("error"));
    this.dispatchEvent(new Event("error"));
  }

  init_connection() {
    this.connection = new ClientConnection(this.real_url, this.options);
    this.connection.onopen = () => {
      this.init_stream();
    };
    this.connection.onclose = () => {
      this.on_conn_close()
    };
    this.connection.onerror = () => {
      this.on_conn_close()
    };
  }

  init_stream() {
    this._ready_state = this.OPEN;
    this.stream = this.connection.create_stream(this.host, this.port);

    this.stream.onmessage = (raw_data) => {
      let data;
      if (this.binaryType == "blob") {
        data = new Blob(raw_data);
      } else if (this.binaryType == "arraybuffer") {
        data = raw_data.buffer;
      } else {
        throw "invalid binaryType string";
      }
      let msg_event = new MessageEvent("message", {data: data});
      this.onmessage(msg_event);
      this.dispatchEvent(msg_event);
    };

    this.stream.onclose = (reason) => {
      this._ready_state = this.CLOSED;
      let close_event = new Event("close");
      this.onclose(close_event);
      this.dispatchEvent(close_event);
    };

    let open_event = new Event("open");
    this.onopen(open_event);
    this.dispatchEvent(open_event);
  }

  send(data) {
    let data_array;

    if (data instanceof Uint8Array) {
      data_array = data;  
    } else if (typeof data === "string") {
      data_array = new TextEncoder().encode(data);
    } else if (data instanceof Blob) {
      data.arrayBuffer().then(array_buffer => {
        this.send(array_buffer);
      });
      return;
    } else if (data instanceof ArrayBuffer) {
      data_array = new Uint8Array(data);
    } else if (ArrayBuffer.isView(data)) {
      data_array = new Uint8Array(data.buffer);
    } else {
      throw "invalid data type to be sent";
    }

    if (!this.stream) {
      throw "websocket is not ready";
    }
    this.stream.send(data_array);
  }

  close() {
    this.stream.close(0x02);
  }

  get extensions() {
    return "";
  }

  get protocol() {
    return "binary";
  }

  get readyState() {
    return this._ready_state;
  }
}
