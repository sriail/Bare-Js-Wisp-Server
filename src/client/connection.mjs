import * as compat from "./compat.mjs";
import {
  packet_classes,
  packet_types,
  stream_types,
  WispBuffer, 
  WispPacket, 
  ConnectPayload, 
  DataPayload, 
  ClosePayload,
  InfoPayload
} from "./packet.mjs";
import { MOTDExtension, UDPExtension, serialize_extensions, parse_extensions } from "./extensions.mjs";

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
    if (this.buffer_size > 0 || !this.open || this.stream_type === stream_types.UDP) {
      let packet = new WispPacket({
        type: packet_types.DATA,
        stream_id: this.stream_id,
        payload: new DataPayload({ data: new WispBuffer(data) })
      });
      this.ws.send(packet.serialize().bytes);
      this.buffer_size--;
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
    let packet = new WispPacket({
      type: packet_types.CLOSE,
      stream_id: this.stream_id,
      payload: new ClosePayload({ reason: reason })
    });
    this.ws.send(packet.serialize().bytes);
    this.open = false;
    delete this.connection.active_streams[this.stream_id];
  }
}

export class ClientConnection {
  constructor(wisp_url, {wisp_version, wisp_extensions} = {}) {
    if (!wisp_url.endsWith("/")) {
      throw new TypeError("wisp endpoints must end with a trailing forward slash");
    }

    this.wisp_url = wisp_url;
    this.wisp_version = wisp_version || 2;
    this.wisp_extensions = wisp_extensions || null;

    this.max_buffer_size = null;
    this.active_streams = {};
    this.connected = false;
    this.connecting = false;
    this.next_stream_id = 1;

    this.server_exts = {};
    this.client_exts = {};
    this.info_received = false;
    this.server_motd = null;
    this.udp_enabled = true;

    this.onopen = () => {};
    this.onclose = () => {};
    this.onerror = () => {};
    this.onmessage = () => {};

    if (this.wisp_version === 2 && this.wisp_extensions === null) {
      this.add_extensions();
    }

    this.connect_ws();
  }

  add_extensions() {
    this.wisp_extensions = [];
    this.wisp_extensions.push(new UDPExtension({client_config: {}}));
    this.wisp_extensions.push(new MOTDExtension({client_config: {}}));
  }

  connect_ws() {
    let subprotocol = this.wisp_version === 2 ? "wisp-v2" : undefined;
    this.ws = new compat.WebSocket(this.wisp_url, subprotocol);
    this.ws.binaryType = "arraybuffer";
    this.connecting = true;

    this.ws.onerror = () => {
      if (this.wisp_version === 2) {
        this.ws.onclose = null;
        this.cleanup();
        this.wisp_version = 1;
        this.connect_ws();
        return;
      }
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
      stream_type = type === "udp" ? stream_types.UDP : stream_types.TCP;

    if (stream_type == stream_types.UDP && !this.udp_enabled) {
      throw new Error("udp is not enabled for this wisp connection");
    }

    let stream_id = this.next_stream_id++;
    let stream = new ClientStream(hostname, port, this.ws, this.max_buffer_size, stream_id, this, stream_type);
    this.active_streams[stream_id] = stream;
    stream.open = this.connected;

    let packet = new WispPacket({
      type: packet_types.CONNECT,
      stream_id: stream_id,
      payload: new ConnectPayload({
        stream_type: stream_type,
        port: port,
        hostname: hostname
      })
    });
    this.ws.send(packet.serialize().bytes);
    return stream;
  }

  close_stream(stream, reason) {
    stream.onclose(reason);
    delete this.active_streams[stream.stream_id];
  }

  on_ws_msg(event) {
    let buffer = new WispBuffer(new Uint8Array(event.data));
    if (buffer.size < WispPacket.min_size) {
      console.warn(`wisp client warning: received a packet which is too short`);
      return;
    }
    let packet = WispPacket.parse_all(buffer);
    let stream = this.active_streams[packet.stream_id];
    if (packet.stream_id === 0 && this.connecting) {
      if (packet.type === packet_types.CONTINUE) {
        this.max_buffer_size = packet.payload.buffer_remaining;
        this.connected = true;
        if (!this.info_received) {
          this.wisp_version = 1;
        }
      }
      return;
    }

    if (typeof stream === "undefined") {
      console.warn(`wisp client warning: received a ${packet_classes[packet.type].name} packet for a stream which doesn't exist`);
      return;
    }

    if (packet.type === packet_types.DATA) {
      stream.onmessage(packet.payload_bytes.bytes);
    } else if (packet.type === packet_types.CONTINUE) {
      stream.continue_received(packet.payload.buffer_remaining);
    } else if (packet.type === packet_types.CLOSE) {
      this.close_stream(stream, packet.payload.reason);
    } else {
      console.warn(`wisp client warning: received an invalid packet of type ${packet.type}`);
    }
  }

  cleanup() {
    this.connected = false;
    this.connecting = false;
    for (let stream_id of Object.keys(this.active_streams)) {
      this.close_stream(this.active_streams[stream_id], 0x03);
    }
  }
}
