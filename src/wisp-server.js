// wisp-server.js
// Wisp server implementation for Cloudflare Workers.
// Manages WebSocket connections, the Wisp v1/v2 handshake, stream lifecycle,
// and delegates TCP proxying to the TcpBridge (wisp-tcp.js).

import {
  PacketType,
  StreamType,
  CloseReason,
  ExtensionID,
  parsePacket,
  parseConnectPayload,
  parseInfoPayload,
  buildContinuePacket,
  buildClosePacket,
  buildDataPacket,
  buildInfoPacket,
} from "./wisp-protocol.js";
import { TcpBridge, STREAM_BUFFER_SIZE } from "./wisp-tcp.js";
import {
  serverOptions,
  logging,
  isPortAllowed,
  isIpLiteral,
  isPrivateIp,
  isLoopbackIp,
} from "./wisp-config.js";

const WISP_MAJOR_VERSION = 2;
const WISP_MINOR_VERSION = 0;

// ─── WispStream ──────────────────────────────────────────────────────────────

class WispStream {
  constructor(streamId, server, hostname, port, ctx) {
    this.streamId = streamId;
    this.server = server;
    this.ctx = ctx;
    this.closed = false;

    this.bridge = new TcpBridge(
      streamId,
      hostname,
      port,
      {
        onData: (data) => {
          server.send(buildDataPacket(streamId, data));
        },
        onClose: (reason) => {
          this._onBridgeClose(reason);
        },
        onContinue: (remaining) => {
          server.send(buildContinuePacket(streamId, remaining));
        },
      },
      ctx
    );
  }

  setup() {
    this.bridge.connect().catch(() => {});
  }

  writeData(data) {
    if (this.closed) return;
    this.bridge.sendData(data);
  }

  close(reason = null) {
    if (this.closed) return;
    this.closed = true;
    this.bridge.close();

    if (reason !== null) {
      this.server.send(buildClosePacket(this.streamId, reason));
    }
    this.server.removeStream(this.streamId);
  }

  _onBridgeClose(reason) {
    if (this.closed) return;
    this.closed = true;

    if (reason !== null) {
      this.server.send(buildClosePacket(this.streamId, reason));
    }
    this.server.removeStream(this.streamId);
  }
}

// ─── WispServer ──────────────────────────────────────────────────────────────

export class WispServer {
  constructor(ws, path, wispVersion, ctx) {
    this.ws = ws;
    this.path = path;
    this.wispVersion = wispVersion;
    this.ctx = ctx;
    this.streams = new Map();
    this.handshakeComplete = false;
    this.extensions = [];
  }

  start() {
    this.ws.accept();

    // Use MOTD from config
    this.extensions = [
      {
        id: ExtensionID.MOTD,
        metadata: new TextEncoder().encode(serverOptions.wisp_motd),
      },
      {
        id: ExtensionID.StreamOpenConfirmation,
        metadata: new Uint8Array(0),
      },
    ];

    if (this.wispVersion === 2) {
      logging.log("Starting Wisp v2 handshake...", logging.DEBUG);
      this.send(
        buildInfoPacket(0, WISP_MAJOR_VERSION, WISP_MINOR_VERSION, this.extensions)
      );
    } else {
      logging.log("Starting Wisp v1 handshake...", logging.DEBUG);
      this._completeHandshake();
    }

    this.ws.addEventListener("message", (event) => this._onMessage(event));
    this.ws.addEventListener("close", () => this._onClose());
    this.ws.addEventListener("error", () => this._onClose());
  }

  _completeHandshake() {
    this.handshakeComplete = true;
    this.send(buildContinuePacket(0, STREAM_BUFFER_SIZE));
  }

  send(data) {
    try {
      this.ws.send(data);
    } catch (e) {
      // Silently ignore
    }
  }

  _onMessage(event) {
    if (typeof event.data === "string") return;

    const data =
      event.data instanceof Uint8Array
        ? event.data
        : new Uint8Array(event.data);

    if (data.byteLength < 5) return;

    let packet;
    try {
      packet = parsePacket(data);
    } catch (err) {
      return; 
    }

    if (!this.handshakeComplete) {
      if (packet.type === PacketType.INFO) {
        this._handleClientInfo(packet);
        return;
      }
      if (packet.type === PacketType.CONNECT) {
        this._completeHandshake();
        this._handleConnect(packet);
        return;
      }
      return;
    }

    try {
      this._routePacket(packet);
    } catch (err) {
      // Ignore routing errors
    }
  }

  _handleClientInfo(packet) {
    try {
      parseInfoPayload(packet.payload);
    } catch (err) {
      this.send(buildClosePacket(0, CloseReason.IncompatibleExtensions));
      try { this.ws.close(); } catch (e) {}
      return;
    }
    this._completeHandshake();
  }

  _routePacket(packet) {
    switch (packet.type) {
      case PacketType.CONNECT:
        this._handleConnect(packet);
        break;
      case PacketType.DATA:
        this._handleData(packet);
        break;
      case PacketType.CLOSE:
        this._handleClosePacket(packet);
        break;
      case PacketType.CONTINUE:
        break;
      default:
        break;
    }
  }

  _handleConnect(packet) {
    if (packet.streamId === 0) {
      this.send(buildClosePacket(0, CloseReason.InvalidInfo));
      return;
    }

    const { streamType, port, hostname } = parseConnectPayload(packet.payload);

    if (streamType !== StreamType.TCP) {
      this.send(buildClosePacket(packet.streamId, CloseReason.InvalidInfo));
      return;
    }

    // ─── Config Validation ────────────────────────────────────────────────
    
    // 1. Port Whitelist
    if (!isPortAllowed(port)) {
      logging.log(`Blocked ${hostname}:${port} - Port not in whitelist`, logging.WARN);
      this.send(buildClosePacket(packet.streamId, CloseReason.Blocked));
      return;
    }

    // 2. IP Literal Checks (DNS resolution happens inside cloudflare:sockets, 
    // so we can only block literals here. Workers block private IPs natively regardless).
    if (isIpLiteral(hostname)) {
      if (isPrivateIp(hostname) && !serverOptions.allow_private_ips) {
        logging.log(`Blocked ${hostname}:${port} - Private IP blocked`, logging.WARN);
        this.send(buildClosePacket(packet.streamId, CloseReason.Blocked));
        return;
      }
      if (isLoopbackIp(hostname) && !serverOptions.allow_loopback_ips) {
        logging.log(`Blocked ${hostname}:${port} - Loopback IP blocked`, logging.WARN);
        this.send(buildClosePacket(packet.streamId, CloseReason.Blocked));
        return;
      }
    }

    if (!port || port < 1 || port > 65535 || !hostname) {
      this.send(buildClosePacket(packet.streamId, CloseReason.InvalidInfo));
      return;
    }

    if (this.streams.has(packet.streamId)) {
      this.streams.get(packet.streamId).close(null);
    }

    logging.log(`Opening stream ${packet.streamId} to ${hostname}:${port}`, logging.INFO);

    const stream = new WispStream(
      packet.streamId,
      this,
      hostname,
      port,
      this.ctx
    );
    this.streams.set(packet.streamId, stream);
    stream.setup();
  }

  _handleData(packet) {
    const stream = this.streams.get(packet.streamId);
    if (!stream) return;
    stream.writeData(packet.payload);
  }

  _handleClosePacket(packet) {
    const stream = this.streams.get(packet.streamId);
    if (!stream) return;
    stream.close(null);
  }

  removeStream(streamId) {
    this.streams.delete(streamId);
  }

  _onClose() {
    for (const stream of this.streams.values()) {
      stream.close(null);
    }
    this.streams.clear();
  }
}
