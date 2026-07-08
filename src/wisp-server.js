// wisp-server.js
// Wisp server implementation for Cloudflare Workers.
// Manages WebSocket connections, the Wisp v1/v2 handshake, stream lifecycle,
// and delegates TCP proxying to the TcpBridge (wisp-tcp.js).
//
// Architecture:
//   WebSocket (inbound) ←→ WispServer (packet routing) ←→ TcpBridge (outbound TCP)
//
// The WispServer handles protocol-level concerns (handshake, packet parsing,
// stream management) while TcpBridge handles the TCP connection lifecycle and
// flow control. This separation ensures a stable routing pipeline.

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

// ─── Protocol Version ────────────────────────────────────────────────────────

const WISP_MAJOR_VERSION = 2;
const WISP_MINOR_VERSION = 0;

// ─── WispStream ──────────────────────────────────────────────────────────────

/**
 * Represents a single Wisp stream and its associated TCP connection.
 * Acts as a thin adapter between the WispServer (WebSocket protocol) and
 * the TcpBridge (TCP socket management).
 */
class WispStream {
  /**
   * @param {number} streamId - Wisp stream ID (must be > 0)
   * @param {WispServer} server - Parent Wisp server instance
   * @param {string} hostname - Destination hostname
   * @param {number} port - Destination port
   * @param {ExecutionContext|null} ctx - Workers execution context for waitUntil
   */
  constructor(streamId, server, hostname, port, ctx) {
    this.streamId = streamId;
    this.server = server;
    this.ctx = ctx;
    this.closed = false;

    // Create the TCP bridge with callbacks that wire back into the Wisp protocol
    this.bridge = new TcpBridge(
      streamId,
      hostname,
      port,
      {
        // TCP → WebSocket: forward data from TCP to the client as DATA packets
        onData: (data) => {
          server.send(buildDataPacket(streamId, data));
        },
        // TCP closed: notify the client with a CLOSE packet (unless silent)
        onClose: (reason) => {
          this._onBridgeClose(reason);
        },
        // Flow control: forward CONTINUE packets to the client
        onContinue: (remaining) => {
          server.send(buildContinuePacket(streamId, remaining));
        },
      },
      ctx
    );
  }

  /**
   * Start the TCP connection. Data can be queued via writeData() before
   * this completes — it will be flushed once the connection is established.
   */
  setup() {
    this.bridge
      .connect()
      .catch(() => {
        // All errors are handled inside connect() via the onClose callback.
        // This catch prevents unhandled promise rejections.
      });
  }

  /**
   * Queue data from the client to be written to the TCP socket.
   * @param {Uint8Array} data
   */
  writeData(data) {
    if (this.closed) return;
    this.bridge.sendData(data);
  }

  /**
   * Close the stream. If a reason is provided, a CLOSE packet is sent to
   * the client. The TCP bridge is closed silently.
   * @param {number|null} reason - Wisp close reason, or null for silent close
   */
  close(reason = null) {
    if (this.closed) return;
    this.closed = true;

    // Close the TCP bridge (triggers onClose with null reason, but we're
    // already marked closed so _onBridgeClose will be a no-op)
    this.bridge.close();

    if (reason !== null) {
      this.server.send(buildClosePacket(this.streamId, reason));
    }

    this.server.removeStream(this.streamId);
  }

  /**
   * Called when the TcpBridge closes (either TCP error or normal EOF).
   * Sends a CLOSE packet to the client with the appropriate reason.
   * @param {number|null} reason
   * @private
   */
  _onBridgeClose(reason) {
    // Already closed by close() or previous _onBridgeClose — no-op
    if (this.closed) return;
    this.closed = true;

    if (reason !== null) {
      this.server.send(buildClosePacket(this.streamId, reason));
    }

    this.server.removeStream(this.streamId);
  }
}

// ─── WispServer ──────────────────────────────────────────────────────────────

/**
 * Wisp server — manages a single WebSocket connection and all streams
 * multiplexed over it.
 */
export class WispServer {
  /**
   * @param {WebSocket} ws - Server-side WebSocket from WebSocketPair
   * @param {string} path - URL pathname (for potential gatekeeping)
   * @param {number} wispVersion - 1 or 2, determined by Sec-WebSocket-Protocol
   * @param {ExecutionContext|null} ctx - Workers execution context
   */
  constructor(ws, path, wispVersion, ctx) {
    this.ws = ws;
    this.path = path;
    this.wispVersion = wispVersion;
    this.ctx = ctx;
    this.streams = new Map();
    this.handshakeComplete = false;
    this.extensions = [];
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  /**
   * Accept the WebSocket and begin the Wisp handshake.
   */
  start() {
    this.ws.accept();

    // Build server-side extensions
    // NOTE: No UDP extension — Workers don't support UDP sockets.
    this.extensions = [
      {
        id: ExtensionID.MOTD,
        metadata: new TextEncoder().encode(
          "Wisp server on Cloudflare Workers — TCP only"
        ),
      },
      {
        id: ExtensionID.StreamOpenConfirmation,
        metadata: new Uint8Array(0),
      },
    ];

    if (this.wispVersion === 2) {
      // V2: send INFO packet first, wait for client INFO, then send CONTINUE
      this.send(
        buildInfoPacket(0, WISP_MAJOR_VERSION, WISP_MINOR_VERSION, this.extensions)
      );
    } else {
      // V1: send CONTINUE immediately, no INFO exchange
      this._completeHandshake();
    }

    this.ws.addEventListener("message", (event) => this._onMessage(event));
    this.ws.addEventListener("close", () => this._onClose());
    this.ws.addEventListener("error", () => this._onClose());
  }

  /**
   * Complete the Wisp handshake by sending the initial CONTINUE packet
   * with stream ID 0. This tells the client the per-stream buffer size.
   */
  _completeHandshake() {
    this.handshakeComplete = true;
    this.send(buildContinuePacket(0, STREAM_BUFFER_SIZE));
  }

  // ─── WebSocket I/O ────────────────────────────────────────────────────────

  /**
   * Safely send data on the WebSocket, ignoring errors if already closed.
   * @param {Uint8Array} data
   */
  send(data) {
    try {
      this.ws.send(data);
    } catch (e) {
      // WebSocket is closed or closing — silently ignore
    }
  }

  /**
   * Handle incoming WebSocket messages.
   * @param {MessageEvent} event
   */
  _onMessage(event) {
    // Ignore text frames — Wisp uses binary only
    if (typeof event.data === "string") return;

    const data =
      event.data instanceof Uint8Array
        ? event.data
        : new Uint8Array(event.data);

    // Minimum packet size: 1 byte type + 4 bytes stream ID
    if (data.byteLength < 5) return;

    let packet;
    try {
      packet = parsePacket(data);
    } catch (err) {
      return; // Malformed packet — ignore
    }

    // ─── Handshake Phase ──────────────────────────────────────────────────
    if (!this.handshakeComplete) {
      if (packet.type === PacketType.INFO) {
        // V2 handshake: client sent INFO, process it and complete handshake
        this._handleClientInfo(packet);
        return;
      }
      if (packet.type === PacketType.CONNECT) {
        // Client sent CONNECT before completing handshake.
        // This can happen with impatient V2 clients or V1 fallback.
        // Complete the handshake first (sends initial CONTINUE), then
        // process the CONNECT. This ensures the client always receives
        // the initial buffer size via the stream-ID-0 CONTINUE.
        this._completeHandshake();
        this._handleConnect(packet);
        return;
      }
      // Ignore other packet types during handshake
      return;
    }

    // ─── Established Phase ────────────────────────────────────────────────
    try {
      this._routePacket(packet);
    } catch (err) {
      // Ignore routing errors to prevent cascading failures
    }
  }

  /**
   * Process the client's INFO packet and complete the V2 handshake.
   * @private
   */
  _handleClientInfo(packet) {
    try {
      parseInfoPayload(packet.payload);
    } catch (err) {
      // Malformed INFO — reject with incompatible extensions
      this.send(buildClosePacket(0, CloseReason.IncompatibleExtensions));
      try {
        this.ws.close();
      } catch (e) {}
      return;
    }

    this._completeHandshake();
  }

  // ─── Packet Routing ───────────────────────────────────────────────────────

  /**
   * Route an established-connection packet to the appropriate handler.
   * @private
   */
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
        // Client should never send CONTINUE — ignore
        break;
      default:
        // Unknown packet type — ignore
        break;
    }
  }

  /**
   * Handle a CONNECT packet — create a new TCP stream.
   * @private
   */
  _handleConnect(packet) {
    // Stream ID 0 is reserved for the handshake — reject
    if (packet.streamId === 0) {
      this.send(buildClosePacket(0, CloseReason.InvalidInfo));
      return;
    }

    const { streamType, port, hostname } = parseConnectPayload(packet.payload);

    // Workers only support TCP — reject UDP with InvalidInfo
    if (streamType !== StreamType.TCP) {
      this.send(buildClosePacket(packet.streamId, CloseReason.InvalidInfo));
      return;
    }

    // Validate port and hostname
    if (!port || port < 1 || port > 65535 || !hostname) {
      this.send(buildClosePacket(packet.streamId, CloseReason.InvalidInfo));
      return;
    }

    // Close existing stream with the same ID if it exists (stream reuse)
    if (this.streams.has(packet.streamId)) {
      this.streams.get(packet.streamId).close(null);
    }

    // Create and start the new stream
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

  /**
   * Handle a DATA packet — forward payload to the TCP socket.
   * @private
   */
  _handleData(packet) {
    const stream = this.streams.get(packet.streamId);
    if (!stream) return;
    stream.writeData(packet.payload);
  }

  /**
   * Handle a CLOSE packet from the client — tear down the stream silently.
   * Per spec: "Any CLOSE packets sent from either the server or the client
   * must immediately close the associated stream and TCP socket."
   * @private
   */
  _handleClosePacket(packet) {
    const stream = this.streams.get(packet.streamId);
    if (!stream) return;
    // Client-initiated close — don't send CLOSE back (null reason)
    stream.close(null);
  }

  // ─── Stream Management ────────────────────────────────────────────────────

  /**
   * Remove a stream from the active map.
   * @param {number} streamId
   */
  removeStream(streamId) {
    this.streams.delete(streamId);
  }

  /**
   * Handle WebSocket closure — clean up ALL streams.
   * @private
   */
  _onClose() {
    for (const stream of this.streams.values()) {
      stream.close(null);
    }
    this.streams.clear();
  }
}
