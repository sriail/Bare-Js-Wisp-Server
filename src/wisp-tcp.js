// wisp-tcp.js
// WebSocket-to-TCP bridge for Cloudflare Workers.
//
// Cloudflare Workers cannot accept raw inbound TCP connections. This module
// implements the bridge pattern: Wisp WebSocket streams (inbound) are proxied
// to outbound TCP sockets via cloudflare:sockets. Each TcpBridge instance
// manages one TCP connection with proper flow control, error handling, and
// write queue backpressure per the Wisp v2 specification.

import { connect } from "cloudflare:sockets";
import { CloseReason } from "./wisp-protocol.js";

// ─── Configuration ───────────────────────────────────────────────────────────

const STREAM_BUFFER_SIZE = 128;            // Max packets buffered per stream
const CONTINUE_INTERVAL = 64;              // Send CONTINUE every N processed packets
const MAX_QUEUE_SIZE = STREAM_BUFFER_SIZE * 2; // Hard limit to prevent unbounded growth
const TCP_CONNECT_TIMEOUT_MS = 15000;      // TCP connection timeout

// ─── Error Classification ────────────────────────────────────────────────────

/**
 * Map a TCP/socket error to the appropriate Wisp close reason.
 * @param {Error|null} err
 * @returns {number} Wisp CloseReason code
 */
function classifyTcpError(err) {
  if (!err) return CloseReason.ConnectionRefused;
  const msg = (err.message || String(err)).toLowerCase();
  if (
    msg.includes("resolve") ||
    msg.includes("dns") ||
    msg.includes("nxdomain") ||
    msg.includes("nodata") ||
    msg.includes("notfound") ||
    msg.includes("getaddrinfo")
  ) {
    return CloseReason.UnreachableHost;
  }
  if (msg.includes("timeout") || msg.includes("timed out")) {
    return CloseReason.ConnectionTimeout;
  }
  if (
    msg.includes("refused") ||
    msg.includes("reset") ||
    msg.includes("econnrefused") ||
    msg.includes("broken pipe")
  ) {
    return CloseReason.ConnectionRefused;
  }
  return CloseReason.ConnectionRefused;
}

// ─── TcpBridge ───────────────────────────────────────────────────────────────

/**
 * Manages a single outbound TCP connection and bridges it bidirectionally
 * to the Wisp WebSocket layer.
 *
 * Lifecycle:
 *   const bridge = new TcpBridge(streamId, host, port, callbacks, ctx);
 *   bridge.connect();          // Async — establishes TCP connection
 *   bridge.sendData(data);     // Queue data → TCP (with flow control)
 *   bridge.close();            // Close TCP connection
 *
 * Callbacks:
 *   onData(data)           — Data received from TCP socket
 *   onClose(reason)        — TCP connection closed (reason is Wisp code or null)
 *   onContinue(remaining)  — Flow control: buffer space available for client
 */
export class TcpBridge {
  /**
   * @param {number} streamId - Wisp stream ID
   * @param {string} hostname - Destination hostname or IP
   * @param {number} port - Destination TCP port
   * @param {Object} callbacks - { onData, onClose, onContinue }
   * @param {ExecutionContext|null} ctx - Cloudflare Workers execution context
   */
  constructor(streamId, hostname, port, callbacks, ctx) {
    this.streamId = streamId;
    this.hostname = hostname;
    this.port = port;
    this.callbacks = callbacks;
    this.ctx = ctx;

    // TCP socket state
    this.socket = null;
    this.writer = null;
    this.reader = null;

    // Lifecycle flags
    this.closed = false;
    this.connected = false;
    this.writing = false;

    // Write queue — FIFO buffer for data waiting to be written to TCP
    this.writeQueue = [];

    // Flow control counters
    this.packetsProcessed = 0;
    this.packetsReceivedSinceLastContinue = 0;
  }

  // ─── Connection Management ────────────────────────────────────────────────

  /**
   * Establish the outbound TCP connection. Data queued via sendData() before
   * this completes will be flushed once the connection is established.
   */
  async connect() {
    if (this.closed) return;

    try {
      this.socket = connect(
        { hostname: this.hostname, port: this.port },
        { allowHalfOpen: false }
      );

      // Race the connection against a timeout to avoid hanging forever
      await this._raceTimeout(this.socket.opened, TCP_CONNECT_TIMEOUT_MS);
    } catch (err) {
      if (this.closed) return;
      const reason = classifyTcpError(err);
      this._handleClose(reason);
      return;
    }

    // Stream might have been closed while we were connecting
    if (this.closed) {
      this.socket.close().catch(() => {});
      return;
    }

    this.connected = true;
    this.writer = this.socket.writable.getWriter();
    this.reader = this.socket.readable.getReader();

    // Start TCP → WebSocket read loop (runs for the lifetime of the stream)
    this._startReadLoop();

    // Send initial CONTINUE — this serves as the "stream open confirmation"
    // per the Wisp spec extension 0x05. Tells the client the buffer is ready.
    this._sendContinue();

    // Flush any data that was queued before the TCP connection completed.
    // The spec allows clients to send DATA before receiving CONTINUE.
    this._processWriteQueue();
  }

  /**
   * Race a promise against a timeout, clearing the timer on completion.
   * @private
   */
  async _raceTimeout(promise, ms) {
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("timeout")), ms);
    });
    try {
      await Promise.race([promise, timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  // ─── TCP → WebSocket (Read Loop) ──────────────────────────────────────────

  /**
   * Start the TCP read loop. Reads data from the TCP socket and forwards it
   * to the client via the onData callback. The loop exits when the TCP
   * connection closes or the stream is closed.
   * @private
   */
  _startReadLoop() {
    const readLoop = async () => {
      try {
        while (true) {
          const { done, value } = await this.reader.read();
          if (done) break;
          if (this.closed) break;
          // Forward TCP data to the Wisp client
          this.callbacks.onData(value);
        }
      } catch (err) {
        // Read error — fall through to close handling below
      }

      // TCP connection closed (either cleanly via done=true or via error)
      if (!this.closed) {
        this._handleClose(CloseReason.Voluntary);
      }
    };

    // Use ctx.waitUntil to ensure the Worker stays alive for the read loop
    if (this.ctx) {
      this.ctx.waitUntil(readLoop());
    } else {
      readLoop().catch(() => {});
    }
  }

  // ─── WebSocket → TCP (Write Queue) ────────────────────────────────────────

  /**
   * Queue data from the Wisp client to be written to the TCP socket.
   * Data is buffered in a FIFO queue and written sequentially to avoid
   * interleaving. Flow control is enforced via the write queue size.
   *
   * @param {Uint8Array} data - Payload from the client's DATA packet
   */
  sendData(data) {
    if (this.closed) return;

    // Track packets received for mandatory CONTINUE per spec:
    // "The server must send another CONTINUE packet when it has received
    //  the same number of packets from the client as its own maximum buffer size."
    this.packetsReceivedSinceLastContinue++;

    // Prevent unbounded queue growth — close with Throttled reason
    if (this.writeQueue.length >= MAX_QUEUE_SIZE) {
      this._handleClose(CloseReason.Throttled);
      return;
    }

    this.writeQueue.push(data);

    // Mandatory CONTINUE: client has sent STREAM_BUFFER_SIZE packets
    // without receiving a CONTINUE. We must send one per the spec.
    if (this.packetsReceivedSinceLastContinue >= STREAM_BUFFER_SIZE) {
      this._sendContinue();
    }

    // Kick off queue processing (no-op if already running)
    this._processWriteQueue();
  }

  /**
   * Process the write queue — writes data to the TCP socket sequentially.
   * Guarded by `this.writing` to ensure only one writer at a time.
   *
   * CONTINUE packets are sent:
   *   1. Every CONTINUE_INTERVAL packets processed (regular flow control)
   *   2. When the queue drains completely (proactive buffer replenishment)
   *
   * @private
   */
  async _processWriteQueue() {
    if (this.writing || !this.writer || this.closed) return;
    this.writing = true;

    while (this.writeQueue.length > 0 && !this.closed) {
      const data = this.writeQueue.shift();
      try {
        await this.writer.write(data);
      } catch (err) {
        this.writing = false;
        this._handleClose(CloseReason.NetworkError);
        return;
      }

      this.packetsProcessed++;

      // Regular CONTINUE — inform client of available buffer space
      if (this.packetsProcessed % CONTINUE_INTERVAL === 0) {
        this._sendContinue();
      }
    }

    this.writing = false;

    // Queue fully drained — send CONTINUE to give the client maximum buffer.
    // This prevents stalling when the client is waiting for buffer space
    // but the server has already processed everything.
    if (!this.closed && this.connected) {
      this._sendContinue();
    }
  }

  // ─── Flow Control ─────────────────────────────────────────────────────────

  /**
   * Get the number of additional packets the server can buffer for this stream.
   * This is the value sent in CONTINUE packets.
   * @returns {number}
   */
  getBufferRemaining() {
    return Math.max(0, STREAM_BUFFER_SIZE - this.writeQueue.length);
  }

  /**
   * Send a CONTINUE packet via the onContinue callback.
   * Resets the received-since-last-continue counter.
   * @private
   */
  _sendContinue() {
    if (this.closed) return;
    this.packetsReceivedSinceLastContinue = 0;
    const remaining = this.getBufferRemaining();
    this.callbacks.onContinue(remaining);
  }

  // ─── Close / Cleanup ──────────────────────────────────────────────────────

  /**
   * Close the TCP connection silently (no reason sent to client).
   * Used when the client initiates the close.
   */
  close() {
    this._handleClose(null);
  }

  /**
   * Internal close handler. Ensures exactly-once close semantics.
   * @param {number|null} reason - Wisp close reason, or null for silent close
   * @private
   */
  _handleClose(reason) {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;

    // Close the TCP socket — this also causes the read loop to exit
    if (this.socket) {
      this.socket.close().catch(() => {});
    }

    // Notify the Wisp server layer
    this.callbacks.onClose(reason);
  }
}

export { STREAM_BUFFER_SIZE, CONTINUE_INTERVAL, MAX_QUEUE_SIZE };
