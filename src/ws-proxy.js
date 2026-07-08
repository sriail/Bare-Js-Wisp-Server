// ws-proxy.js
// Legacy wsproxy implementation — proxies a single TCP connection over WebSocket
// where the destination host:port is encoded in the URL path.
// URL format: ws://example.com/prefix/hostname:port

import { connect } from "cloudflare:sockets";

const TCP_CONNECT_TIMEOUT_MS = 15000;

export class WSProxyHandler {
  /**
   * @param {WebSocket} ws - Server-side WebSocket
   * @param {string} path - URL pathname (e.g. "/hostname:port")
   * @param {ExecutionContext|null} ctx - Workers execution context for waitUntil
   */
  constructor(ws, path, ctx) {
    this.ws = ws;
    this.path = path;
    this.ctx = ctx;
    this.socket = null;
    this.writer = null;
    this.reader = null;
    this.closed = false;

    // Write queue state
    this.writeQueue = [];
    this.writing = false;
  }

  /**
   * Race a promise against a timeout.
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

  /**
   * Accept the WebSocket and establish the TCP connection.
   */
  async start() {
    // Parse hostname:port from the last path segment
    const segments = this.path.split("/").filter(Boolean);
    const lastSegment = segments[segments.length - 1];
    const colonIndex = lastSegment.lastIndexOf(":");

    if (colonIndex === -1) {
      this.ws.accept();
      try { this.ws.close(4000, "Invalid URL format"); } catch (e) {}
      return;
    }

    this.hostname = lastSegment.substring(0, colonIndex).trim();
    this.port = parseInt(lastSegment.substring(colonIndex + 1));

    if (!this.port || this.port < 1 || this.port > 65535 || !this.hostname) {
      this.ws.accept();
      try { this.ws.close(4000, "Invalid host or port"); } catch (e) {}
      return;
    }

    this.ws.accept();

    try {
      this.socket = connect(
        { hostname: this.hostname, port: this.port },
        { allowHalfOpen: false }
      );
      // Prevent hanging if the destination is unreachable
      await this._raceTimeout(this.socket.opened, TCP_CONNECT_TIMEOUT_MS);
    } catch (err) {
      try { this.ws.close(4000, "Connection failed"); } catch (e) {}
      return;
    }

    if (this.closed) {
      this.socket.close().catch(() => {});
      return;
    }

    this.writer = this.socket.writable.getWriter();
    this.reader = this.socket.readable.getReader();

    // Start TCP → WebSocket read loop. 
    // We must use ctx.waitUntil so the Worker isn't killed while idle waiting for TCP data.
    const readLoopPromise = this._readLoop();
    if (this.ctx) {
      this.ctx.waitUntil(readLoopPromise);
    }

    // Handle WebSocket → TCP writes
    this.ws.addEventListener("message", (event) => this._onMessage(event));
    this.ws.addEventListener("close", () => this._cleanup());
    this.ws.addEventListener("error", () => this._cleanup());
  }

  /**
   * Read from TCP socket and forward to WebSocket.
   */
  async _readLoop() {
    try {
      while (true) {
        const { done, value } = await this.reader.read();
        if (done) break;
        if (this.closed) break;
        try {
          this.ws.send(value);
        } catch (e) {
          break;
        }
      }
    } catch (err) {
      // Socket read error
    }
    this._cleanup();
  }

  /**
   * Handle incoming WebSocket data — queue and forward to TCP socket sequentially.
   */
  _onMessage(event) {
    if (typeof event.data === "string") return;

    const data = event.data instanceof Uint8Array
      ? event.data
      : new Uint8Array(event.data);

    if (this.closed) return;

    this.writeQueue.push(data);
    this._processWriteQueue();
  }

  /**
   * Process the write queue sequentially to avoid concurrent write races.
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
        this._cleanup();
        return;
      }
    }

    this.writing = false;
  }

  /**
   * Clean up both WebSocket and TCP socket.
   */
  _cleanup() {
    if (this.closed) return;
    this.closed = true;

    if (this.socket) {
      this.socket.close().catch(() => {});
    }

    try { this.ws.close(); } catch (e) {}
  }
}
