'use strict';

import { wispRates } from './rates.js';
import { connect } from 'cloudflare:sockets';
import { INDEX_HTML } from './index.js';
import { WispClient } from './client.js';

// ===================== CONSTANTS =====================
export const packet_types = {
  CONNECT: 0x01,
  DATA: 0x02,
  CONTINUE: 0x03,
  CLOSE: 0x04
};

export const stream_types = {
  TCP: 0x01,
  UDP: 0x02
};

export const close_reasons = {
  NetworkError: 0x01,
  ServerError: 0x02,
  Refused: 0x03,
  Timeout: 0x04,
  Closed: 0x05
};

const rateStore = new Map();
const STORE_MAX_SIZE = 10_000;
const ENTRY_TTL_MS = 120_000; // clean entries older than 2 min

function getRateConfig(name) {
  const cfg = wispRates.wisp_rates.find(r => r.rate === name);
  return cfg;
}

function checkRate(key, rateConfig) {
  if (!rateConfig) return { allowed: true, retryAfter: 0 };

  // both 0 → fully unlimited
  if (rateConfig.request === 0 && rateConfig.time === 0) {
    return { allowed: true, retryAfter: 0 };
  }
  // time 0 → unlimited (no time window)
  if (rateConfig.time === 0) {
    return { allowed: true, retryAfter: 0 };
  }
  // request 0 → unlimited (no request cap)
  if (rateConfig.request === 0) {
    return { allowed: true, retryAfter: 0 };
  }

  const now = Date.now();

  // opportunistic cleanup
  if (rateStore.size > STORE_MAX_SIZE) {
    for (const [k, entry] of rateStore) {
      if (now - entry.startTime > ENTRY_TTL_MS) rateStore.delete(k);
    }
  }

  let entry = rateStore.get(key);
  if (!entry || now - entry.startTime >= rateConfig.time) {
    rateStore.set(key, { startTime: now, count: 1 });
    return { allowed: true, retryAfter: 0 };
  }

  if (entry.count >= rateConfig.request) {
    const retryAfter = Math.ceil((rateConfig.time - (now - entry.startTime)) / 1000);
    return { allowed: false, retryAfter: Math.max(retryAfter, 1) };
  }

  entry.count++;
  return { allowed: true, retryAfter: 0 };
}
// ===================== STREAM CLASSES =====================
export class ServerStream {
  static buffer_size = 524288; // 512 KB — referenced by client.js

  constructor(streamID, client, hostname, port, streamType) {
    this.streamID = streamID;
    this.client = client;
    this.hostname = hostname;
    this.port = port;
    this.streamType = streamType;
    this.socket = null;
    this.writer = null;
    this.reader = null;
    this.closed = false;
    this.pendingData = [];
  }

  async setup() {
    try {
      this.socket = connect(
        { hostname: this.hostname, port: this.port },
        { secureTransport: 'off' }
      );
      this.writer = this.socket.writable.getWriter();
      this.reader = this.socket.readable.getReader();

      // flush any data that arrived before the socket was ready
      while (this.pendingData.length > 0) {
        const data = this.pendingData.shift();
        await this.writer.write(data);
      }

      // initial CONTINUE — tell client it can send data
      const payload = new ArrayBuffer(4);
      new DataView(payload).setUint32(0, ServerStream.buffer_size, true);
      this.client.send_packet(packet_types.CONTINUE, this.streamID, payload);

      this._readLoop();
    } catch (err) {
      console.error('ServerStream setup error:', err);
      await this.close(close_reasons.NetworkError);
    }
  }

  async _readLoop() {
    try {
      while (!this.closed) {
        const { done, value } = await this.reader.read();
        if (done) break;
        const buf = value.buffer.slice(
          value.byteOffset,
          value.byteOffset + value.byteLength
        );
        this.client.send_packet(packet_types.DATA, this.streamID, buf);
      }
    } catch (_) {
      // socket read error / closed
    }
    if (!this.closed) await this.close(close_reasons.Closed);
  }

  put_data(data) {
    if (this.closed) return;
    if (this.writer) {
      this.writer.write(data).catch(() => {});
    } else {
      this.pendingData.push(data);
    }
  }

  async close(reason) {
    if (this.closed) return;
    this.closed = true;

    if (reason !== null) {
      const payload = new Uint8Array([reason & 0xff]);
      this.client.send_packet(packet_types.CLOSE, this.streamID, payload.buffer);
    }

    try { if (this.writer) await this.writer.close(); } catch (_) {}
    try { if (this.socket) this.socket.close(); } catch (_) {}
  }
}

export class FetchStream {
  constructor(streamID, client, hostname, port, streamType) {
    this.streamID = streamID;
    this.client = client;
    this.hostname = hostname;
    this.port = port;
    this.streamType = streamType;
    this.closed = false;
    this.requestBuffer = [];
    this.fetchStarted = false;
    this.abortController = new AbortController();
  }

  async setup() {
    // initial CONTINUE
    const payload = new ArrayBuffer(4);
    new DataView(payload).setUint32(0, ServerStream.buffer_size, true);
    this.client.send_packet(packet_types.CONTINUE, this.streamID, payload);
  }

  put_data(data) {
    if (this.closed || this.fetchStarted) return;
    this.requestBuffer.push(data);

    // accumulate chunks until we have the full HTTP request header
    const totalLen = this.requestBuffer.reduce((a, c) => a + c.length, 0);
    const combined = new Uint8Array(totalLen);
    let off = 0;
    for (const chunk of this.requestBuffer) {
      combined.set(chunk, off);
      off += chunk.length;
    }

    const text = new TextDecoder().decode(combined);
    const headerEnd = text.indexOf('\r\n\r\n');
    if (headerEnd === -1) return; // need more data

    this.fetchStarted = true;
    this.requestBuffer = [];

    // parse HTTP request line + headers
    const headerText = text.substring(0, headerEnd);
    const lines = headerText.split('\r\n');
    const [method, path] = lines[0].split(' ');
    if (!method || !path) {
      this.close(close_reasons.ServerError);
      return;
    }

    const headers = {};
    for (let i = 1; i < lines.length; i++) {
      const idx = lines[i].indexOf(':');
      if (idx > 0) {
        const k = lines[i].substring(0, idx).trim();
        const v = lines[i].substring(idx + 1).trim();
        if (k.toLowerCase() !== 'host') headers[k] = v;
      }
    }

    const body = combined.slice(headerEnd + 4);
    const protocol =
      this.port === 443 || this.port === 8443 ? 'https' : 'http';
    const url = `${protocol}://${this.hostname}${path}`;

    const options = {
      method,
      headers,
      signal: this.abortController.signal,
    };
    if (method !== 'GET' && method !== 'HEAD' && body.byteLength > 0) {
      options.body = body;
    }

    this._doFetch(url, options);
  }

  async _doFetch(url, options) {
    try {
      const response = await fetch(url, options);

      // build raw HTTP response head
      let head = `HTTP/1.1 ${response.status} ${response.statusText}\r\n`;
      response.headers.forEach((v, k) => { head += `${k}: ${v}\r\n`; });
      head += '\r\n';
      const headBytes = new TextEncoder().encode(head);
      this.client.send_packet(packet_types.DATA, this.streamID, headBytes.buffer);

      // stream body
      if (response.body) {
        const reader = response.body.getReader();
        while (!this.closed) {
          const { done, value } = await reader.read();
          if (done) break;
          const buf = value.buffer.slice(
            value.byteOffset,
            value.byteOffset + value.byteLength
          );
          this.client.send_packet(packet_types.DATA, this.streamID, buf);
        }
        try { await reader.cancel(); } catch (_) {}
      }

      if (!this.closed) await this.close(close_reasons.Closed);
    } catch (_) {
      if (!this.closed) await this.close(close_reasons.NetworkError);
    }
  }

  async close(reason) {
    if (this.closed) return;
    this.closed = true;

    if (reason !== null) {
      const payload = new Uint8Array([reason & 0xff]);
      this.client.send_packet(packet_types.CLOSE, this.streamID, payload.buffer);
    }
    try { this.abortController.abort(); } catch (_) {}
  }
}
// ===================== END STREAM CLASSES =====================

// ===================== MAIN FETCH HANDLER =====================
export default {
  async fetch(request, env, ctx) {
    const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
    const upgradeHeader = request.headers.get('Upgrade');

    // ---------- Non-WebSocket: serve the test page (test-rate) ----------
    if (upgradeHeader !== 'websocket') {
      const testRate = getRateConfig('test-rate');
      const rc = checkRate(`test:${ip}`, testRate);

      if (!rc.allowed) {
        return new Response('Rate limit exceeded. Please try again later.', {
          status: 429,
          headers: {
            'Content-Type': 'text/plain',
            'Retry-After': String(rc.retryAfter),
          },
        });
      }

      return new Response(INDEX_HTML, {
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }

    // ---------- WebSocket: determine traffic source ----------
    //
    //  • If the WebSocket Origin header matches the worker's own origin,
    //    the connection is coming from the test page → test-rate.
    //  • Otherwise it's a service / direct relay call → main-rate.
    //
    const origin = request.headers.get('Origin');
    const serverOrigin = new URL(request.url).origin;
    const isFromTestPage = origin === serverOrigin;

    const rateName = isFromTestPage ? 'test-rate' : 'main-rate';
    const rateConfig = getRateConfig(rateName);
    const rc = checkRate(`${rateName}:${ip}`, rateConfig);

    if (!rc.allowed) {
      return new Response('Rate limit exceeded.', {
        status: 429,
        headers: {
          'Content-Type': 'text/plain',
          'Retry-After': String(rc.retryAfter),
        },
      });
    }

    // ---------- Create WebSocket pair & hand off to WispClient ----------
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    const wispClient = new WispClient(server);
    ctx.passThroughOnException();
    wispClient.run();

    return new Response(null, { status: 101, webSocket: client });
  },
};
// ===================== END MAIN FETCH HANDLER =====================
