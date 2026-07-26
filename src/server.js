// src/server.js
'use strict';

// ========== Imports ==========
import { connect } from 'cloudflare:sockets';
import { INDEX_HTML } from './index.js';
import { wispRates } from './rates.js';

// ========== Wisp Protocol Constants ==========
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
  Unspecified: 0x01,
  Voluntary: 0x02,
  NetworkError: 0x03,
  InvalidInfo: 0x41,
  UnreachableHost: 0x42,
  Timeout: 0x43,
  ConnectionRefused: 0x44,
  TcpTimeout: 0x47,
  Blocked: 0x48,
  Throttled: 0x49,
  ClientError: 0x81
};

// ========== ServerStream (TCP via cloudflare:sockets) ==========
export class ServerStream {
  static buffer_size = 128; // Max packets to buffer

  constructor(stream_id, client, hostname, port, stream_type) {
    this.stream_id = stream_id;
    this.client = client;
    this.hostname = hostname;
    this.port = port;
    this.stream_type = stream_type;
    this.closed = false;
    this.socket = null;
    this.writer = null;
    this.recv_count = 0;
  }

  async setup() {
    try {
      this.socket = connect({ hostname: this.hostname, port: this.port });
      this.writer = this.socket.writable.getWriter();

      // Send initial CONTINUE with buffer size
      const buf = new ArrayBuffer(4);
      new DataView(buf).setUint32(0, ServerStream.buffer_size, true);
      this.client.send_packet(packet_types.CONTINUE, this.stream_id, buf);

      // Begin reading from remote → forwarding to WS client
      this.readLoop();
    } catch (err) {
      console.error('ServerStream setup error:', err);
      await this.close(close_reasons.ConnectionRefused);
    }
  }

  async readLoop() {
    const reader = this.socket.readable.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        this.client.send_packet(packet_types.DATA, this.stream_id, value.buffer);
      }
    } catch {
      // Socket read error — fall through to close
    } finally {
      try { reader.releaseLock(); } catch {}
      await this.close(close_reasons.NetworkError);
    }
  }

  put_data(data) {
    if (this.closed || !this.writer) return;
    this.writer.write(data).catch(() => {});
  }

  async close(reason) {
    if (this.closed) return;
    this.closed = true;
    if (this.writer) { try { await this.writer.close(); } catch {} }
    if (reason !== null) {
      const p = new Uint8Array(1);
      p[0] = reason;
      this.client.send_packet(packet_types.CLOSE, this.stream_id, p.buffer);
    }
  }
}

// ========== FetchStream (HTTP via fetch()) ==========
export class FetchStream {
  static buffer_size = 128;

  constructor(stream_id, client, hostname, port, stream_type) {
    this.stream_id = stream_id;
    this.client = client;
    this.hostname = hostname;
    this.port = port;
    this.stream_type = stream_type;
    this.closed = false;
    this.chunks = [];
    this.fetchStarted = false;
    this.recv_count = 0;
  }

  async setup() {
    const buf = new ArrayBuffer(4);
    new DataView(buf).setUint32(0, FetchStream.buffer_size, true);
    this.client.send_packet(packet_types.CONTINUE, this.stream_id, buf);
  }

  put_data(data) {
    if (this.closed || this.fetchStarted) return;
    this.chunks.push(data);
    this._tryParse();
  }

  _combined() {
    const total = this.chunks.reduce((a, c) => a + c.length, 0);
    const out = new Uint8Array(total);
    let off = 0;
    for (const c of this.chunks) { out.set(c, off); off += c.length; }
    return out;
  }

  _tryParse() {
    const raw = this._combined();
    const text = new TextDecoder().decode(raw);
    const hdrEnd = text.indexOf('\r\n\r\n');
    if (hdrEnd === -1) return; // headers not complete yet

    const hdrText = text.slice(0, hdrEnd);
    const clMatch = hdrText.match(/content-length:\s*(\d+)/i);
    const cl = clMatch ? parseInt(clMatch[1], 10) : 0;
    const bodyStart = hdrEnd + 4;
    if (raw.length - bodyStart < cl) return; // body not fully received

    this.fetchStarted = true;
    this.chunks = [];
    this._fetch(text, raw, bodyStart, cl);
  }

  async _fetch(text, raw, bodyStart, cl) {
    const proto = (this.port === 443 || this.port === 8443) ? 'https' : 'http';
    const lines = text.split('\r\n');
    const [method, path] = lines[0].split(' ');
    const url = `${proto}://${this.hostname}${path || '/'}`;

    const h = new Headers();
    for (let i = 1; i < lines.length; i++) {
      if (!lines[i]) break;
      const ci = lines[i].indexOf(':');
      if (ci < 0) continue;
      const k = lines[i].slice(0, ci).trim();
      const v = lines[i].slice(ci + 1).trim();
      const lk = k.toLowerCase();
      if (lk !== 'host' && lk !== 'connection' && lk !== 'content-length') {
        h.set(k, v);
      }
    }

    let body = undefined;
    const m = (method || 'GET').toUpperCase();
    if (cl > 0 && m !== 'GET' && m !== 'HEAD') {
      body = raw.slice(bodyStart, bodyStart + cl);
    }

    try {
      const resp = await fetch(url, { method: m, headers: h, body });

      // Build HTTP/1.1 response text
      let respText = `HTTP/1.1 ${resp.status} ${resp.statusText}\r\n`;
      resp.headers.forEach((v, k) => { respText += `${k}: ${v}\r\n`; });
      respText += '\r\n';
      const hb = new TextEncoder().encode(respText);
      this.client.send_packet(packet_types.DATA, this.stream_id, hb.buffer);

      // Stream response body
      if (resp.body) {
        const reader = resp.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          this.client.send_packet(packet_types.DATA, this.stream_id, value.buffer);
        }
      }
      await this.close(null);
    } catch (err) {
      console.error('FetchStream error:', err);
      await this.close(close_reasons.NetworkError);
    }
  }

  async close(reason) {
    if (this.closed) return;
    this.closed = true;
    if (reason !== null) {
      const p = new Uint8Array(1);
      p[0] = reason;
      this.client.send_packet(packet_types.CLOSE, this.stream_id, p.buffer);
    }
  }
}

// ========== Rate Limiter ==========
class RateLimiter {
  constructor() {
    this.hard = new Map();       // time-windowed buckets
    this.throttle = new Map();   // throttle buckets (time:0 mode)
    this.lastClean = Date.now();
  }

  _cfg(name) {
    return wispRates.wisp_rates.find(r => r.rate === name);
  }

  checkHard(name, id) {
    const c = this._cfg(name);
    if (!c) return { allowed: true };
    if (c.request === 0 && c.time === 0) return { allowed: true };
    if (c.time === 0) return { allowed: true };

    const key = `${name}:${id}`;
    const now = Date.now();
    let b = this.hard.get(key);

    if (!b || now > b.reset) {
      b = { n: 1, reset: now + c.time };
      this.hard.set(key, b);
      return { allowed: true, remaining: c.request - 1 };
    }

    b.n++;
    if (b.n > c.request) {
      return { allowed: false, retryAfter: b.reset - now };
    }
    return { allowed: true, remaining: c.request - b.n };
  }

  async checkThrottle(name, id) {
    const c = this._cfg(name);
    if (!c) return true;
    if (c.request === 0 && c.time === 0) return true;
    if (c.time > 0) return this.checkHard(name, id).allowed;

    const key = `${name}:${id}`;
    const now = Date.now();
    let b = this.throttle.get(key);

    if (!b) {
      b = { n: 1, start: now };
      this.throttle.set(key, b);
      return true;
    }

    b.n++;

    if (now - b.start > 60000) {
      b.n = 1;
      b.start = now;
      return true;
    }

    if (b.n > c.request) {
      const delay = Math.min(500, (b.n - c.request) * 5);
      await new Promise(r => setTimeout(r, delay));
    }

    return true;
  }

  maybeClean() {
    const now = Date.now();
    if (now - this.lastClean < 300000) return; // 5 min
    this.lastClean = now;
    for (const [k, b] of this.hard) {
      if (now > b.reset) this.hard.delete(k);
    }
    for (const [k, b] of this.throttle) {
      if (now - b.start > 60000) this.throttle.delete(k);
    }
  }
}

const limiter = new RateLimiter();

// ========== Wisp Server Connection Handler ==========
export class WispServer {
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

// ========== Cloudflare Worker Entry Point ==========
export default {
  async fetch(request, env, ctx) {
    limiter.maybeClean();

    const url = new URL(request.url);
    const ip = request.headers.get('CF-Connecting-IP') || 'unknown';

    // --- WebSocket upgrade → main-rate (throttled) ---
    if (request.headers.get('Upgrade') === 'websocket') {
      const ok = await limiter.checkThrottle('main-rate', ip);
      if (!ok) {
        return new Response('Rate limit exceeded.', { status: 429 });
      }

      const [clientWs, serverWs] = Object.values(new WebSocketPair());
      serverWs.accept();

      const wisp = new WispServer(serverWs);
      ctx.waitUntil(wisp.run().catch(e => console.error('WispServer:', e)));

      return new Response(null, { status: 101, webSocket: clientWs });
    }

    // --- Test page → test-rate (hard limit) ---
    if (url.pathname === '/' || url.pathname === '/index.html') {
      const r = limiter.checkHard('test-rate', ip);
      if (!r.allowed) {
        return new Response('Rate limit exceeded. Try again later.', {
          status: 429,
          headers: {
            'Retry-After': String(Math.ceil((r.retryAfter || 60000) / 1000)),
            'Content-Type': 'text/plain'
          }
        });
      }

      return new Response(INDEX_HTML, {
        headers: { 'Content-Type': 'text/html; charset=utf-8' }
      });
    }

    return new Response('Not Found', { status: 404 });
  }
};
