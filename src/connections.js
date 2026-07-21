// connections.js
'use strict';

import { WispClient } from './client.js';
import { INDEX_HTML } from './index.js';

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get('Upgrade');

    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      if (url.pathname === '/' || url.pathname === '/index.html') {
        return new Response(INDEX_HTML, {
          headers: { 'Content-Type': 'text/html; charset=utf-8' }
        });
      }
      return new Response('Not Found', { status: 404 });
    }

    if (!url.pathname.endsWith('/')) {
      return new Response('Not Found', { status: 404 });
    }

    const secWsProtocol = request.headers.get('Sec-WebSocket-Protocol');
    if (!secWsProtocol) {
      return new Response('Wisp v1 is not supported by this endpoint', { status: 400 });
    }

    const [clientSocket, serverSocket] = Object.values(new WebSocketPair());
    serverSocket.accept();

    const wispClient = new WispClient(serverSocket);
    ctx.waitUntil(wispClient.run());

    return new Response(null, {
      status: 101,
      webSocket: clientSocket,
      headers: {
        'Sec-WebSocket-Protocol': secWsProtocol.split(',')[0].trim()
      }
    });
  }
};
