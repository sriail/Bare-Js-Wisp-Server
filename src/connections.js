// connections.js
'use strict';

import { Client } from './client.js';
import { INDEX_HTML } from './index.js';

export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get('Upgrade');

    // 1. If no Upgrade header is present, serve the test HTML page
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response(INDEX_HTML, {
        headers: { 'Content-Type': 'text/html; charset=utf-8' }
      });
    }

    const url = new URL(request.url);
    
    // Spec: The URL of the websocket should always end with a trailing forward slash (/)
    if (!url.pathname.endsWith('/')) {
      return new Response('Not Found', { status: 404 });
    }

    // Spec: The Sec-WebSocket-Protocol request header must be present for Wisp v2
    const secWsProtocol = request.headers.get('Sec-WebSocket-Protocol');
    if (!secWsProtocol) {
      return new Response('Wisp v1 is not supported by this endpoint', { status: 400 });
    }

    // Establish WebSocket pair
    const [clientSocket, serverSocket] = Object.values(new WebSocketPair());
    serverSocket.accept();

    // Instantiate the Wisp Client handler
    const wispClient = new Client(serverSocket);
    
    // Keep the Worker alive for the duration of the WebSocket connection
    ctx.waitUntil(wispClient.initialize());

    // Return the WebSocket response to the client, echoing the subprotocol
    return new Response(null, {
      status: 101,
      webSocket: clientSocket,
      headers: {
        'Sec-WebSocket-Protocol': secWsProtocol.split(',')[0].trim()
      }
    });
  }
};
