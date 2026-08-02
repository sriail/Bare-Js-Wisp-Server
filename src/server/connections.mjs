import { ConnectionHandler } from "./connectionHandler.mjs";

export class WispHandler {
  static async handle(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Accept the WebSocket
    server.accept();
    
    // Initialize the connection handler which sets up the initial CONTINUE packet
    new ConnectionHandler(server);

    // Prepare response headers
    const responseHeaders = {};
    
    // Check if the client requested a subprotocol and echo it back
    const protocol = request.headers.get("Sec-WebSocket-Protocol");
    if (protocol) {
      // The client may send multiple protocols separated by commas (e.g. "wisp-v2, wisp-v1")
      // We'll just grab the first one and agree to it to satisfy the browser handshake.
      const firstProtocol = protocol.split(",")[0].trim();
      responseHeaders["Sec-WebSocket-Protocol"] = firstProtocol;
    }

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: responseHeaders,
    });
  }
}
