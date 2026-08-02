import { ConnectionHandler } from "./connectionHandler.mjs";

export class WispHandler {
  static async handle(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Accept the WebSocket
    server.accept();
    
    // Initialize the connection handler which sets up the initial CONTINUE packet
    new ConnectionHandler(server);

    // Use standard Web Headers object
    const responseHeaders = new Headers();
    const protocol = request.headers.get("Sec-WebSocket-Protocol");
    if (protocol) {
      const firstProtocol = protocol.split(",")[0].trim();
      responseHeaders.set("Sec-WebSocket-Protocol", firstProtocol);
    }

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: responseHeaders,
    });
  }
}
