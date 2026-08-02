import { ConnectionHandler } from "./connectionHandler.mjs";

export class WispHandler {
  static async handle(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Accept the WebSocket
    server.accept();
    
    // Initialize the connection handler which sets up the initial CONTINUE packet
    new ConnectionHandler(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }
}
