import { WispHandler } from "./connections.mjs";
import { WSProxyConnection } from "./wsproxy.mjs";

export class WispServer {
  static async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    // If the path ends with a trailing slash, it's a Wisp endpoint
    if (path.endsWith("/")) {
      return WispHandler.handle(request);
    } else {
      // Otherwise, treat it as a wsproxy connection (e.g. /host:port)
      return WSProxyConnection.handle(request, path);
    }
  }
}
