import { connect } from "cloudflare:sockets";
import { Buffer } from "node:buffer";

export class WSProxyConnection {
  static async handle(request, path) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    const target = path.split("/").pop().split(":");
    const hostname = target[0];
    const port = parseInt(target[1]);

    server.accept();

    (async () => {
      let tcpSocket, writer, reader;
      try {
        tcpSocket = connect({ hostname, port });
        writer = tcpSocket.writable.getWriter();
        reader = tcpSocket.readable.getReader();

        server.addEventListener("message", async (event) => {
          try {
            await writer.ready;
            await writer.write(event.data);
          } catch (e) {}
        });

        server.addEventListener("close", async () => {
          if (writer) await writer.close().catch(() => {});
        });

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          server.send(value);
        }
      } catch (e) {
        // Connection failure or network error
      } finally {
        try { server.close(); } catch (e) {}
      }
    })();

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }
}
