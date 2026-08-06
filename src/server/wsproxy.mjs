import { connect } from "cloudflare:sockets";

export class WSProxyConnection {
  static async handle(request, path) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    const target = path.split("/").pop().split(":");
    const hostname = target[0];
    const port = parseInt(target[1]);

    server.accept();

    (async () => {
      let writer, reader;
      try {
        const tcpSocket = connect({ hostname, port });
        
        // Await the connection opening so it doesn't crash on write
        await tcpSocket.opened;
        
        writer = tcpSocket.writable.getWriter();
        reader = tcpSocket.readable.getReader();

        server.addEventListener("message", async (event) => {
          try {
            if (typeof event.data === "string") return;
            const buf = new Uint8Array(event.data);
            await writer.ready;
            await writer.write(buf);
          } catch (e) {}
        });

        server.addEventListener("close", async () => {
          try { if (writer) await writer.close(); } catch (e) {}
        });

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          // Send strict ArrayBuffer
          server.send(value.buffer);
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
