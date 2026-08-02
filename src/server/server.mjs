import { WispHandler } from "./connections.mjs";
import { WSProxyConnection } from "./wsproxy.mjs";

const HTML_PAGE = `<!DOCTYPE html>
<html>
<head>
  <title>Wisp Server Tester</title>
  <meta charset="utf-8">
</head>
<body>
  <h1>Wisp V1 Server Test</h1>
  <div>
    <label>Wisp Server URL: <input type="text" id="serverUrl" value="ws://localhost:8787/" size="50"></label>
  </div>
  <br>
  <div>
    <label>Target Web Address: <input type="text" id="targetUrl" value="http://example.com" size="50"></label>
  </div>
  <br>
  <button id="testBtn">Send Request</button>
  <hr>
  <h3>Response:</h3>
  <pre id="output" style="white-space: pre-wrap; word-wrap: break-word;"></pre>

  <script type="module">
    // Import client files from the specified GitHub repository via jsDelivr CDN
    import { ClientConnection, WispWebSocket } from "https://cdn.jsdelivr.net/gh/sriail/Bare-Js-Wisp-Server@refactor-proxy-for-wisp-js-client/src/client/index.js";

    document.getElementById('testBtn').addEventListener('click', () => {
      const serverUrl = document.getElementById('serverUrl').value;
      const targetUrl = document.getElementById('targetUrl').value;
      const output = document.getElementById('output');
      
      output.textContent = "Connecting to " + serverUrl + "...\\n";
      
      try {
        const parsedTarget = new URL(targetUrl);
        const host = parsedTarget.hostname;
        const port = parsedTarget.port || (parsedTarget.protocol === 'https:' ? '443' : '80');
        const path = parsedTarget.pathname + parsedTarget.search || '/';
        
        // The WispWebSocket polyfill expects the URL format: ws://wisp-server-host/target-host:target-port
        let wispUrl = serverUrl;
        if (!wispUrl.endsWith('/')) wispUrl += '/';
        wispUrl += host + ':' + port;
        
        output.textContent += "Proxying to: " + wispUrl + "\\n\\n";
        
        const ws = new WispWebSocket(wispUrl);
        ws.binaryType = "arraybuffer";
        
        ws.onopen = () => {
          output.textContent += "--- Stream Opened ---\\n";
          // Construct a raw HTTP/1.1 GET request
          const httpRequest = "GET " + path + " HTTP/1.1\\r\\nHost: " + host + "\\r\\nConnection: close\\r\\nUser-Agent: WispTester/1.0\\r\\nAccept: */*\\r\\n\\r\\n";
          ws.send(httpRequest);
          output.textContent += "--- Request Sent ---\\n\\n";
        };
        
        ws.onmessage = (event) => {
          // Decode the incoming ArrayBuffer to text
          const text = new TextDecoder().decode(event.data);
          output.textContent += text;
        };
        
        ws.onerror = (e) => {
          output.textContent += "\\n[ERROR] An error occurred. Check console.\\n";
          console.error("Wisp Error:", e);
        };
        
        ws.onclose = () => {
          output.textContent += "\\n--- Stream Closed ---\\n";
        };
      } catch (err) {
        output.textContent += "\\n[ERROR] " + err.message + "\\n";
        console.error(err);
      }
    });
  </script>
</body>
</html>`;

export class WispServer {
  static async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get("Upgrade");
    const url = new URL(request.url);
    const path = url.pathname;

    // Handle WebSocket upgrades
    if (upgradeHeader && upgradeHeader === "websocket") {
      // If the path ends with a trailing slash, it's a Wisp endpoint
      if (path.endsWith("/")) {
        return WispHandler.handle(request);
      } else {
        // Otherwise, treat it as a wsproxy connection (e.g. /host:port)
        return WSProxyConnection.handle(request, path);
      }
    }

    // Serve the static HTML test page for standard HTTP GET requests
    if (path === "/" || path === "/test" || path === "/index.html") {
      return new Response(HTML_PAGE, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    return new Response("Not Found", { status: 404 });
  }
}
