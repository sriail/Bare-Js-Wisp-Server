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
  <label>Wisp Server URL: 
    <input type="text" id="serverUrl" value="" size="50">
  </label>
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
  import { ClientConnection } from "https://cdn.jsdelivr.net/gh/sriail/Bare-Js-Wisp-Server@refactor-proxy-for-wisp-js-client/src/client/index.js";

  // --- Dynamic Server URL Setup ---
  const serverInput = document.getElementById('serverUrl');
  if (serverInput) {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    serverInput.value = `${protocol}
  }
  // ---------------------------------

  document.getElementById('testBtn').addEventListener('click', () => {
    const serverUrl = document.getElementById('serverUrl').value;
    const targetUrl = document.getElementById('targetUrl').value;
    const output = document.getElementById('output');
    
    if (!serverUrl.endsWith('/')) {
      output.textContent = "Error: Wisp Server URL must end with a trailing slash (/)";
      return;
    }

    output.textContent = "Connecting to " + serverUrl + "...\n";
    
    try {
      const parsedTarget = new URL(targetUrl);
      const host = parsedTarget.hostname;
      const port = parsedTarget.port || (parsedTarget.protocol === 'https:' ? '443' : '80');
      const path = parsedTarget.pathname + parsedTarget.search || '/';
      
      // Explicitly use Wisp V1 to match our server implementation
      const conn = new ClientConnection(serverUrl, { wisp_version: 1 });
      
      conn.onopen = () => {
        output.textContent += "Connected! Creating stream to " + host + ":" + port + "\n";
        const stream = conn.create_stream(host, parseInt(port));
        
        stream.onmessage = (raw_data) => {
          const text = new TextDecoder().decode(raw_data);
          output.textContent += text;
        };
        
        stream.onclose = (reason) => {
          output.textContent += "\n--- Stream Closed (Reason: " + reason + ") ---\n";
        };

        const httpRequest = "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\nUser-Agent: WispTester/1.0\r\nAccept: */*\r\n\r\n";
        
        setTimeout(() => {
          stream.send(new TextEncoder().encode(httpRequest));
          output.textContent += "--- Request Sent ---\n\n";
        }, 100);
      };
      
      conn.onerror = (e) => {
        output.textContent += "\n[ERROR] Connection error.\n";
        console.error(e);
      };
      
      conn.onclose = () => {
        output.textContent += "\n--- Wisp Connection Closed ---\n";
      };

    } catch (err) {
      output.textContent += "\n[ERROR] " + err.message + "\n";
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

    if (upgradeHeader && upgradeHeader === "websocket") {
      if (path.endsWith("/")) {
        return WispHandler.handle(request);
      } else {
        return WSProxyConnection.handle(request, path);
      }
    }

    if (path === "/" || path === "/test" || path === "/index.html") {
      return new Response(HTML_PAGE, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    return new Response("Not Found", { status: 404 });
  }
}
