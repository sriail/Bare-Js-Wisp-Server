// index.js
'use strict';

export const INDEX_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Wisp Proxy Tester</title>
    <style>
        body { font-family: monospace; padding: 20px; }
        textarea { width: 100%; box-sizing: border-box; }
        .row { margin-bottom: 10px; }
    </style>
</head>
<body>
    <h2>Wisp Local Relay Tester (Free Plan HTTP Mode)</h2>
    <div class="row">
        <label>Target Host: </label>
        <input type="text" id="host" value="example.com" style="width: 300px;">
        <button id="sendBtn">Send Request</button>
    </div>
    
    <h3>Log:</h3>
    <textarea id="log" rows="10" readonly></textarea>

    <h3>Response:</h3>
    <textarea id="response" rows="20" readonly></textarea>

    <script>
        const logEl = document.getElementById('log');
        const respEl = document.getElementById('response');
        const sendBtn = document.getElementById('sendBtn');

        function log(msg) {
            const time = new Date().toISOString().split('T')[1];
            logEl.value += '[' + time + '] ' + msg + '\\n';
            logEl.scrollTop = logEl.scrollHeight;
        }

        const packet_types = {
            CONNECT: 0x01, DATA: 0x02, CONTINUE: 0x03, CLOSE: 0x04, INFO: 0x05
        };

        let ws;
        let streamId = 1;
        let handshakeComplete = false;
        let streamOpenConfirmed = false;
        let pendingData = null;

        function makePacket(type, sId, payload) {
            const buf = new ArrayBuffer(5 + payload.length);
            const view = new DataView(buf);
            view.setUint8(0, type);
            view.setUint32(1, sId, true); // Little-endian
            new Uint8Array(buf, 5).set(payload);
            return buf;
        }

        function sendInfo() {
            const payload = new Uint8Array(7);
            const view = new DataView(payload.buffer);
            view.setUint8(0, 2); // Major
            view.setUint8(1, 1); // Minor
            view.setUint8(2, 0x05); // Ext ID
            view.setUint32(3, 0, true); // Ext Length
            ws.send(makePacket(packet_types.INFO, 0, payload));
            log('Sent client INFO packet');
        }

        function sendConnect(host) {
            const hostBytes = new TextEncoder().encode(host);
            const payload = new Uint8Array(3 + hostBytes.length);
            const view = new DataView(payload.buffer);
            view.setUint8(0, 0x01); // TCP
            view.setUint16(1, 80, true); // Port 80
            payload.set(hostBytes, 3);
            
            ws.send(makePacket(packet_types.CONNECT, streamId, payload));
            log('Sent CONNECT packet for ' + host + ':80 (Stream ID: ' + streamId + ')');
        }

        function sendData(data) {
            ws.send(makePacket(packet_types.DATA, streamId, data));
            log('Sent HTTP GET request as DATA packet');
        }

        function connectWs() {
            const wsUrl = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/';
            log('Connecting to ' + wsUrl);
            ws = new WebSocket(wsUrl, 'wisp-v2');
            ws.binaryType = 'arraybuffer';

            ws.onopen = () => log('WebSocket connected. Waiting for server INFO...');

            ws.onmessage = (event) => {
                const buf = new Uint8Array(event.data);
                if (buf.length < 5) return;
                const view = new DataView(buf.buffer);
                const type = view.getUint8(0);
                const sId = view.getUint32(1, true);
                const payload = buf.slice(5);

                if (type === packet_types.INFO) {
                    log('Received server INFO. Sending client INFO...');
                    sendInfo();
                } else if (type === packet_types.CONTINUE) {
                    if (sId === 0 && !handshakeComplete) {
                        log('Received CONTINUE on stream 0. Handshake complete!');
                        handshakeComplete = true;
                    } else if (sId === streamId && !streamOpenConfirmed) {
                        log('Received CONTINUE on stream ' + sId + '. Socket connected to destination.');
                        streamOpenConfirmed = true;
                        if (pendingData) {
                            sendData(pendingData);
                            pendingData = null;
                        }
                    }
                } else if (type === packet_types.DATA) {
                    const text = new TextDecoder().decode(payload);
                    respEl.value += text;
                } else if (type === packet_types.CLOSE) {
                    const reason = payload.length > 0 ? payload[0] : 0;
                    log('Received CLOSE packet. Reason: 0x' + reason.toString(16));
                    log('Stream closed.');
                }
            };

            ws.onclose = () => log('WebSocket disconnected.');
            ws.onerror = () => log('WebSocket error.');
        }

        sendBtn.onclick = () => {
            respEl.value = '';
            let host = document.getElementById('host').value;
            
            // Clean the URL to just the hostname
            host = host.replace(/^https?:\/\//, '').split('/')[0].split(':')[0];
            
            streamId = Math.floor(Math.random() * 1000) + 1; // Random stream ID
            handshakeComplete = false;
            streamOpenConfirmed = false;
            
            const httpReq = 'GET / HTTP/1.1\\r\\nHost: ' + host + '\\r\\nConnection: close\\r\\n\\r\\n';
            pendingData = new TextEncoder().encode(httpReq);
            
            if (!ws || ws.readyState !== 1) {
                connectWs();
                const interval = setInterval(() => {
                    if (handshakeComplete) {
                        clearInterval(interval);
                        sendConnect(host);
                    }
                }, 100);
            } else {
                sendConnect(host);
            }
        };

        // Auto-connect on load
        connectWs();
    </script>
</body>
</html>`;
