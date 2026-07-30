// crawler.js
'use strict';

export const CRAWLER_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Wisp Crawler</title>
</head>
<body>
    <h2>Wisp Seed Crawler</h2>
    
    <div>
        <label>Upload seeds.txt:</label>
        <input type="file" id="uploadBtn" accept=".txt">
    </div>
    
    <div style="margin-top: 10px;">
        <button id="startBtn">Start Crawl</button>
        <button id="stopBtn">Stop</button>
        <button id="downloadBtn">Download data.txt</button>
    </div>

    <h3>Status: <span id="spinner">|</span> Pages Crawled: <span id="counter">0</span></h3>
    
    <h3>Log:</h3>
    <textarea id="log" rows="10" readonly style="width: 100%;"></textarea>

    <script>
        // ---COMENT--- UI Element References
        const logEl = document.getElementById('log');
        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');
        const downloadBtn = document.getElementById('downloadBtn');
        const uploadBtn = document.getElementById('uploadBtn');
        const counterEl = document.getElementById('counter');
        const spinnerEl = document.getElementById('spinner');

        // ---COMENT--- Crawler State Variables
        let ws;
        let streamId = 1;
        let handshakeComplete = false;
        let isCrawling = false;
        let pagesCrawled = 0;
        let dataTxt = "";
        let crawlQueue = [];
        let crawledUrls = new Set();
        let activeStreamId = 0;
        let streamBuffer = "";
        let currentDomain = "";
        let spinnerInterval;

        // ---COMENT--- Wisp Protocol Packet Types
        const packet_types = {
            CONNECT: 0x01, DATA: 0x02, CONTINUE: 0x03, CLOSE: 0x04
        };

        function log(msg) {
            const time = new Date().toISOString().split('T')[1];
            logEl.value += '[' + time + '] ' + msg + '\\n';
            logEl.scrollTop = logEl.scrollHeight;
        }

        // ---COMENT--- Spinner Animation Logic
        function startSpinner() {
            const chars = ['|', '/', '-', '\\'];
            let i = 0;
            spinnerInterval = setInterval(() => {
                spinnerEl.textContent = chars[i++ % chars.length];
            }, 100);
        }
        function stopSpinner() {
            clearInterval(spinnerInterval);
            spinnerEl.textContent = 'Idle';
        }

        function makePacket(type, sId, payload) {
            const buf = new ArrayBuffer(5 + payload.length);
            const view = new DataView(buf);
            view.setUint8(0, type);
            view.setUint32(1, sId, true);
            new Uint8Array(buf, 5).set(payload);
            return buf;
        }

        // ---COMENT--- Attempt to load seeds.txt automatically on page load
        function loadDefaultSeeds() {
            fetch('seeds.txt')
                .then(res => {
                    if (!res.ok) throw new Error('File not found');
                    return res.text();
                })
                .then(text => {
                    parseSeeds(text);
                    log('Successfully loaded local seeds.txt');
                })
                .catch(() => log('No local seeds.txt found. Please upload a file.'));
        }

        // ---COMENT--- Handle manual seed file upload
        uploadBtn.onchange = (event) => {
            const file = event.target.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = (e) => {
                parseSeeds(e.target.result);
                log('Loaded seeds from uploaded file: ' + file.name);
            };
            reader.readAsText(file);
        };

        function parseSeeds(text) {
            const urls = text.split('\\n').map(u => u.trim()).filter(u => u.length > 0);
            crawlQueue = crawlQueue.concat(urls);
        }

        // ---COMENT--- WebSocket Connection Setup
        function connectWs() {
            const wsUrl = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/';
            log('Connecting to ' + wsUrl);
            ws = new WebSocket(wsUrl);
            ws.binaryType = 'arraybuffer';

            ws.onopen = () => log('WebSocket connected. Waiting for handshake...');

            ws.onmessage = (event) => {
                const buf = new Uint8Array(event.data);
                if (buf.length < 5) return;
                const view = new DataView(buf.buffer);
                const type = view.getUint8(0);
                const sId = view.getUint32(1, true);
                const payload = buf.slice(5);

                if (type === packet_types.CONTINUE) {
                    if (sId === 0 && !handshakeComplete) {
                        handshakeComplete = true;
                        log('Handshake complete!');
                        processQueue();
                    }
                } else if (type === packet_types.DATA) {
                    if (sId === activeStreamId) {
                        streamBuffer += new TextDecoder().decode(payload);
                    }
                } else if (type === packet_types.CLOSE) {
                    if (sId === activeStreamId) {
                        processScrapedData();
                        processQueue();
                    }
                }
            };

            ws.onclose = () => {
                log('WebSocket disconnected.');
                isCrawling = false;
                stopSpinner();
            };
            ws.onerror = () => log('WebSocket error.');
        }

        // ---COMENT--- Process the next URL in the queue
        function processQueue() {
            if (!isCrawling) return;
            
            if (crawlQueue.length === 0) {
                log('Crawl queue empty. Finished.');
                isCrawling = false;
                stopSpinner();
                return;
            }

            let rawUrl = crawlQueue.shift();
            // ---COMENT--- Normalize URL and prevent duplicates
            if (!rawUrl.startsWith('http')) rawUrl = 'http://' + rawUrl;
            if (crawledUrls.has(rawUrl)) {
                processQueue();
                return;
            }
            
            crawledUrls.add(rawUrl);
            
            try {
                const url = new URL(rawUrl);
                currentDomain = url.hostname;
                const path = url.pathname === '/' ? '/' : url.pathname + url.search;
                
                streamBuffer = "";
                activeStreamId = Math.floor(Math.random() * 10000) + 1;
                
                const hostBytes = new TextEncoder().encode(url.hostname);
                const payload = new Uint8Array(3 + hostBytes.length);
                const view = new DataView(payload.buffer);
                view.setUint8(0, 0x01); // TCP
                view.setUint16(1, 80, true); // Port 80
                payload.set(hostBytes, 3);
                
                ws.send(makePacket(packet_types.CONNECT, activeStreamId, payload));
                
                const httpReq = 'GET ' + path + ' HTTP/1.1\\r\\nHost: ' + url.hostname + '\\r\\nConnection: close\\r\\n\\r\\n';
                ws.send(makePacket(packet_types.DATA, activeStreamId, new TextEncoder().encode(httpReq)));
                
                log('Fetching: ' + rawUrl);
            } catch (e) {
                log('Invalid URL, skipping: ' + rawUrl);
                processQueue();
            }
        }

        // ---COMENT--- Parse scraped HTML, extract stats, and save to dataTxt
        function processScrapedData() {
            if (streamBuffer.length === 0) return;
            
            // ---COMENT--- Split headers and body
            const parts = streamBuffer.split('\\r\\n\\r\\n');
            const headers = parts[0];
            const body = parts.slice(1).join('\\r\\n\\r\\n');
            
            // ---COMENT--- Extract status code
            let status = "Unknown";
            const statusMatch = headers.match(/HTTP\\/[\\d.]+ (\\d+)/);
            if (statusMatch) status = statusMatch[1];
            
            // ---COMENT--- Extract links for systematic crawling
            const linkRegex = /href=["'](.*?)["']/g;
            let match;
            let linksFound = 0;
            while ((match = linkRegex.exec(body)) !== null) {
                let link = match[1];
                if (link.startsWith('http')) {
                    crawlQueue.push(link);
                    linksFound++;
                } else if (link.startsWith('/')) {
                    crawlQueue.push('http://' + currentDomain + link);
                    linksFound++;
                }
            }

            // ---COMENT--- Format the data using the required labeling
            dataTxt += "---COMENT--- Domain: " + currentDomain + "\\n";
            dataTxt += "---COMENT--- Status: " + status + " | Bytes: " + body.length + " | Links Found: " + linksFound + "\\n";
            dataTxt += "---COMENT--- HTML Content:\\n";
            dataTxt += body + "\\n\\n========================================\\n\\n";
            
            pagesCrawled++;
            counterEl.textContent = pagesCrawled;
            log('Scraped ' + currentDomain + ' (Status: ' + status + ', Links: ' + linksFound + ')');
        }

        // ---COMENT--- Start Button Logic
        startBtn.onclick = () => {
            if (isCrawling) return;
            if (crawlQueue.length === 0) {
                log('No seeds to crawl. Upload a file or provide seeds.txt');
                return;
            }
            isCrawling = true;
            startSpinner();
            log('Starting crawler...');
            if (!ws || ws.readyState !== 1) {
                connectWs();
            } else if (handshakeComplete) {
                processQueue();
            }
        };

        // ---COMENT--- Stop Button Logic
        stopBtn.onclick = () => {
            isCrawling = false;
            stopSpinner();
            log('Crawler stopped by user.');
        };

        // ---COMENT--- Download Button Logic
        downloadBtn.onclick = () => {
            const blob = new Blob([dataTxt], { type: 'text/plain' });
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = 'data.txt';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            log('Downloaded data.txt');
        };

        // ---COMENT--- Initialize on load
        stopSpinner();
        loadDefaultSeeds();
    </script>
</body>
</html>`;
