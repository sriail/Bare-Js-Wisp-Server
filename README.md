<div align="center">
  <img src="src/wisp-logo.svg" alt="Light Wisp Server" width="200" height="200" />
  <h1>Light Wisp Server</h1>
  <p>Exactly what it is, a Light Wisp Server (No Node.js, Just pure JS and Cloudflare Workers)</p>
  <br />
</div>


## Deploy
Use the Button Below to Deploy To Cloudflare

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/sriail/Light-Wisp-Server)

## Stability and Notes
From what I know, the proxy is very stable. However, Cloudflare can limit the number of request (occasionally subrequest) to 50 for Non-Premum
Accounts, and does not support TCP (Meaning only UDP request get fully proxied) which is a Huge issue which i will hopefully patch with some Janky
solution.

## Quick Roadmap

- [ ] Add TCP support (grab tcp in some other way and send it out as alt udp streams to the client, or just wait for them to add it natively???)
- [ ] Add Domain Mirroring ( Can access on alt Cloudflare domains for the worker, may already work?)
- [ ] Add Rate Limiting in editable JS File.
