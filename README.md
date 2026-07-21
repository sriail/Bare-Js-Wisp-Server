<div align="center">
  <img src="src/wisp-logo.svg" alt="Light Wisp Server" width="200" height="200" />
  <h1>Bare Js Wisp Server</h1>
  <p>Exactly what it is, a Light Wisp Server (No Node.js, Just pure JS and Cloudflare Workers)</p>

  <h2>Deploy</h2>
  <p>Use the Button Below to Deploy To Cloudflare</p>
  <br />

  [![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/sriail/Bare-Js-Wisp-Server)
  <br />

  <h2>Stability and Notes</h2>
  <p>From what I know, the proxy is very stable. However, Cloudflare can limit the number of request (occasionally subrequest) to 50 for Non-Premium Accounts, and does not support TCP (Meaning only UDP request get fully proxied) which is a Huge issue which i will hopefully patch with some Janky solution.</p>
  <br />

  <h2>Quick Roadmap</h2>
</div>

<div align="center">
<div align="left" style="display: inline-block;">

- [x] Add TCP support (grab tcp in some other way and send it out as alt udp streams to the client, or just wait for them to add it natively???)
- [x] Add Domain Mirroring ( Can access on alt Cloudflare domains for the worker, may already work?)
- [ ] Add Rate Limiting in editable JS File.
- [ ] Wisp v2.1???

<p> Writen By [Sriail](https://github.com/sriail) </p>
Credit to [Ading2210](https://github.com/ading2210) For the [Wisp V1](https://github.com/MercuryWorkshop/wisp-protocol/blob/v1/protocol.md) Spec and the Rest of the (Mercury Workshop Team)[https://github.com/MercuryWorkshop/wisp-protocol/blob/v1/protocol.md]
</div>
</div>
