import { WispServer } from "./server.mjs";

export default {
  async fetch(request, env, ctx) {
    return WispServer.fetch(request, env, ctx);
  },
};
