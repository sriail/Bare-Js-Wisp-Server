// wisp-entry.js
// Main Cloudflare Worker entry point.
// Exports the fetch handler that routes all incoming requests.

import { handleRequest } from "./wisp-routes.js";

export default {
  /**
   * @param {Request} request - Incoming HTTP request
   * @param {Record<string, unknown>} env - Environment variables/bindings
   * @param {ExecutionContext} ctx - Execution context (for waitUntil, etc.)
   */
  async fetch(request, env, ctx) {
    try {
      return handleRequest(request, env, ctx);
    } catch (err) {
      return new Response("Internal Server Error", { status: 500 });
    }
  },
};
