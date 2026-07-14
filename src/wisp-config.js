// wisp-config.js
// Configuration options for the Wisp server, mimicking the @mercuryworkshop/wisp-js options API.
// Cloudflare Workers do not support custom DNS resolvers, so those options are accepted
// for compatibility but ignored.

export const serverOptions = {
  port_whitelist: [
    80,
    443,
    [5000, 6000] // Ranges supported
  ],
  allow_private_ips: false,
  allow_loopback_ips: false,
  wisp_version: null, // null = auto-detect via Sec-WebSocket-Protocol, 1 or 2 to force
  wisp_motd: "Wisp server on Cloudflare Workers — TCP only",
  
  // Ignored on Workers, but present for API compatibility
  dns_method: "resolve",
  dns_servers: ["1.1.1.1", "1.0.0.1"],
  dns_result_order: "ipv4first",
};

export const logging = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
  NONE: 4,
  level: 1,
  set_level(level) {
    this.level = level;
  },
  log(msg, level = 1) {
    if (level >= this.level) {
      console.log(`[Wisp] ${msg}`);
    }
  }
};

// ─── IP & Port Validation Helpers ────────────────────────────────────────────

function isIPv4(hostname) {
  return /^(\d{1,3}\.){3}\d{1,3}$/.test(hostname);
}

function isIPv6(hostname) {
  return /^[0-9a-fA-F:]+$/.test(hostname) && hostname.includes(":");
}

export function isIpLiteral(hostname) {
  return isIPv4(hostname) || isIPv6(hostname);
}

export function isPrivateIp(hostname) {
  if (isIPv4(hostname)) {
    if (hostname.startsWith("10.")) return true;
    if (hostname.startsWith("192.168.")) return true;
    if (hostname.startsWith("169.254.")) return true; // Link-local
    if (hostname.startsWith("127.")) return true; // Loopback
    if (hostname.startsWith("172.")) {
      const parts = hostname.split(".");
      const second = parseInt(parts[1], 10);
      if (second >= 16 && second <= 31) return true;
    }
  }
  if (isIPv6(hostname)) {
    const h = hostname.toLowerCase();
    if (h === "::1") return true; // Loopback
    if (h.startsWith("fc") || h.startsWith("fd")) return true; // Unique local
    if (h.startsWith("fe80")) return true; // Link-local
  }
  return false;
}

export function isLoopbackIp(hostname) {
  if (isIPv4(hostname) && hostname.startsWith("127.")) return true;
  if (isIPv6(hostname) && hostname.toLowerCase() === "::1") return true;
  return false;
}

export function isPortAllowed(port) {
  const whitelist = serverOptions.port_whitelist;
  if (!whitelist || whitelist.length === 0) return true; // Empty = allow all

  for (const entry of whitelist) {
    if (Array.isArray(entry)) {
      if (port >= entry[0] && port <= entry[1]) return true;
    } else if (port === entry) {
      return true;
    }
  }
  return false;
}
