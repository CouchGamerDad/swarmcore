const express = require("express");
const fs = require("fs");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const TICK_RATE = 24;
const BROADCAST_RATE = 6;
const SOCKET_OVERHEAD_MULTIPLIER = 1.25;
const FIVE_GB_BYTES = 5 * 1024 * 1024 * 1024;
const BYTES_PER_KB = 1024;
const BYTES_PER_MB = 1024 * 1024;
const BYTES_PER_GB = 1024 * 1024 * 1024;
const BANDWIDTH_LOG_INTERVAL_MS = 60000;
const DT = 1 / TICK_RATE;
const BASE_WORLD_RADIUS = 4200;
const MAX_WORLD_RADIUS = 18000;
const DRONE_RADIUS = 8;
const CORE_RADIUS = 20;
const SHARD_RADIUS = 8;
const MAX_DRONES = 10000;
const TARGET_SHARDS = 420;
const BOT_COUNT = 50;
const MAX_REAL_PLAYERS = 50;
const COMBAT_DRONE_SAMPLE = 168;
const SHARD_VIEW_RADIUS = 4200;
const MAX_SHARDS_PER_CLIENT = 180;
const STORM_INTERVAL = 120;
const STORM_DURATION = 15;
const DRONE_HIT_COOLDOWN = 700;
const TITAN_DRONES = 1000;
const MEGA_TITAN_DRONES = 5000;
const NEEDLE_DASH_MAX_DRONES = 150;
const NEEDLE_DASH_CHARGE_MS = 800;
const NEEDLE_DASH_DURATION_MS = 260;
const NEEDLE_DASH_COOLDOWN_MS = 2600;

const STATS_FILE = path.join(__dirname, "swarmcore_stats.json");
const STATS_TMP_FILE = `${STATS_FILE}.tmp`;
const MAX_RECENT_EVENTS = 100;
const STATS_FLUSH_INTERVAL_MS = 30000;

function createEmptyStats() {
  return {
    totalSocketConnections: 0,
    totalGameStarts: 0,
    totalDisconnects: 0,
    totalDeaths: 0,
    totalKills: 0,
    botDeaths: 0,
    botKills: 0,
    activePlayers: 0,
    peakActivePlayers: 0,
    sessionsStarted: 0,
    completedSessions: 0,
    averageSessionSeconds: 0,
    totalSessionSeconds: 0,
    playerKilledBot: 0,
    botKilledPlayer: 0,
    playerKilledPlayer: 0,
    botKilledBot: 0,
    totalEstimatedBytesSent: 0,
    totalEstimatedStateBytesSent: 0,
    totalStateBroadcasts: 0,
    totalStatePayloadBytes: 0,
    totalStatePayloadSamples: 0,
    lastStatePayloadBytes: 0,
    averageStatePayloadBytes: 0,
    peakStatePayloadBytes: 0,
    minStatePayloadBytes: 0,
    estimatedBytesPerSecond: 0,
    estimatedMBPerHourPerPlayer: 0,
    estimatedGBPerHourPerPlayer: 0,
    estimatedHoursFor5GBAtCurrentLoad: 0,
    estimatedHoursFor5GBAt1Player: 0,
    estimatedHoursFor5GBAt5Players: 0,
    estimatedHoursFor5GBAt10Players: 0,
    estimatedHoursFor5GBAt25Players: 0,
    recentEvents: []
  };
}

const analytics = createEmptyStats();
const socketSessions = new Map();
let statsDirty = false;
let pendingFlushTimer = null;
let statsFlushInProgress = false;

function normalizeLoadedStats(raw = {}) {
  const base = createEmptyStats();
  const normalized = {
    ...base,
    ...raw
  };

  normalized.totalSocketConnections = Number.isFinite(normalized.totalSocketConnections) ? normalized.totalSocketConnections : 0;
  normalized.totalGameStarts = Number.isFinite(normalized.totalGameStarts) ? normalized.totalGameStarts : 0;
  normalized.totalDisconnects = Number.isFinite(normalized.totalDisconnects) ? normalized.totalDisconnects : 0;
  normalized.totalDeaths = Number.isFinite(normalized.totalDeaths) ? normalized.totalDeaths : 0;
  normalized.totalKills = Number.isFinite(normalized.totalKills) ? normalized.totalKills : 0;
  normalized.botDeaths = Number.isFinite(normalized.botDeaths) ? normalized.botDeaths : 0;
  normalized.botKills = Number.isFinite(normalized.botKills) ? normalized.botKills : 0;
  normalized.activePlayers = Number.isFinite(normalized.activePlayers) ? normalized.activePlayers : 0;
  normalized.peakActivePlayers = Number.isFinite(normalized.peakActivePlayers) ? normalized.peakActivePlayers : 0;
  normalized.sessionsStarted = Number.isFinite(normalized.sessionsStarted) ? normalized.sessionsStarted : 0;
  normalized.completedSessions = Number.isFinite(normalized.completedSessions) ? normalized.completedSessions : 0;
  normalized.totalSessionSeconds = Number.isFinite(normalized.totalSessionSeconds) ? normalized.totalSessionSeconds : 0;
  normalized.averageSessionSeconds = Number.isFinite(normalized.averageSessionSeconds) ? normalized.averageSessionSeconds : 0;
  normalized.playerKilledBot = Number.isFinite(normalized.playerKilledBot) ? normalized.playerKilledBot : 0;
  normalized.botKilledPlayer = Number.isFinite(normalized.botKilledPlayer) ? normalized.botKilledPlayer : 0;
  normalized.playerKilledPlayer = Number.isFinite(normalized.playerKilledPlayer) ? normalized.playerKilledPlayer : 0;
  normalized.botKilledBot = Number.isFinite(normalized.botKilledBot) ? normalized.botKilledBot : 0;
  normalized.totalEstimatedBytesSent = Number.isFinite(normalized.totalEstimatedBytesSent) ? normalized.totalEstimatedBytesSent : 0;
  normalized.totalEstimatedStateBytesSent = Number.isFinite(normalized.totalEstimatedStateBytesSent) ? normalized.totalEstimatedStateBytesSent : 0;
  normalized.totalStateBroadcasts = Number.isFinite(normalized.totalStateBroadcasts) ? normalized.totalStateBroadcasts : 0;
  normalized.totalStatePayloadBytes = Number.isFinite(normalized.totalStatePayloadBytes) ? normalized.totalStatePayloadBytes : 0;
  normalized.totalStatePayloadSamples = Number.isFinite(normalized.totalStatePayloadSamples) ? normalized.totalStatePayloadSamples : 0;
  normalized.lastStatePayloadBytes = Number.isFinite(normalized.lastStatePayloadBytes) ? normalized.lastStatePayloadBytes : 0;
  normalized.averageStatePayloadBytes = Number.isFinite(normalized.averageStatePayloadBytes) ? normalized.averageStatePayloadBytes : 0;
  normalized.peakStatePayloadBytes = Number.isFinite(normalized.peakStatePayloadBytes) ? normalized.peakStatePayloadBytes : 0;
  normalized.minStatePayloadBytes = Number.isFinite(normalized.minStatePayloadBytes) ? normalized.minStatePayloadBytes : 0;
  normalized.estimatedBytesPerSecond = Number.isFinite(normalized.estimatedBytesPerSecond) ? normalized.estimatedBytesPerSecond : 0;
  normalized.estimatedMBPerHourPerPlayer = Number.isFinite(normalized.estimatedMBPerHourPerPlayer) ? normalized.estimatedMBPerHourPerPlayer : 0;
  normalized.estimatedGBPerHourPerPlayer = Number.isFinite(normalized.estimatedGBPerHourPerPlayer) ? normalized.estimatedGBPerHourPerPlayer : 0;
  normalized.estimatedHoursFor5GBAtCurrentLoad = Number.isFinite(normalized.estimatedHoursFor5GBAtCurrentLoad) ? normalized.estimatedHoursFor5GBAtCurrentLoad : 0;
  normalized.estimatedHoursFor5GBAt1Player = Number.isFinite(normalized.estimatedHoursFor5GBAt1Player) ? normalized.estimatedHoursFor5GBAt1Player : 0;
  normalized.estimatedHoursFor5GBAt5Players = Number.isFinite(normalized.estimatedHoursFor5GBAt5Players) ? normalized.estimatedHoursFor5GBAt5Players : 0;
  normalized.estimatedHoursFor5GBAt10Players = Number.isFinite(normalized.estimatedHoursFor5GBAt10Players) ? normalized.estimatedHoursFor5GBAt10Players : 0;
  normalized.estimatedHoursFor5GBAt25Players = Number.isFinite(normalized.estimatedHoursFor5GBAt25Players) ? normalized.estimatedHoursFor5GBAt25Players : 0;
  normalized.recentEvents = Array.isArray(normalized.recentEvents)
    ? normalized.recentEvents.slice(-MAX_RECENT_EVENTS)
    : [];

  if (!normalized.averageStatePayloadBytes && normalized.totalStatePayloadSamples > 0) {
    normalized.averageStatePayloadBytes = Number((normalized.totalStatePayloadBytes / normalized.totalStatePayloadSamples).toFixed(2));
  }

  return normalized;
}

function loadStatsFromDisk() {
  try {
    if (!fs.existsSync(STATS_FILE)) return;
    const raw = fs.readFileSync(STATS_FILE, "utf8");
    const parsed = normalizeLoadedStats(JSON.parse(raw));
    Object.assign(analytics, parsed);
  } catch (error) {
    console.error("Analytics startup warning: unable to load swarmcore_stats.json", error.message);
  }
}

function resetBandwidthAnalytics() {
  analytics.totalEstimatedBytesSent = 0;
  analytics.totalEstimatedStateBytesSent = 0;
  analytics.totalStateBroadcasts = 0;
  analytics.totalStatePayloadBytes = 0;
  analytics.totalStatePayloadSamples = 0;
  analytics.lastStatePayloadBytes = 0;
  analytics.averageStatePayloadBytes = 0;
  analytics.peakStatePayloadBytes = 0;
  analytics.minStatePayloadBytes = 0;
  analytics.estimatedBytesPerSecond = 0;
  analytics.estimatedMBPerHourPerPlayer = 0;
  analytics.estimatedGBPerHourPerPlayer = 0;
  analytics.estimatedHoursFor5GBAtCurrentLoad = 0;
  analytics.estimatedHoursFor5GBAt1Player = 0;
  analytics.estimatedHoursFor5GBAt5Players = 0;
  analytics.estimatedHoursFor5GBAt10Players = 0;
  analytics.estimatedHoursFor5GBAt25Players = 0;
}

function recalculateAverageSessionSeconds() {
  analytics.averageSessionSeconds = analytics.completedSessions > 0
    ? Number((analytics.totalSessionSeconds / analytics.completedSessions).toFixed(2))
    : 0;
}

function markStatsDirty() {
  statsDirty = true;
}

function recalculateActivePlayers() {
  let active = 0;
  for (const session of socketSessions.values()) {
    if (session.startedAt) active += 1;
  }
  analytics.activePlayers = active;
  analytics.peakActivePlayers = Math.max(analytics.peakActivePlayers, active);
  markStatsDirty();
}

function addRecentEvent(type, details = {}) {
  analytics.recentEvents.push({
    at: new Date().toISOString(),
    type,
    details
  });
  if (analytics.recentEvents.length > MAX_RECENT_EVENTS) {
    analytics.recentEvents.splice(0, analytics.recentEvents.length - MAX_RECENT_EVENTS);
  }
  markStatsDirty();
}

async function flushStatsToDisk() {
  if (statsFlushInProgress || !statsDirty) return;
  statsFlushInProgress = true;
  const payload = {
    ...analytics,
    recentEvents: analytics.recentEvents.slice(-MAX_RECENT_EVENTS)
  };
  try {
    const json = `${JSON.stringify(payload, null, 2)}\n`;
    await fs.promises.writeFile(STATS_TMP_FILE, json, "utf8");
    await fs.promises.rename(STATS_TMP_FILE, STATS_FILE);
    statsDirty = false;
  } catch (error) {
    console.error("Analytics warning: failed to flush swarmcore_stats.json", error.message);
  } finally {
    statsFlushInProgress = false;
  }
}

function scheduleStatsFlushSoon(delayMs = 1200) {
  markStatsDirty();
  if (pendingFlushTimer) return;
  pendingFlushTimer = setTimeout(async () => {
    pendingFlushTimer = null;
    await flushStatsToDisk();
  }, delayMs);
}

function analyticsSnapshot() {
  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    activePlayers: analytics.activePlayers,
    peakActivePlayers: analytics.peakActivePlayers,
    totalSocketConnections: analytics.totalSocketConnections,
    totalGameStarts: analytics.totalGameStarts,
    totalDisconnects: analytics.totalDisconnects,
    totalDeaths: analytics.totalDeaths,
    totalKills: analytics.totalKills,
    botDeaths: analytics.botDeaths,
    botKills: analytics.botKills,
    sessionsStarted: analytics.sessionsStarted,
    completedSessions: analytics.completedSessions,
    averageSessionSeconds: analytics.averageSessionSeconds,
    playerKilledBot: analytics.playerKilledBot,
    botKilledPlayer: analytics.botKilledPlayer,
    playerKilledPlayer: analytics.playerKilledPlayer,
    botKilledBot: analytics.botKilledBot,
    recentEvents: analytics.recentEvents.slice(-MAX_RECENT_EVENTS)
  };
}

function roundMetric(value, decimals = 2) {
  if (!Number.isFinite(value)) return 0;
  return Number(value.toFixed(decimals));
}

function bytesToMB(bytes) {
  return bytes / BYTES_PER_MB;
}

function bytesToGB(bytes) {
  return bytes / BYTES_PER_GB;
}

function getConnectedSocketCount() {
  return io.sockets.sockets.size;
}

function estimateUsageForSocketCount(socketCount, payloadBytes = analytics.averageStatePayloadBytes || analytics.lastStatePayloadBytes) {
  if (!socketCount || !payloadBytes) {
    return {
      socketCount,
      bytesPerSecond: 0,
      mbPerHour: 0,
      gbPerHour: 0,
      hoursFor5GB: 0
    };
  }

  const bytesPerSecond = payloadBytes * socketCount * SOCKET_OVERHEAD_MULTIPLIER * BROADCAST_RATE;
  const bytesPerHour = bytesPerSecond * 3600;
  const gbPerHour = bytesToGB(bytesPerHour);

  return {
    socketCount,
    bytesPerSecond,
    mbPerHour: bytesToMB(bytesPerHour),
    gbPerHour,
    hoursFor5GB: gbPerHour > 0 ? FIVE_GB_BYTES / bytesPerHour : 0
  };
}

function refreshBandwidthEstimates() {
  const current = estimateUsageForSocketCount(getConnectedSocketCount());
  const onePlayer = estimateUsageForSocketCount(1);
  const fivePlayers = estimateUsageForSocketCount(5);
  const tenPlayers = estimateUsageForSocketCount(10);
  const twentyFivePlayers = estimateUsageForSocketCount(25);

  analytics.estimatedBytesPerSecond = roundMetric(current.bytesPerSecond, 2);
  analytics.estimatedMBPerHourPerPlayer = roundMetric(onePlayer.mbPerHour, 3);
  analytics.estimatedGBPerHourPerPlayer = roundMetric(onePlayer.gbPerHour, 4);
  analytics.estimatedHoursFor5GBAtCurrentLoad = roundMetric(current.hoursFor5GB, 2);
  analytics.estimatedHoursFor5GBAt1Player = roundMetric(onePlayer.hoursFor5GB, 2);
  analytics.estimatedHoursFor5GBAt5Players = roundMetric(fivePlayers.hoursFor5GB, 2);
  analytics.estimatedHoursFor5GBAt10Players = roundMetric(tenPlayers.hoursFor5GB, 2);
  analytics.estimatedHoursFor5GBAt25Players = roundMetric(twentyFivePlayers.hoursFor5GB, 2);
}

function recordStatePayloadSample(payloadBytes) {
  analytics.lastStatePayloadBytes = payloadBytes;
  analytics.peakStatePayloadBytes = Math.max(analytics.peakStatePayloadBytes, payloadBytes);
  analytics.minStatePayloadBytes = analytics.minStatePayloadBytes === 0
    ? payloadBytes
    : Math.min(analytics.minStatePayloadBytes, payloadBytes);
  analytics.totalStatePayloadBytes += payloadBytes;
  analytics.totalStatePayloadSamples += 1;
  analytics.averageStatePayloadBytes = roundMetric(
    analytics.totalStatePayloadBytes / analytics.totalStatePayloadSamples,
    2
  );
}

function createBandwidthSnapshot() {
  const connectedSockets = getConnectedSocketCount();
  const current = estimateUsageForSocketCount(connectedSockets);
  const onePlayer = estimateUsageForSocketCount(1);
  const fivePlayers = estimateUsageForSocketCount(5);
  const tenPlayers = estimateUsageForSocketCount(10);
  const twentyFivePlayers = estimateUsageForSocketCount(25);

  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    broadcastRate: BROADCAST_RATE,
    socketOverheadMultiplier: SOCKET_OVERHEAD_MULTIPLIER,
    activePlayers: analytics.activePlayers,
    totalSocketConnections: analytics.totalSocketConnections,
    connectedSockets,
    totalEstimatedBytesSent: Math.round(analytics.totalEstimatedBytesSent),
    totalEstimatedStateBytesSent: Math.round(analytics.totalEstimatedStateBytesSent),
    totalStateBroadcasts: analytics.totalStateBroadcasts,
    minimumStatePayloadBytes: analytics.minStatePayloadBytes,
    lastStatePayloadBytes: analytics.lastStatePayloadBytes,
    averageStatePayloadBytes: analytics.averageStatePayloadBytes,
    peakStatePayloadBytes: analytics.peakStatePayloadBytes,
    estimatedBytesPerSecondCurrent: roundMetric(current.bytesPerSecond, 2),
    estimatedMBPerHourCurrent: roundMetric(current.mbPerHour, 3),
    estimatedGBPerHourCurrent: roundMetric(current.gbPerHour, 4),
    estimatedHoursFor5GBCurrent: roundMetric(current.hoursFor5GB, 2),
    estimates: {
      onePlayer: {
        mbPerHour: roundMetric(onePlayer.mbPerHour, 3),
        gbPerHour: roundMetric(onePlayer.gbPerHour, 4),
        hoursFor5GB: roundMetric(onePlayer.hoursFor5GB, 2)
      },
      fivePlayers: {
        mbPerHour: roundMetric(fivePlayers.mbPerHour, 3),
        gbPerHour: roundMetric(fivePlayers.gbPerHour, 4),
        hoursFor5GB: roundMetric(fivePlayers.hoursFor5GB, 2)
      },
      tenPlayers: {
        mbPerHour: roundMetric(tenPlayers.mbPerHour, 3),
        gbPerHour: roundMetric(tenPlayers.gbPerHour, 4),
        hoursFor5GB: roundMetric(tenPlayers.hoursFor5GB, 2)
      },
      twentyFivePlayers: {
        mbPerHour: roundMetric(twentyFivePlayers.mbPerHour, 3),
        gbPerHour: roundMetric(twentyFivePlayers.gbPerHour, 4),
        hoursFor5GB: roundMetric(twentyFivePlayers.hoursFor5GB, 2)
      }
    },
    notes: [
      "These are estimates based on JSON payload size plus a Socket.IO overhead multiplier.",
      "Render bandwidth may also include static assets, dashboard traffic, reconnects, and other outbound responses."
    ]
  };
}

function logBandwidthEstimate() {
  const snapshot = createBandwidthSnapshot();
  const averagePayloadKB = roundMetric(snapshot.averageStatePayloadBytes / BYTES_PER_KB, 1);

  console.log("Bandwidth estimate:");
  console.log(`Average state payload: ${averagePayloadKB} KB`);
  console.log(`Broadcast rate: ${snapshot.broadcastRate} per second`);
  console.log(`Connected sockets: ${snapshot.connectedSockets}`);
  console.log(`Estimated current usage: ${snapshot.estimatedGBPerHourCurrent} GB per hour`);
  console.log(`Estimated 5 GB life at current load: ${snapshot.estimatedHoursFor5GBCurrent} hours`);
}

function getStatsAccessError(req) {
  const token = process.env.STATS_TOKEN;
  if (!token) {
    return {
      status: 403,
      code: "stats_token_missing",
      message: "STATS_TOKEN is not configured on this server."
    };
  }

  const providedToken = typeof req.query.token === "string" ? req.query.token : "";
  if (!providedToken) {
    return {
      status: 403,
      code: "stats_token_required",
      message: "Missing token. Open this page with ?token=YOUR_SECRET."
    };
  }

  if (providedToken !== token) {
    return {
      status: 403,
      code: "stats_token_invalid",
      message: "Invalid token."
    };
  }

  return null;
}

function renderStatsDashboardPage() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SwarmCore Analytics</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #05070d;
      --panel: rgba(12, 16, 27, 0.84);
      --panel-strong: rgba(16, 21, 36, 0.94);
      --line: rgba(255, 255, 255, 0.09);
      --text: #eef6ff;
      --muted: rgba(238, 246, 255, 0.64);
      --cyan: #59f3ff;
      --amber: #ffe66d;
      --rose: #ff4f70;
      --mint: #54ffc6;
      --shadow: 0 24px 80px rgba(0, 0, 0, 0.45);
    }

    * {
      box-sizing: border-box;
    }

    html,
    body {
      margin: 0;
      min-height: 100%;
      background:
        radial-gradient(circle at 15% 20%, rgba(89, 243, 255, 0.16), transparent 24rem),
        radial-gradient(circle at 85% 18%, rgba(255, 79, 112, 0.14), transparent 22rem),
        radial-gradient(circle at 50% 120%, rgba(255, 230, 109, 0.12), transparent 30rem),
        linear-gradient(180deg, #05070d 0%, #080b14 48%, #03050a 100%);
      color: var(--text);
      font-family: "Segoe UI Variable", "Bahnschrift", "Trebuchet MS", system-ui, sans-serif;
    }

    body {
      padding: 24px;
    }

    .shell {
      max-width: 1440px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }

    .hero,
    .panel,
    .event-item,
    .empty,
    .locked {
      border: 1px solid var(--line);
      background: var(--panel);
      box-shadow: var(--shadow);
      backdrop-filter: blur(18px);
    }

    .hero {
      position: relative;
      overflow: hidden;
      border-radius: 22px;
      padding: 26px;
      background:
        linear-gradient(145deg, rgba(8, 12, 22, 0.96), rgba(16, 21, 36, 0.88)),
        radial-gradient(circle at top right, rgba(89, 243, 255, 0.2), transparent 18rem);
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: auto -80px -100px auto;
      width: 240px;
      height: 240px;
      background: radial-gradient(circle, rgba(89, 243, 255, 0.2), transparent 70%);
      pointer-events: none;
    }

    .eyebrow {
      color: var(--cyan);
      font-size: 12px;
      font-weight: 900;
      letter-spacing: 0.24em;
      text-transform: uppercase;
    }

    .hero-row {
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: flex-start;
      flex-wrap: wrap;
    }

    .hero h1 {
      margin: 8px 0 8px;
      font-size: clamp(32px, 5vw, 58px);
      line-height: 0.95;
      letter-spacing: -0.04em;
      text-transform: uppercase;
    }

    .hero-sub {
      color: var(--muted);
      font-size: 15px;
      max-width: 760px;
    }

    .hero-tools {
      display: grid;
      gap: 10px;
      justify-items: end;
      min-width: 240px;
    }

    .timestamp {
      color: var(--muted);
      font-size: 13px;
      text-align: right;
    }

    .button-row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    button,
    .chip {
      appearance: none;
      border: 1px solid rgba(255, 255, 255, 0.12);
      background: rgba(255, 255, 255, 0.06);
      color: var(--text);
      border-radius: 999px;
      padding: 10px 15px;
      font: inherit;
      font-weight: 800;
      letter-spacing: 0.01em;
      cursor: pointer;
      transition: 140ms ease;
    }

    button:hover,
    .chip.active {
      border-color: rgba(89, 243, 255, 0.52);
      background: rgba(89, 243, 255, 0.12);
      box-shadow: 0 0 0 1px rgba(89, 243, 255, 0.08) inset;
    }

    button:disabled {
      opacity: 0.6;
      cursor: wait;
    }

    .statusbar {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      align-items: center;
    }

    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 8px 12px;
      background: rgba(84, 255, 198, 0.1);
      border: 1px solid rgba(84, 255, 198, 0.18);
      color: #d9fff3;
      font-size: 13px;
      font-weight: 800;
    }

    .status-pill.warn {
      background: rgba(255, 230, 109, 0.1);
      border-color: rgba(255, 230, 109, 0.18);
      color: #fff8cf;
    }

    .status-pill.danger {
      background: rgba(255, 79, 112, 0.1);
      border-color: rgba(255, 79, 112, 0.2);
      color: #ffd3db;
    }

    .status-pill::before {
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: currentColor;
      box-shadow: 0 0 14px currentColor;
    }

    .error-banner {
      display: none;
      border-radius: 16px;
      padding: 14px 16px;
      background: rgba(255, 79, 112, 0.1);
      border: 1px solid rgba(255, 79, 112, 0.22);
      color: #ffd8df;
      font-weight: 700;
    }

    .error-banner.visible {
      display: block;
    }

    .grid {
      display: grid;
      gap: 16px;
    }

    .stats-grid {
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }

    .combat-grid {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }

    .insight-grid {
      grid-template-columns: 1.2fr 0.8fr;
      align-items: start;
    }

    .panel {
      border-radius: 20px;
      padding: 18px;
      background: linear-gradient(180deg, rgba(13, 18, 31, 0.95), rgba(10, 14, 24, 0.88));
    }

    .panel-title {
      margin: 0 0 14px;
      font-size: 13px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-weight: 900;
    }

    .panel-subtitle {
      margin: -4px 0 14px;
      color: var(--muted);
      font-size: 13px;
    }

    .stat-card {
      position: relative;
      overflow: hidden;
      min-height: 136px;
    }

    .stat-card::before {
      content: "";
      position: absolute;
      inset: 0 auto auto 0;
      width: 100%;
      height: 3px;
      background: linear-gradient(90deg, var(--cyan), rgba(255, 230, 109, 0.6));
      opacity: 0.8;
    }

    .stat-label {
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
      font-weight: 900;
    }

    .stat-value {
      margin-top: 18px;
      font-size: clamp(32px, 4vw, 48px);
      font-weight: 900;
      line-height: 0.95;
      letter-spacing: -0.04em;
    }

    .stat-note {
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
    }

    .combat-note {
      margin-top: 12px;
      color: var(--muted);
      font-size: 13px;
    }

    .session-metrics {
      display: grid;
      gap: 12px;
    }

    .metric-row {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 0;
      border-bottom: 1px solid rgba(255, 255, 255, 0.08);
    }

    .metric-row:last-child {
      border-bottom: 0;
      padding-bottom: 0;
    }

    .metric-label {
      color: var(--muted);
      font-weight: 700;
    }

    .metric-value {
      font-weight: 900;
    }

    .insight-message {
      display: grid;
      gap: 14px;
      align-content: start;
    }

    .message-card {
      border-radius: 18px;
      padding: 18px;
      background: linear-gradient(180deg, rgba(89, 243, 255, 0.09), rgba(255, 255, 255, 0.03));
      border: 1px solid rgba(89, 243, 255, 0.14);
    }

    .message-title {
      color: var(--cyan);
      text-transform: uppercase;
      letter-spacing: 0.14em;
      font-size: 12px;
      font-weight: 900;
    }

    .message-body {
      margin-top: 10px;
      font-size: 24px;
      font-weight: 900;
      line-height: 1.1;
    }

    .event-tools {
      display: flex;
      justify-content: space-between;
      gap: 14px;
      flex-wrap: wrap;
      margin-bottom: 14px;
      align-items: center;
    }

    .filter-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }

    .events {
      display: grid;
      gap: 10px;
      max-height: 720px;
      overflow: auto;
      padding-right: 2px;
    }

    .event-item,
    .empty,
    .locked {
      border-radius: 16px;
      padding: 14px 16px;
    }

    .event-item {
      display: grid;
      gap: 8px;
      background: linear-gradient(180deg, rgba(14, 19, 32, 0.96), rgba(10, 14, 24, 0.9));
    }

    .event-meta {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--muted);
      font-weight: 900;
    }

    .event-summary {
      font-size: 15px;
      font-weight: 800;
      color: var(--text);
    }

    .event-tags {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }

    .tag {
      border-radius: 999px;
      padding: 5px 9px;
      background: rgba(255, 255, 255, 0.06);
      color: var(--muted);
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }

    .tag.player {
      color: #d6ffff;
      background: rgba(89, 243, 255, 0.1);
    }

    .tag.bot {
      color: #fff6ca;
      background: rgba(255, 230, 109, 0.1);
    }

    .tag.warn {
      color: #ffd1da;
      background: rgba(255, 79, 112, 0.1);
    }

    .empty,
    .locked {
      text-align: center;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.04);
    }

    @media (max-width: 1080px) {
      .stats-grid,
      .combat-grid,
      .insight-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    @media (max-width: 760px) {
      body {
        padding: 14px;
      }

      .hero,
      .panel,
      .event-item,
      .empty,
      .locked {
        border-radius: 18px;
      }

      .hero {
        padding: 18px;
      }

      .hero-row,
      .event-tools {
        display: grid;
      }

      .hero-tools {
        justify-items: start;
      }

      .button-row {
        justify-content: flex-start;
      }

      .stats-grid,
      .combat-grid,
      .insight-grid {
        grid-template-columns: 1fr;
      }

      .stat-card {
        min-height: 112px;
      }

      .events {
        max-height: none;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-row">
        <div>
          <div class="eyebrow">Early Public Testing</div>
          <h1>SwarmCore Analytics</h1>
          <div class="hero-sub">Live admin view for early multiplayer playtesting. Real player metrics are separated from bot combat so you can see who actually starts, stays, dies, and fights.</div>
        </div>
        <div class="hero-tools">
          <div class="timestamp">Last updated: <strong id="lastUpdated">Waiting for data…</strong></div>
          <div class="button-row">
            <button id="refreshButton" type="button">Refresh</button>
          </div>
        </div>
      </div>
      <div class="statusbar">
        <div class="status-pill" id="headlineStatus">Waiting for analytics…</div>
      </div>
    </section>

    <div id="errorBanner" class="error-banner"></div>

    <section class="grid stats-grid" id="mainStats"></section>

    <section class="panel">
      <h2 class="panel-title">Combat Breakdown</h2>
      <div class="panel-subtitle">Bot combat is tracked separately so real player testing does not get polluted.</div>
      <div class="grid combat-grid" id="combatStats"></div>
      <div class="combat-note">Player-facing combat health is mostly visible through player-only cards above. The bot cards below help you judge how noisy the sandbox currently is.</div>
    </section>

    <section class="grid insight-grid">
      <div class="panel">
        <h2 class="panel-title">Session Quality</h2>
        <div class="session-metrics" id="sessionMetrics"></div>
      </div>
      <div class="panel insight-message">
        <div class="message-card">
          <div class="message-title">Testing Signal</div>
          <div class="message-body" id="qualityMessage">Waiting for data…</div>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="event-tools">
        <div>
          <h2 class="panel-title" style="margin-bottom: 6px;">Recent Events</h2>
          <div class="panel-subtitle" style="margin: 0;">Readable event stream with quick filters for player activity, bot combat, and connection flow.</div>
        </div>
        <div class="filter-row" id="eventFilters">
          <button class="chip active" data-filter="all" type="button">All</button>
          <button class="chip" data-filter="players" type="button">Players Only</button>
          <button class="chip" data-filter="bot" type="button">Bot Combat</button>
          <button class="chip" data-filter="connections" type="button">Connections</button>
        </div>
      </div>
      <div class="events" id="eventsList"></div>
    </section>
  </div>

  <script>
    (() => {
      const params = new URLSearchParams(window.location.search);
      const token = params.get('token');
      const lastUpdatedEl = document.getElementById('lastUpdated');
      const errorBanner = document.getElementById('errorBanner');
      const headlineStatus = document.getElementById('headlineStatus');
      const mainStats = document.getElementById('mainStats');
      const combatStats = document.getElementById('combatStats');
      const sessionMetrics = document.getElementById('sessionMetrics');
      const qualityMessage = document.getElementById('qualityMessage');
      const eventsList = document.getElementById('eventsList');
      const refreshButton = document.getElementById('refreshButton');
      const eventFilters = document.getElementById('eventFilters');

      let statsCache = null;
      let activeFilter = 'all';
      let loading = false;

      function formatNumber(value) {
        return new Intl.NumberFormat().format(Number.isFinite(value) ? value : 0);
      }

      function formatPercent(value) {
        return Number.isFinite(value) ? value.toFixed(value >= 10 ? 0 : 1) + '%' : '0%';
      }

      function formatSeconds(value) {
        const seconds = Number.isFinite(value) ? value : 0;
        if (seconds >= 3600) return (seconds / 3600).toFixed(1) + 'h';
        if (seconds >= 60) return (seconds / 60).toFixed(1) + 'm';
        return seconds.toFixed(1) + 's';
      }

      function setError(message) {
        if (!message) {
          errorBanner.classList.remove('visible');
          errorBanner.textContent = '';
          return;
        }
        errorBanner.textContent = message;
        errorBanner.classList.add('visible');
      }

      function escapeHtml(value) {
        return String(value).replace(/[&<>"']/g, (char) => ({
          '&': '&amp;',
          '<': '&lt;',
          '>': '&gt;',
          '"': '&quot;',
          "'": '&#39;'
        })[char]);
      }

      function statusTone(message) {
        if (/no real players/i.test(message)) return 'warn';
        if (/dying fast/i.test(message)) return 'danger';
        return '';
      }

      function computeQualityMessage(stats) {
        if (!stats || !stats.totalGameStarts) return 'No real players yet';
        if (stats.averageSessionSeconds >= 150 && stats.completedSessions >= 3) return 'Players are sticking around';
        if (stats.totalDeaths > Math.max(3, stats.totalGameStarts * 1.5)) return 'People are dying fast';
        if (stats.activePlayers > 0 || stats.sessionsStarted > 0) return 'Players are testing';
        return 'Players are testing';
      }

      function renderCards(target, cards) {
        target.innerHTML = cards.map((card) => {
          return '<article class="panel stat-card">'
            + '<div class="stat-label">' + escapeHtml(card.label) + '</div>'
            + '<div class="stat-value">' + escapeHtml(card.value) + '</div>'
            + '<div class="stat-note">' + escapeHtml(card.note || '') + '</div>'
            + '</article>';
        }).join('');
      }

      function summarizeEvent(event) {
        const type = event && event.type ? event.type : 'unknown';
        const details = event && event.details ? event.details : {};

        if (type === 'socket_connected') return 'Socket connected';
        if (type === 'socket_disconnected') {
          return details.hadStartedSession ? 'Player disconnected after starting a session' : 'Visitor disconnected before starting';
        }
        if (type === 'game_start') {
          return details.playerName ? 'Player started a session as ' + details.playerName : 'Player started a session';
        }
        if (type === 'kill') {
          if (details.isKillerBot && details.isVictimBot) return 'Bot killed bot';
          if (details.isKillerBot && !details.isVictimBot) return 'Bot killed player';
          if (!details.isKillerBot && details.isVictimBot) return 'Player killed bot';
          return 'Player killed player';
        }
        if (type === 'death') {
          if (details.isVictimBot && details.isKillerBot) return 'Bot died to bot combat';
          if (details.isVictimBot && !details.isKillerBot) return 'Bot died to player combat';
          if (!details.isVictimBot && details.isKillerBot) return 'Player died to bot combat';
          return 'Player died in player combat';
        }

        return type.replace(/_/g, ' ');
      }

      function eventTags(event) {
        const tags = [];
        const details = event && event.details ? event.details : {};

        if (event.type === 'socket_connected' || event.type === 'socket_disconnected') {
          tags.push({ tone: 'player', text: 'connection' });
        }
        if (event.type === 'game_start') {
          tags.push({ tone: 'player', text: 'session' });
        }
        if (details.isKillerBot === false || details.isVictimBot === false) {
          tags.push({ tone: 'player', text: 'player' });
        }
        if (details.isKillerBot === true || details.isVictimBot === true) {
          tags.push({ tone: 'bot', text: 'bot' });
        }
        if (event.type === 'death' || event.type === 'kill') {
          tags.push({ tone: 'warn', text: event.type });
        }

        return tags;
      }

      function matchesFilter(event, filterName) {
        const details = event && event.details ? event.details : {};

        if (filterName === 'all') return true;
        if (filterName === 'connections') {
          return event.type === 'socket_connected' || event.type === 'socket_disconnected';
        }
        if (filterName === 'bot') {
          return details.isVictimBot === true || details.isKillerBot === true;
        }
        if (filterName === 'players') {
          return event.type === 'socket_connected'
            || event.type === 'socket_disconnected'
            || details.hadStartedSession === true
            || (details.isVictimBot === false && details.isKillerBot === false);
        }
        return true;
      }

      function renderEvents(stats) {
        const events = Array.isArray(stats && stats.recentEvents) ? stats.recentEvents.slice().reverse() : [];
        const filtered = events.filter((event) => matchesFilter(event, activeFilter));

        if (!filtered.length) {
          eventsList.innerHTML = '<div class="empty">No events match this filter yet.</div>';
          return;
        }

        eventsList.innerHTML = filtered.map((event) => {
          const time = event && event.at ? new Date(event.at) : null;
          const label = time && !Number.isNaN(time.getTime())
            ? time.toLocaleString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', month: 'short', day: 'numeric' })
            : 'Unknown time';
          const tags = eventTags(event).map((tag) => '<span class="tag ' + escapeHtml(tag.tone) + '">' + escapeHtml(tag.text) + '</span>').join('');
          return '<article class="event-item">'
            + '<div class="event-meta"><span>' + escapeHtml(event.type || 'event') + '</span><span>' + escapeHtml(label) + '</span></div>'
            + '<div class="event-summary">' + escapeHtml(summarizeEvent(event)) + '</div>'
            + '<div class="event-tags">' + tags + '</div>'
            + '</article>';
        }).join('');
      }

      function renderSessionQuality(stats) {
        const socketConnections = Math.max(0, stats.totalSocketConnections || 0);
        const starts = Math.max(0, stats.totalGameStarts || 0);
        const sessionsStarted = Math.max(0, stats.sessionsStarted || 0);
        const completed = Math.max(0, stats.completedSessions || 0);
        const conversion = socketConnections > 0 ? (starts / socketConnections) * 100 : 0;
        const completionRate = sessionsStarted > 0 ? (completed / sessionsStarted) * 100 : 0;

        sessionMetrics.innerHTML = [
          ['Socket to Start Conversion', formatPercent(conversion)],
          ['Completed Session Rate', formatPercent(completionRate)],
          ['Average Session Length', formatSeconds(stats.averageSessionSeconds || 0)],
          ['Active Session Count', formatNumber(stats.activePlayers || 0)]
        ].map((row) => {
          return '<div class="metric-row"><div class="metric-label">' + escapeHtml(row[0]) + '</div><div class="metric-value">' + escapeHtml(row[1]) + '</div></div>';
        }).join('');

        const message = computeQualityMessage(stats);
        qualityMessage.textContent = message;
        headlineStatus.textContent = message;
        headlineStatus.className = 'status-pill ' + statusTone(message);
      }

      function renderDashboard(stats) {
        statsCache = stats;
        setError('');

        const updatedAt = stats.generatedAt ? new Date(stats.generatedAt) : null;
        lastUpdatedEl.textContent = updatedAt && !Number.isNaN(updatedAt.getTime())
          ? updatedAt.toLocaleString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', month: 'short', day: 'numeric' })
          : 'Unknown';

        renderCards(mainStats, [
          { label: 'Active Players', value: formatNumber(stats.activePlayers), note: 'Live started sessions' },
          { label: 'Peak Active Players', value: formatNumber(stats.peakActivePlayers), note: 'Highest concurrent tester count seen' },
          { label: 'Total Game Starts', value: formatNumber(stats.totalGameStarts), note: 'Real testers only via hello event' },
          { label: 'Total Socket Connections', value: formatNumber(stats.totalSocketConnections), note: 'All incoming sockets before game start' },
          { label: 'Completed Sessions', value: formatNumber(stats.completedSessions), note: 'Sessions that ended with disconnect after starting' },
          { label: 'Average Session Seconds', value: formatSeconds(stats.averageSessionSeconds), note: 'Average across completed sessions' },
          { label: 'Player Deaths', value: formatNumber(stats.totalDeaths), note: 'Human player deaths only' },
          { label: 'Player Kills', value: formatNumber(stats.totalKills), note: 'Human player kills only' }
        ]);

        renderCards(combatStats, [
          { label: 'Player Killed Bot', value: formatNumber(stats.playerKilledBot), note: 'Human over bot eliminations' },
          { label: 'Bot Killed Player', value: formatNumber(stats.botKilledPlayer), note: 'Bot pressure on real testers' },
          { label: 'Player Killed Player', value: formatNumber(stats.playerKilledPlayer), note: 'Real PvP eliminations' },
          { label: 'Bot Killed Bot', value: formatNumber(stats.botKilledBot), note: 'Background bot combat noise' },
          { label: 'Bot Deaths', value: formatNumber(stats.botDeaths), note: 'All dead bots' },
          { label: 'Bot Kills', value: formatNumber(stats.botKills), note: 'All kills made by bots' }
        ]);

        renderSessionQuality(stats);
        renderEvents(stats);
      }

      async function loadStats() {
        if (loading) return;
        if (!token) {
          setError('Missing token in dashboard URL. Open /admin/dashboard?token=YOUR_SECRET.');
          headlineStatus.textContent = 'Dashboard locked';
          headlineStatus.className = 'status-pill danger';
          return;
        }

        loading = true;
        refreshButton.disabled = true;
        try {
          const response = await fetch('/admin/stats?token=' + encodeURIComponent(token), {
            cache: 'no-store',
            headers: { 'Accept': 'application/json' }
          });

          if (response.status === 403) {
            let message = 'Locked. Token rejected by /admin/stats.';
            try {
              const body = await response.json();
              if (body && body.message) message = body.message;
            } catch (error) {
              // ignore parse failure
            }
            setError(message);
            headlineStatus.textContent = 'Dashboard locked';
            headlineStatus.className = 'status-pill danger';
            return;
          }

          if (!response.ok) {
            throw new Error('Stats request failed with status ' + response.status);
          }

          const data = await response.json();
          renderDashboard(data);
        } catch (error) {
          setError(error && error.message ? error.message : 'Unable to refresh analytics right now.');
        } finally {
          refreshButton.disabled = false;
          loading = false;
        }
      }

      refreshButton.addEventListener('click', () => {
        loadStats();
      });

      eventFilters.addEventListener('click', (event) => {
        const button = event.target.closest('[data-filter]');
        if (!button) return;
        activeFilter = button.getAttribute('data-filter') || 'all';
        for (const chip of eventFilters.querySelectorAll('[data-filter]')) {
          chip.classList.toggle('active', chip === button);
        }
        if (statsCache) renderEvents(statsCache);
      });

      loadStats();
      window.setInterval(loadStats, 5000);
    })();
  </script>
</body>
</html>`;
}

function renderStatsLockedPage(title, message) {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${title}</title>
  <style>
    html, body {
      margin: 0;
      min-height: 100%;
      background: radial-gradient(circle at top, rgba(255, 79, 112, 0.16), transparent 20rem), linear-gradient(180deg, #05070d, #03050a);
      color: #eef6ff;
      font-family: "Segoe UI Variable", "Bahnschrift", system-ui, sans-serif;
    }
    body {
      display: grid;
      place-items: center;
      padding: 24px;
    }
    .locked {
      max-width: 720px;
      border-radius: 22px;
      padding: 28px;
      border: 1px solid rgba(255, 255, 255, 0.1);
      background: rgba(12, 16, 27, 0.92);
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.45);
    }
    .eyebrow {
      color: #ff4f70;
      font-size: 12px;
      font-weight: 900;
      letter-spacing: 0.24em;
      text-transform: uppercase;
    }
    h1 {
      margin: 12px 0 10px;
      font-size: clamp(28px, 5vw, 46px);
      line-height: 0.96;
      text-transform: uppercase;
    }
    p {
      margin: 0;
      color: rgba(238, 246, 255, 0.7);
      font-size: 16px;
      line-height: 1.5;
    }
  </style>
</head>
<body>
  <section class="locked">
    <div class="eyebrow">Access Restricted</div>
    <h1>${title}</h1>
    <p>${message}</p>
  </section>
</body>
</html>`;
}

loadStatsFromDisk();
resetBandwidthAnalytics();
recalculateAverageSessionSeconds();
recalculateActivePlayers();

setInterval(() => {
  flushStatsToDisk();
}, STATS_FLUSH_INTERVAL_MS);

// NOTE: local JSON persistence can be lost on Render restart/redeploy; move to durable storage later.

app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

app.get("/admin/stats", (req, res) => {
  const authError = getStatsAccessError(req);
  if (authError) {
    return res.status(403).json({
      ok: false,
      code: authError.code,
      message: authError.message
    });
  }
  return res.json(analyticsSnapshot());
});

app.get("/admin/bandwidth", (req, res) => {
  const authError = getStatsAccessError(req);
  if (authError) {
    return res.status(403).json({
      ok: false,
      code: authError.code,
      message: authError.message
    });
  }

  return res.json(createBandwidthSnapshot());
});

app.get("/admin/dashboard", (req, res) => {
  const authError = getStatsAccessError(req);
  if (authError) {
    return res
      .status(authError.status)
      .send(renderStatsLockedPage("Dashboard Locked", authError.message));
  }

  return res.type("html").send(renderStatsDashboardPage());
});

app.use(express.static(path.join(__dirname, "public"), {
  setHeaders: (res) => {
    res.setHeader("Cache-Control", "no-store");
  }
}));

const players = new Map();
const shards = new Map();
const bots = new Map();
let shardId = 1;
let botId = 1;
let startTime = Date.now();
let tickCount = 0;
let worldRadius = BASE_WORLD_RADIUS;

const asteroids = [];

const botNames = [
  "Nova", "Vex", "Kite", "Ion", "Hex", "Rift", "Flux", "Juno",
  "Nyx", "Bolt", "Echo", "Zed", "Luma", "Crow", "Sable"
];

const SKINS = {
  cyan: { color: "#55f7ff", palette: ["#55f7ff", "#9ffbff"] },
  magenta: { color: "#ff4fd8", palette: ["#ff4fd8", "#ff9bea"] },
  amber: { color: "#ffe66d", palette: ["#ffe66d", "#ffb84d"] },
  venom: { color: "#7dff8a", palette: ["#7dff8a", "#54ffc6"] },
  flare: { color: "#ff8155", palette: ["#ff8155", "#ffe66d", "#ff4f70"] },
  violet: { color: "#9c7dff", palette: ["#9c7dff", "#ff4fd8"] },
  glacier: { color: "#4da3ff", palette: ["#4da3ff", "#55f7ff", "#f9fbff"] },
  ghost: { color: "#f9fbff", palette: ["#f9fbff", "#b9c8ff"] },
  prism: { color: "#55f7ff", palette: ["#55f7ff", "#ff4fd8", "#ffe66d", "#7dff8a"] },
  inferno: { color: "#ff4f70", palette: ["#ff4f70", "#ff8155", "#ffe66d"] },
  aurora: { color: "#54ffc6", palette: ["#54ffc6", "#55f7ff", "#9c7dff", "#ff4fd8"] },
  eclipse: { color: "#f9fbff", palette: ["#f9fbff", "#9c7dff", "#141827"] },
  candy: { color: "#ff9bea", palette: ["#ff9bea", "#55f7ff", "#ffe66d"] },
  hazard: { color: "#ffe66d", palette: ["#ffe66d", "#101827", "#ff8155"] },
  plasma: { color: "#ff4fd8", palette: ["#ff4fd8", "#9c7dff", "#55f7ff"] },
  matrix: { color: "#7dff8a", palette: ["#7dff8a", "#54ffc6", "#d8ffe0"] }
};

function rand(min, max) {
  return min + Math.random() * (max - min);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function distSq(a, b) {
  const dx = a.x - b.x;
  const dy = a.y - b.y;
  return dx * dx + dy * dy;
}

function safeName(value) {
  const name = String(value || "").replace(/[^\w .-]/g, "").trim();
  return name.slice(0, 16) || "Core";
}

function randomColor() {
  const colors = Object.values(SKINS).map((skin) => skin.color);
  return colors[Math.floor(Math.random() * colors.length)];
}

function isBotId(id) {
  return typeof id === "string" && id.startsWith("bot");
}

function normalizeSkin(skinId) {
  return SKINS[skinId] ? skinId : "cyan";
}

function applySkin(player, skinId) {
  player.skin = normalizeSkin(skinId);
  player.color = SKINS[player.skin].color;
  player.palette = SKINS[player.skin].palette;
}

function randomPoint(inner = 0, outer = worldRadius - 80) {
  const a = Math.random() * Math.PI * 2;
  const r = Math.sqrt(rand(inner * inner, outer * outer));
  return { x: Math.cos(a) * r, y: Math.sin(a) * r };
}

function levelFor(drones) {
  const progress = clamp((Math.max(1, drones) - 1) / (MAX_DRONES - 1), 0, 1);
  return clamp(1 + Math.floor(Math.sqrt(progress) * 99), 1, 100);
}

function titanTier(player) {
  if (player.drones >= MAX_DRONES) return "god";
  if (player.drones >= MEGA_TITAN_DRONES) return "mega";
  if (player.drones >= TITAN_DRONES) return "titan";
  return "normal";
}

function weakSpotCount(player) {
  if (player.drones < TITAN_DRONES) return 0;
  const progress = clamp((player.drones - TITAN_DRONES) / (MAX_DRONES - TITAN_DRONES), 0, 1);
  return clamp(3 + Math.floor(progress * 6), 3, 8);
}

function weakSpotOrbitRadius(player) {
  return (player.drones > 1 ? droneDistance(player, player.drones - 1) : 90) + 28;
}

function syncWeakSpots(player, now = Date.now()) {
  const count = weakSpotCount(player);
  if (!count) {
    player.weakSpots = [];
    return;
  }

  while (player.weakSpots.length < count) {
    player.weakSpots.push({
      id: `${player.id}-weak-${player.weakSpots.length}`,
      hp: 3,
      seed: rand(0, Math.PI * 2),
      brokenUntil: 0
    });
  }
  if (player.weakSpots.length > count) player.weakSpots.length = count;

  for (let i = 0; i < player.weakSpots.length; i++) {
    const spot = player.weakSpots[i];
    if (spot.brokenUntil && now > spot.brokenUntil) {
      spot.hp = 3;
      spot.brokenUntil = 0;
    }
  }
}

function weakSpotPositions(player, now = Date.now()) {
  syncWeakSpots(player, now);
  const count = player.weakSpots.length;
  if (!count) return [];

  const radius = weakSpotOrbitRadius(player);
  return player.weakSpots.map((spot, index) => {
    const angle = (index / count) * Math.PI * 2 + spot.seed * 0.18;
    return {
      id: spot.id,
      index,
      hp: spot.hp,
      broken: spot.brokenUntil > now,
      x: player.x + Math.cos(angle) * radius,
      y: player.y + Math.sin(angle) * radius,
      angle,
      r: 22 + Math.min(18, Math.sqrt(player.drones) * 0.22)
    };
  });
}

function damageWeakSpot(attacker, titan, spotIndex, now, damage = 1) {
  if (!attacker.alive || !titan.alive || titan.drones < TITAN_DRONES) return false;
  if (attacker.id === titan.id || attacker.drones >= titan.drones) return false;

  syncWeakSpots(titan, now);
  const spot = titan.weakSpots[spotIndex];
  if (!spot || spot.brokenUntil > now) return false;

  const key = `${titan.id}:${spot.id}`;
  const last = attacker.weakSpotHitCooldown.get(key) || 0;
  if (now - last < 360) return false;
  attacker.weakSpotHitCooldown.set(key, now);

  spot.hp -= damage;
  attacker.needleHit = true;

  if (spot.hp <= 0) {
    spot.hp = 0;
    spot.brokenUntil = now + rand(6500, 9500);
    titan.exposedUntil = Math.max(titan.exposedUntil || 0, now + rand(2200, 4200));
    const chunk = clamp(Math.ceil(titan.drones * (titan.drones >= MEGA_TITAN_DRONES ? 0.08 : 0.055)), 80, 750);
    stripDrones(titan, chunk, {
      drop: true,
      value: 6,
      maxPieces: clamp(Math.ceil(Math.sqrt(chunk) * 2.2), 18, 90)
    });
    attacker.score += 15;
    return true;
  }

  return true;
}

function makePlayer(id, name, isBot = false) {
  const p = randomPoint(260, Math.min(worldRadius - 260, 2700));
  return {
    id,
    name: safeName(name),
    isBot,
    skin: "cyan",
    color: randomColor(),
    palette: SKINS.cyan.palette,
    x: p.x,
    y: p.y,
    vx: 0,
    vy: 0,
    angle: rand(0, Math.PI * 2),
    targetX: p.x,
    targetY: p.y,
    boosting: false,
    alive: true,
    energy: 0,
    score: 0,
    drones: 1,
    maxDrones: 1,
    style: "ring",
    styleChosen: false,
    orbit: rand(0, Math.PI * 2),
    lastBoostBurn: 0,
    boostStartedAt: 0,
    wasBoosting: false,
    needleDashUntil: 0,
    needleCooldownUntil: 0,
    needleVulnerableUntil: 0,
    needleHit: false,
    lastPulse: -99999,
    pulseUntil: 0,
    hitCooldown: new Map(),
    weakSpotHitCooldown: new Map(),
    weakSpots: [],
    exposedUntil: 0,
    respawnAt: 0,
    spawnGraceUntil: Date.now() + 2400,
    botTarget: randomPoint(0, Math.min(worldRadius - 200, 3200)),
    botThinkAt: 0,
    bountyRank: 0
  };
}

function spawnShard(x, y, value = 1, vx = 0, vy = 0, rare = false, ownerId = null, kind = "energy") {
  const safe = Math.hypot(x, y) < worldRadius - 30;
  if (!safe) return;
  const id = String(shardId++);
  shards.set(id, {
    id,
    x,
    y,
    vx,
    vy,
    value,
    rare,
    ownerId,
    kind,
    born: Date.now()
  });
}

function dropDroneLoot(player, amount, options = {}) {
  if (!amount || amount <= 0) return;

  const totalDrones = Math.floor(amount);
  const maxPieces = options.maxPieces || totalDrones;
  const pieces = Math.max(1, Math.min(totalDrones, maxPieces));
  const positions = getDronePositions(player, Math.min(pieces, 56));
  const valuePerDrone = options.value || 6;
  let dropped = 0;

  for (let i = 0; i < pieces; i++) {
    const position = positions[i % positions.length] || { x: player.x, y: player.y };
    const droneCount = Math.floor(totalDrones / pieces) + (i < totalDrones % pieces ? 1 : 0);
    const a = Math.atan2(position.y - player.y, position.x - player.x) + rand(-0.72, 0.72);
    const speed = rand(120, 420);
    spawnShard(
      position.x + rand(-14, 14),
      position.y + rand(-14, 14),
      valuePerDrone * droneCount,
      player.vx * 0.18 + Math.cos(a) * speed,
      player.vy * 0.18 + Math.sin(a) * speed,
      true,
      options.lockOwner ? player.id : null,
      "drone"
    );
    dropped += droneCount;
    if (dropped >= totalDrones) return;
  }
}

function fillShards() {
  while (shards.size < TARGET_SHARDS) {
    let p;
    const alive = [...players.values()].filter((player) => player.alive);
    if (alive.length && Math.random() < 0.45) {
      const near = alive[Math.floor(Math.random() * alive.length)];
      const a = Math.random() * Math.PI * 2;
      const r = rand(900, 3100);
      p = { x: near.x + Math.cos(a) * r, y: near.y + Math.sin(a) * r };
      if (Math.hypot(p.x, p.y) > worldRadius - 120) p = randomPoint(180, worldRadius - 120);
    } else {
      p = randomPoint(180, worldRadius - 120);
    }
    const rare = Math.random() < 0.025;
    spawnShard(p.x, p.y, rare ? 3 : 1, 0, 0, rare);
  }
}

function droneDistance(player, index) {
  const count = Math.max(1, player.drones);
  const maxRadius = 58 + Math.sqrt(count) * 31;
  if (player.style === "blade") {
    const step = Math.floor(index / 6);
    return 46 + Math.sqrt(step + 1) * 38;
  }
  if (player.style === "cloud") {
    const t = Math.sqrt((index + 1) / count);
    return 42 + t * maxRadius + Math.sin(player.orbit * 1.7 + index) * 22;
  }
  const ringSize = 36;
  const rings = Math.max(1, Math.ceil(count / ringSize));
  const layer = Math.floor(index / ringSize);
  return 54 + ((layer + 0.75) / rings) * maxRadius;
}

function droneAngle(player, index) {
  const count = Math.max(1, player.drones);
  if (player.style === "blade") {
    const arms = 6;
    const arm = index % arms;
    const step = Math.floor(index / arms);
    return player.orbit + arm * Math.PI * 2 / arms + step * 0.028;
  }
  if (player.style === "cloud") {
    return player.orbit * (0.8 + (index % 5) * 0.03) + index * 2.399;
  }
  const ringSize = 36;
  const position = index % ringSize;
  const layer = Math.floor(index / ringSize);
  return player.orbit + (position / Math.min(ringSize, count - layer * ringSize)) * Math.PI * 2 + layer * 0.19;
}

function getDronePositions(player, maxPositions = player.drones) {
  const positions = [];
  const count = Math.min(player.drones, maxPositions);
  const stride = Math.max(1, player.drones / Math.max(1, count));
  for (let i = 0; i < count; i++) {
    const droneIndex = Math.floor(i * stride);
    const a = droneAngle(player, droneIndex);
    const d = droneDistance(player, droneIndex);
    positions.push({
      i: droneIndex,
      x: player.x + Math.cos(a) * d,
      y: player.y + Math.sin(a) * d
    });
  }
  return positions;
}

function addEnergy(player, value) {
  player.energy += value;
  player.score += value;
  const earned = clamp(1 + Math.floor(player.energy / 10), 1, MAX_DRONES);
  if (earned > player.maxDrones) {
    const gained = earned - player.maxDrones;
    player.maxDrones = earned;
    player.drones = clamp(player.drones + gained, 0, player.maxDrones);
  }
}

function repairDrones(player, amount) {
  if (!player.alive || amount <= 0) return 0;
  const before = player.drones;
  player.drones = clamp(player.drones + amount, 0, player.maxDrones);
  return player.drones - before;
}

function stripDrones(player, amount, options = {}) {
  if (!player.alive) return 0;
  const lost = Math.min(player.drones, amount);
  if (options.drop) {
    dropDroneLoot(player, lost, {
      lockOwner: options.lockOwner !== false,
      value: options.value || 6
    });
  }
  player.drones -= lost;
  return lost;
}

function killPlayer(player, killer) {
  if (!player.alive) return;
  const isVictimBot = isBotId(player.id);
  const isKillerBot = Boolean(killer && isBotId(killer.id));

  if (isVictimBot) analytics.botDeaths += 1;
  else analytics.totalDeaths += 1;
  addRecentEvent("death", {
    playerId: player.id,
    killerId: killer && killer.id ? killer.id : null,
    bountyRank: player.bountyRank || 0,
    isVictimBot,
    isKillerBot
  });
  scheduleStatsFlushSoon();

  const remainingDrones = player.drones;
  const killedTier = titanTier(player);
  if (remainingDrones > 0) {
    dropDroneLoot(player, remainingDrones, {
      lockOwner: true,
      value: 6,
      maxPieces: clamp(Math.ceil(Math.sqrt(remainingDrones) * 3.6), 24, 260)
    });
    player.drones = 0;
  }
  player.alive = false;
  player.respawnAt = Date.now() + (player.isBot ? 1800 : 0);

  const loot = clamp(Math.floor(player.score / 24) + 4 + player.bountyRank * 8, 4, 28);
  for (let i = 0; i < loot; i++) {
    const a = Math.random() * Math.PI * 2;
    const speed = rand(90, 420);
    const value = Math.random() < 0.08 + player.bountyRank * 0.03 ? 3 : 1;
    spawnShard(
      player.x + Math.cos(a) * rand(18, 90),
      player.y + Math.sin(a) * rand(18, 90),
      value,
      Math.cos(a) * speed,
      Math.sin(a) * speed,
      value > 1
    );
  }

  if (killedTier !== "normal") {
    const bonus = killedTier === "god" ? 90 : killedTier === "mega" ? 55 : 28;
    for (let i = 0; i < bonus; i++) {
      const a = Math.random() * Math.PI * 2;
      const speed = rand(160, 520);
      spawnShard(
        player.x + Math.cos(a) * rand(40, 180),
        player.y + Math.sin(a) * rand(40, 180),
        killedTier === "god" ? 5 : 3,
        Math.cos(a) * speed,
        Math.sin(a) * speed,
        true
      );
    }
  }

  if (killer && killer.id !== player.id && killer.alive) {
    if (isKillerBot) analytics.botKills += 1;
    else analytics.totalKills += 1;

    if (!isKillerBot && isVictimBot) analytics.playerKilledBot += 1;
    else if (isKillerBot && !isVictimBot) analytics.botKilledPlayer += 1;
    else if (!isKillerBot && !isVictimBot) analytics.playerKilledPlayer += 1;
    else analytics.botKilledBot += 1;

    addRecentEvent("kill", {
      killerId: killer.id,
      victimId: player.id,
      victimBountyRank: player.bountyRank || 0,
      isVictimBot,
      isKillerBot
    });
    killer.score += 25 + player.bountyRank * 50;
    repairDrones(killer, 10);
    addEnergy(killer, 8 + player.bountyRank * 6);
    scheduleStatsFlushSoon();
  }
}

function respawn(player, name, skinId = player.skin) {
  const fresh = makePlayer(player.id, name || player.name, player.isBot);
  applySkin(fresh, skinId || player.skin);
  players.set(player.id, fresh);
  if (player.isBot) bots.set(player.id, fresh);
}

function pulse(player, nowSeconds) {
  if (levelFor(player.drones) < 3) return;
  const cooldown = player.drones >= MEGA_TITAN_DRONES ? 8 : player.drones >= TITAN_DRONES ? 6.5 : 5;
  if (nowSeconds - player.lastPulse < cooldown) return;
  player.lastPulse = nowSeconds;
  player.pulseUntil = nowSeconds + 0.32;

  const radius = 240;
  const now = Date.now();
  for (const other of players.values()) {
    if (!other.alive || other.id === player.id) continue;
    const dx = other.x - player.x;
    const dy = other.y - player.y;
    const d = Math.hypot(dx, dy) || 1;
    if (d < radius) {
      const force = (1 - d / radius) * 850;
      other.vx += (dx / d) * force;
      other.vy += (dy / d) * force;
      stripDrones(other, d < 150 ? 2 : 1, { drop: true });
    }

    if (other.drones >= TITAN_DRONES && player.drones < other.drones) {
      for (const spot of weakSpotPositions(other, now)) {
        if (spot.broken) continue;
        const sx = spot.x - player.x;
        const sy = spot.y - player.y;
        if (sx * sx + sy * sy < (radius + spot.r) ** 2) {
          damageWeakSpot(player, other, spot.index, now, player.drones < NEEDLE_DASH_MAX_DRONES ? 2 : 1);
        }
      }
    }
  }

  for (const shard of shards.values()) {
    const dx = shard.x - player.x;
    const dy = shard.y - player.y;
    const d = Math.hypot(dx, dy) || 1;
    if (d < radius + 120) {
      const force = (1 - d / (radius + 120)) * 720;
      shard.vx += (dx / d) * force;
      shard.vy += (dy / d) * force;
    }
  }
}

function updateBot(bot, nowSeconds) {
  if (nowSeconds < bot.botThinkAt) return;
  bot.botThinkAt = nowSeconds + rand(0.25, 0.7);

  let bestShard = null;
  let bestShardDist = Infinity;
  for (const shard of shards.values()) {
    const d = distSq(bot, shard);
    if (d < bestShardDist) {
      bestShardDist = d;
      bestShard = shard;
    }
  }

  let threat = null;
  let threatDist = Infinity;
  let prey = null;
  let preyDist = Infinity;
  for (const other of players.values()) {
    if (!other.alive || other.id === bot.id) continue;
    const d = distSq(bot, other);
    if (other.drones > bot.drones + 8 && d < threatDist) {
      threat = other;
      threatDist = d;
    }
    if (bot.drones > other.drones + 4 && d < preyDist) {
      prey = other;
      preyDist = d;
    }
  }

  if (threat && threatDist < 520 * 520) {
    bot.targetX = bot.x + (bot.x - threat.x);
    bot.targetY = bot.y + (bot.y - threat.y);
    bot.boosting = bot.drones > 2 && threatDist < 340 * 340;
    if (levelFor(bot.drones) >= 3 && threatDist < 220 * 220) pulse(bot, nowSeconds);
    return;
  }

  if (prey && preyDist < 680 * 680) {
    bot.targetX = prey.x;
    bot.targetY = prey.y;
    bot.boosting = bot.drones > 3 && preyDist < 460 * 460;
    return;
  }

  if (bestShard && bestShardDist < 1200 * 1200) {
    bot.targetX = bestShard.x;
    bot.targetY = bestShard.y;
  } else if (Math.hypot(bot.x - bot.botTarget.x, bot.y - bot.botTarget.y) < 120) {
    bot.botTarget = randomPoint(0, Math.min(worldRadius - 180, 3400));
    bot.targetX = bot.botTarget.x;
    bot.targetY = bot.botTarget.y;
  }
  bot.boosting = false;
}

function applyMovement(player, now) {
  if (player.boosting && !player.wasBoosting) {
    player.boostStartedAt = now;
  }
  if (!player.boosting && player.wasBoosting) {
    const charged = now - player.boostStartedAt;
    if (
      player.drones > 0 &&
      player.drones < NEEDLE_DASH_MAX_DRONES &&
      charged >= NEEDLE_DASH_CHARGE_MS &&
      now >= player.needleCooldownUntil
    ) {
      player.needleDashUntil = now + NEEDLE_DASH_DURATION_MS;
      player.needleCooldownUntil = now + NEEDLE_DASH_COOLDOWN_MS;
      player.needleVulnerableUntil = now + 950;
      player.needleHit = false;
      player.vx += Math.cos(player.angle) * 1450;
      player.vy += Math.sin(player.angle) * 1450;
    }
  }
  player.wasBoosting = player.boosting;

  const dx = player.targetX - player.x;
  const dy = player.targetY - player.y;
  const desiredAngle = Math.atan2(dy, dx);
  const distance = Math.hypot(dx, dy);
  const level = levelFor(player.drones);
  const titanHandling = player.drones >= MEGA_TITAN_DRONES ? 0.74 : player.drones >= TITAN_DRONES ? 0.86 : 1;
  const baseSpeed = clamp(305 - Math.sqrt(player.drones) * 7.5, 185, 305) * titanHandling;
  const boostMult = player.boosting && player.drones > 0 ? 2.35 : 1;
  const dashing = now < player.needleDashUntil;
  const speed = distance < 16 ? 0 : baseSpeed * (dashing ? 2.9 : boostMult);

  if (speed > 0) {
    player.angle = desiredAngle;
    player.vx += Math.cos(desiredAngle) * speed * (player.boosting || dashing ? 9.5 : 7.5) * DT;
    player.vy += Math.sin(desiredAngle) * speed * (player.boosting || dashing ? 9.5 : 7.5) * DT;
  }

  if (player.boosting && player.drones > 0 && now - player.lastBoostBurn > 360) {
    player.lastBoostBurn = now;
    stripDrones(player, player.drones >= MEGA_TITAN_DRONES ? 4 : player.drones >= TITAN_DRONES ? 2 : 1);
    for (let i = 0; i < 2; i++) {
      const a = player.angle + Math.PI + rand(-0.42, 0.42);
      const side = player.angle + Math.PI / 2 + rand(-0.8, 0.8);
      const back = rand(38, 82);
      const offset = rand(-16, 16);
      spawnShard(
        player.x + Math.cos(a) * back + Math.cos(side) * offset,
        player.y + Math.sin(a) * back + Math.sin(side) * offset,
        1,
        player.vx * 0.18 + Math.cos(a) * rand(120, 260),
        player.vy * 0.18 + Math.sin(a) * rand(120, 260),
        false,
        player.id
      );
    }
  }

  const orbitSpeed = (level >= 6 ? 2.9 : 2.15) + Math.min(player.drones, 45) * 0.012;
  player.orbit += orbitSpeed * DT * (player.style === "blade" ? 1.35 : 1);

  player.vx *= 0.88;
  player.vy *= 0.88;
  player.x += player.vx * DT;
  player.y += player.vy * DT;

  const d = Math.hypot(player.x, player.y);
  const limit = worldRadius - CORE_RADIUS;
  if (d > limit) {
    const nx = player.x / d;
    const ny = player.y / d;
    player.x = nx * limit;
    player.y = ny * limit;
    player.vx -= nx * Math.abs(player.vx) * 1.6;
    player.vy -= ny * Math.abs(player.vy) * 1.6;
  }

  for (const asteroid of asteroids) {
    const ax = player.x - asteroid.x;
    const ay = player.y - asteroid.y;
    const ad = Math.hypot(ax, ay) || 1;
    const limitRadius = asteroid.r + CORE_RADIUS * 0.9;
    if (ad < limitRadius) {
      const nx = ax / ad;
      const ny = ay / ad;
      player.x = asteroid.x + nx * limitRadius;
      player.y = asteroid.y + ny * limitRadius;
      player.vx += nx * 110;
      player.vy += ny * 110;
    }
  }
}

function collectShards(player) {
  const level = levelFor(player.drones);
  const swarmRadius = player.drones > 1 ? droneDistance(player, player.drones - 1) : 0;
  const baseMagnet = level >= 3 ? 230 : level >= 2 ? 190 : 145;
  const magnet = Math.max(baseMagnet, swarmRadius * 0.58 + 90);
  const collectRadius = CORE_RADIUS + SHARD_RADIUS + (player.boosting ? 34 : 24);
  const scoopRadius = Math.max(collectRadius + Math.min(110, Math.sqrt(Math.max(1, player.drones)) * 3.2), swarmRadius * 0.08);
  const now = Date.now();
  for (const [id, shard] of shards) {
    if (shard.ownerId === player.id && now - shard.born < 1000) continue;
    const dx = player.x - shard.x;
    const dy = player.y - shard.y;
    const d = Math.hypot(dx, dy) || 1;
    if (d < magnet) {
      const closeness = clamp((magnet - d) / magnet, 0, 1);
      const pull = (620 + closeness * 1900) * closeness * DT;
      shard.vx += (dx / d) * pull;
      shard.vy += (dy / d) * pull;
    }
    if (d < scoopRadius) {
      addEnergy(player, shard.value);
      shards.delete(id);
    }
  }
}

function updateShards() {
  for (const [id, shard] of shards) {
    shard.vx *= 0.965;
    shard.vy *= 0.965;
    shard.x += shard.vx * DT;
    shard.y += shard.vy * DT;
    const d = Math.hypot(shard.x, shard.y);
    if (d > worldRadius - 24) {
      const nx = shard.x / d;
      const ny = shard.y / d;
      shard.x = nx * (worldRadius - 24);
      shard.y = ny * (worldRadius - 24);
      shard.vx *= -0.45;
      shard.vy *= -0.45;
    }
  }
}

function resolveCombat(now) {
  const alive = [...players.values()].filter((p) => p.alive);
  for (const p of alive) syncWeakSpots(p, now);
  const droneCache = new Map();
  for (const p of alive) droneCache.set(p.id, getDronePositions(p, COMBAT_DRONE_SAMPLE));

  const losses = new Map();
  for (const p of alive) losses.set(p.id, 0);

  for (let a = 0; a < alive.length; a++) {
    for (let b = a + 1; b < alive.length; b++) {
      const p1 = alive[a];
      const p2 = alive[b];
      if (Math.hypot(p1.x - p2.x, p1.y - p2.y) > 520 + (p1.drones + p2.drones) * 7) continue;

      const p1Drones = droneCache.get(p1.id);
      const p2Drones = droneCache.get(p2.id);
      const p1Dashing = now < p1.needleDashUntil && p1.drones < NEEDLE_DASH_MAX_DRONES;
      const p2Dashing = now < p2.needleDashUntil && p2.drones < NEEDLE_DASH_MAX_DRONES;

      for (let i = 0; i < p1Drones.length; i++) {
        const d1 = p1Drones[i];
        if ((losses.get(p1.id) || 0) > i) continue;
        for (let j = 0; j < p2Drones.length; j++) {
          const d2 = p2Drones[j];
          const dx = d1.x - d2.x;
          const dy = d1.y - d2.y;
          if (dx * dx + dy * dy < (DRONE_RADIUS * 2) ** 2) {
            if (!p1Dashing) losses.set(p1.id, (losses.get(p1.id) || 0) + 1);
            if (!p2Dashing) losses.set(p2.id, (losses.get(p2.id) || 0) + 1);
            break;
          }
        }
      }

      checkWeakSpotContact(p1, p2, now);
      checkWeakSpotContact(p2, p1, now);
      checkCoreHit(p1, p2, p1Drones, now, losses);
      checkCoreHit(p2, p1, p2Drones, now, losses);
    }
  }

  for (const p of alive) {
    const lost = losses.get(p.id) || 0;
    if (lost > 0) {
      const vulnerable = p.needleVulnerableUntil && now < p.needleVulnerableUntil && !p.needleHit;
      const cap = vulnerable ? Math.max(2, Math.ceil(p.drones * 0.08)) : p.drones <= 8 ? 1 : Math.max(2, Math.ceil(p.drones * 0.025));
      stripDrones(p, Math.min(lost, cap), { drop: true });
    }
  }
}

function checkWeakSpotContact(attacker, titan, now) {
  if (!attacker.alive || !titan.alive || titan.drones < TITAN_DRONES) return;
  if (attacker.id === titan.id || attacker.drones >= titan.drones) return;
  const dashing = now < attacker.needleDashUntil;
  if (!dashing && !attacker.boosting) return;

  const hitRadius = dashing ? 34 : 24;
  for (const spot of weakSpotPositions(titan, now)) {
    if (spot.broken) continue;
    const dx = attacker.x - spot.x;
    const dy = attacker.y - spot.y;
    if (dx * dx + dy * dy < (spot.r + hitRadius) ** 2) {
      damageWeakSpot(attacker, titan, spot.index, now, dashing ? 2 : 1);
      if (dashing) {
        attacker.needleVulnerableUntil = 0;
      }
      return;
    }
  }
}

function checkCoreHit(attacker, victim, attackerDrones, now, losses) {
  if (!attacker.alive || !victim.alive) return;
  if (victim.spawnGraceUntil && now < victim.spawnGraceUntil) return;
  const key = victim.id;
  const last = attacker.hitCooldown.get(key) || 0;
  if (now - last < DRONE_HIT_COOLDOWN) return;

  for (const drone of attackerDrones) {
    const dx = drone.x - victim.x;
    const dy = drone.y - victim.y;
    if (dx * dx + dy * dy < (CORE_RADIUS + DRONE_RADIUS) ** 2) {
      attacker.hitCooldown.set(key, now);
      losses.set(attacker.id, (losses.get(attacker.id) || 0) + 1);
      if (victim.drones <= 0 || (victim.exposedUntil && now < victim.exposedUntil)) {
        killPlayer(victim, attacker);
      } else {
        stripDrones(victim, victim.drones <= 5 ? 1 : 2, { drop: true });
        victim.vx += Math.sign(victim.x - attacker.x || 1) * 160;
        victim.vy += Math.sign(victim.y - attacker.y || 1) * 160;
      }
      return;
    }
  }
}

function stormState(nowSeconds) {
  const cycleLength = STORM_INTERVAL + STORM_DURATION;
  const cycle = nowSeconds % cycleLength;
  const active = cycle >= STORM_INTERVAL;
  return {
    active,
    nextIn: active ? 0 : Math.ceil(STORM_INTERVAL - cycle),
    endsIn: active ? Math.ceil(cycleLength - cycle) : 0,
    progress: active ? (cycle - STORM_INTERVAL) / STORM_DURATION : 0
  };
}

function updateStorm(nowSeconds) {
  const storm = stormState(nowSeconds);
  if (!storm.active) return;

  if (Math.random() < 0.16) {
    const p = randomPoint(80, 540);
    spawnShard(p.x, p.y, Math.random() < 0.35 ? 3 : 2, rand(-40, 40), rand(-40, 40), true);
  }

  const waveRadius = 120 + storm.progress * 950;
  for (const player of players.values()) {
    if (!player.alive) continue;
    const d = Math.hypot(player.x, player.y) || 1;
    if (Math.abs(d - waveRadius) < 42) {
      const force = 620 * (1 - Math.abs(d - waveRadius) / 42);
      player.vx += (player.x / d) * force;
      player.vy += (player.y / d) * force;
    }
  }
}

function updateWorldRadius() {
  let biggest = 1;
  for (const player of players.values()) {
    if (player.alive) biggest = Math.max(biggest, player.drones);
  }
  const desired = clamp(BASE_WORLD_RADIUS + Math.sqrt(biggest) * 130, BASE_WORLD_RADIUS, MAX_WORLD_RADIUS);
  worldRadius += (desired - worldRadius) * 0.015;
}

function assignBounties() {
  const ranked = [...players.values()]
    .filter((p) => p.alive)
    .sort((a, b) => b.score - a.score);
  for (const p of players.values()) p.bountyRank = 0;
  ranked.slice(0, 3).forEach((p, i) => {
    if (p.score > 80) p.bountyRank = i + 1;
  });
}

function tick() {
  tickCount += 1;
  const now = Date.now();
  const nowSeconds = (now - startTime) / 1000;

  for (const bot of bots.values()) {
    if (!bot.alive && now > bot.respawnAt) respawn(bot, bot.name);
    if (bot.alive) updateBot(bot, nowSeconds);
  }

  for (const player of players.values()) {
    if (!player.alive) continue;
    applyMovement(player, now);
    collectShards(player);
  }

  resolveCombat(now);
  updateShards();
  updateStorm(nowSeconds);
  fillShards();
  if (tickCount % Math.max(1, Math.round(TICK_RATE / BROADCAST_RATE)) === 0) {
    updateWorldRadius();
    assignBounties();
    broadcastState(nowSeconds);
  }
}

function serializePlayerForClient(player) {
  const level = levelFor(player.drones);
  const now = Date.now();
  const tier = titanTier(player);
  const pulseCooldownSeconds = player.drones >= MEGA_TITAN_DRONES ? 8 : player.drones >= TITAN_DRONES ? 6.5 : 5;
  const serverSeconds = (now - startTime) / 1000;
  return {
    id: player.id,
    name: player.name,
    x: Math.round(player.x),
    y: Math.round(player.y),
    angle: player.angle,
    orbit: player.orbit,
    color: player.color,
    palette: player.palette || SKINS.cyan.palette,
    alive: player.alive,
    drones: player.drones,
    maxDrones: player.maxDrones,
    style: player.style,
    styleChosen: player.styleChosen,
    level,
    boosting: player.boosting,
    pulse: Math.max(0, player.pulseUntil - serverSeconds),
    pulseCooldown: clamp(pulseCooldownSeconds - (serverSeconds - player.lastPulse), 0, pulseCooldownSeconds),
    bountyRank: player.bountyRank,
    titanTier: tier,
    exposed: player.exposedUntil > now,
    needleReady: player.drones < NEEDLE_DASH_MAX_DRONES && now >= player.needleCooldownUntil,
    needleCooldown: Math.max(0, player.needleCooldownUntil - now),
    weakSpots: tier === "normal" ? [] : weakSpotPositions(player, now).map((spot) => ({
      id: spot.id,
      x: Math.round(spot.x),
      y: Math.round(spot.y),
      hp: spot.hp,
      broken: spot.broken,
      r: Math.round(spot.r)
    }))
  };
}

function broadcastState(nowSeconds) {
  const playersSnapshot = [...players.values()];
  const playerPayload = playersSnapshot.map(serializePlayerForClient);
  const leaderboard = playersSnapshot
    .filter((player) => player.alive)
    .sort((a, b) => b.score - a.score)
    .slice(0, 10)
    .map((player, index) => ({
      rank: index + 1,
      id: player.id,
      name: player.name,
      score: Math.floor(player.score),
      drones: player.drones,
      maxDrones: player.maxDrones,
      bountyRank: player.bountyRank
    }));

  const basePayload = {
    worldRadius: Math.round(worldRadius),
    players: playerPayload,
    leaderboard,
    storm: stormState(nowSeconds)
  };

  const connectedSockets = getConnectedSocketCount();
  let broadcastBytes = 0;

  for (const socket of io.sockets.sockets.values()) {
    const viewer = players.get(socket.id);
    const center = viewer && viewer.alive ? viewer : { x: 0, y: 0 };
    const shardPayload = [];
    for (const s of shards.values()) {
      const dx = s.x - center.x;
      const dy = s.y - center.y;
      if (dx * dx + dy * dy > SHARD_VIEW_RADIUS * SHARD_VIEW_RADIUS && !s.rare) continue;
      shardPayload.push({
        x: Math.round(s.x),
        y: Math.round(s.y),
        value: s.value,
        rare: s.rare,
        kind: s.kind
      });
      if (shardPayload.length >= MAX_SHARDS_PER_CLIENT) break;
    }
    const payload = { ...basePayload, shards: shardPayload };
    const payloadJson = JSON.stringify(payload);
    const payloadBytes = Buffer.byteLength(payloadJson);
    const estimatedBytes = payloadBytes * SOCKET_OVERHEAD_MULTIPLIER;

    recordStatePayloadSample(payloadBytes);
    broadcastBytes += estimatedBytes;
    socket.volatile.emit("state", payload);
  }

  if (connectedSockets > 0) {
    analytics.totalStateBroadcasts += 1;
    analytics.totalEstimatedStateBytesSent += broadcastBytes;
    analytics.totalEstimatedBytesSent += broadcastBytes;
    refreshBandwidthEstimates();
    markStatsDirty();
  }
}

io.on("connection", (socket) => {
  analytics.totalSocketConnections += 1;
  socketSessions.set(socket.id, {
    connectedAt: Date.now(),
    startedAt: null
  });
  addRecentEvent("socket_connected", { socketId: socket.id });
  scheduleStatsFlushSoon();

  const realPlayers = [...players.values()].filter((player) => !player.isBot).length;
  if (realPlayers >= MAX_REAL_PLAYERS) {
    socket.emit("serverFull", { maxPlayers: MAX_REAL_PLAYERS });
    socket.disconnect(true);
    return;
  }

  const player = makePlayer(socket.id, `Core-${socket.id.slice(0, 3)}`);
  player.alive = false;
  players.set(socket.id, player);

  socket.emit("init", {
    id: socket.id,
    worldRadius: Math.round(worldRadius),
    asteroids,
    serverStartedAt: startTime
  });

  socket.on("hello", (payload = {}) => {
    const existing = players.get(socket.id);
    if (!existing) return;

    analytics.totalGameStarts += 1;
    const session = socketSessions.get(socket.id);
    if (session && !session.startedAt) {
      session.startedAt = Date.now();
      analytics.sessionsStarted += 1;
      recalculateActivePlayers();
    }
    addRecentEvent("game_start", {
      socketId: socket.id,
      playerName: safeName(payload.name || existing.name)
    });
    scheduleStatsFlushSoon();

    if (existing.alive) {
      existing.name = safeName(payload.name || existing.name);
      applySkin(existing, payload.skin || existing.skin);
    } else {
      respawn(existing, payload.name || existing.name, payload.skin || existing.skin);
    }
  });

  socket.on("input", (input = {}) => {
    const p = players.get(socket.id);
    if (!p || !p.alive) return;
    p.targetX = Number.isFinite(input.targetX) ? clamp(input.targetX, -worldRadius, worldRadius) : p.targetX;
    p.targetY = Number.isFinite(input.targetY) ? clamp(input.targetY, -worldRadius, worldRadius) : p.targetY;
    p.boosting = Boolean(input.boosting);
  });

  socket.on("pulse", () => {
    const p = players.get(socket.id);
    if (p && p.alive) pulse(p, (Date.now() - startTime) / 1000);
  });

  socket.on("style", (style) => {
    const p = players.get(socket.id);
    if (!p || !p.alive || p.styleChosen || levelFor(p.drones) < 4) return;
    if (!["ring", "blade", "cloud"].includes(style)) return;
    p.style = style;
    p.styleChosen = true;
  });

  socket.on("respawn", (payload = {}) => {
    const p = players.get(socket.id);
    if (p && !p.alive) respawn(p, payload.name, payload.skin || p.skin);
  });

  socket.on("disconnect", () => {
    analytics.totalDisconnects += 1;
    const session = socketSessions.get(socket.id);
    if (session && session.startedAt) {
      const durationSeconds = Math.max(0, (Date.now() - session.startedAt) / 1000);
      analytics.completedSessions += 1;
      analytics.totalSessionSeconds += durationSeconds;
      recalculateAverageSessionSeconds();
    }
    socketSessions.delete(socket.id);
    recalculateActivePlayers();
    addRecentEvent("socket_disconnected", {
      socketId: socket.id,
      hadStartedSession: Boolean(session && session.startedAt)
    });
    scheduleStatsFlushSoon();

    players.delete(socket.id);
  });
});

function createBots() {
  for (let i = 0; i < BOT_COUNT; i++) {
    const id = `bot-${botId++}`;
    const bot = makePlayer(id, botNames[i % botNames.length], true);
    applySkin(bot, Object.keys(SKINS)[i % Object.keys(SKINS).length]);
    bot.energy = rand(0, 28);
    bot.maxDrones = clamp(1 + Math.floor(bot.energy / 10), 1, 5);
    bot.drones = bot.maxDrones;
    bot.score = Math.floor(bot.energy);
    players.set(id, bot);
    bots.set(id, bot);
  }
}

fillShards();
createBots();
refreshBandwidthEstimates();
setInterval(tick, 1000 / TICK_RATE);
setInterval(logBandwidthEstimate, BANDWIDTH_LOG_INTERVAL_MS);

server.listen(PORT, () => {
  console.log(`SwarmCore.io running on port ${PORT}`);
});
