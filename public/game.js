const socket = io();
const canvas = document.getElementById("game");
const ctx = canvas.getContext("2d");
const nativeArc = ctx.arc.bind(ctx);
ctx.arc = (x, y, radius, startAngle, endAngle, counterclockwise) => {
  nativeArc(x, y, Math.max(0, Number.isFinite(radius) ? radius : 0), startAngle, endAngle, counterclockwise);
};
const minimap = document.getElementById("minimap");
const miniCtx = minimap.getContext("2d");

const startPanel = document.getElementById("startPanel");
const deadPanel = document.getElementById("deadPanel");
const stylePanel = document.getElementById("stylePanel");
const playButton = document.getElementById("playButton");
const respawnButton = document.getElementById("respawnButton");
const nameInput = document.getElementById("nameInput");
const skinPicker = document.getElementById("skinPicker");
const leaderboardEl = document.getElementById("leaderboard");
const levelStat = document.getElementById("levelStat");
const droneStat = document.getElementById("droneStat");
const pulseStat = document.getElementById("pulseStat");
const stormText = document.getElementById("stormText");
const mobileBoost = document.getElementById("mobileBoost");
const mobilePulse = document.getElementById("mobilePulse");
const stickBase = document.getElementById("stickBase");
const stickKnob = document.getElementById("stickKnob");

let dpr = 1;
let myId = null;
let worldRadius = 2400;
let asteroids = [];
let state = { players: [], shards: [], leaderboard: [], storm: { active: false } };
const playerRenderCache = new Map();
let camera = { x: 0, y: 0, zoom: 1 };
let cameraLockedToPlayer = false;
let mouse = { x: innerWidth / 2, y: innerHeight / 2, down: false };
let target = { x: 0, y: 0 };
let playing = false;
let boostHeld = false;
let touchVector = null;
let activeStickTouchId = null;
let selectedSkin = "cyan";
let stars = Array.from({ length: 180 }, () => ({
  x: (Math.random() - 0.5) * 5400,
  y: (Math.random() - 0.5) * 5400,
  r: Math.random() * 1.7 + 0.3,
  a: Math.random() * 0.6 + 0.18
}));

function isMobilePerformanceMode() {
  return window.matchMedia("(pointer: coarse), (max-width: 760px)").matches;
}

function getRenderQuality() {
  const mobile = isMobilePerformanceMode();
  const alivePlayers = state.players.filter((player) => player.alive);
  const aliveCount = alivePlayers.length;
  const totalDrones = alivePlayers.reduce((sum, player) => sum + Math.max(0, player.drones || 0), 0);
  const bigSwarmCount = alivePlayers.filter((player) => (player.drones || 0) >= 500).length;

  if (mobile) {
    return {
      mobile: true,
      heavyScene: true,
      maxPlayersDrawn: 18,
      selfDotBudget: 160,
      otherDotBudget: 70,
      shardBudget: 140,
      swarmBands: 6,
      useGlow: false,
      drawStars: false,
      drawMinimap: false,
      drawPings: true
    };
  }

  const heavyScene = aliveCount >= 20 || totalDrones >= 7000 || bigSwarmCount >= 5;

  if (heavyScene) {
    return {
      mobile: false,
      heavyScene: true,
      maxPlayersDrawn: 28,
      selfDotBudget: 420,
      otherDotBudget: 90,
      shardBudget: 220,
      swarmBands: 8,
      useGlow: false,
      drawStars: false,
      drawMinimap: true,
      drawPings: true
    };
  }

  return {
    mobile: false,
    heavyScene: false,
    maxPlayersDrawn: 60,
    selfDotBudget: 700,
    otherDotBudget: 180,
    shardBudget: 360,
    swarmBands: 16,
    useGlow: true,
    drawStars: true,
    drawMinimap: true,
    drawPings: true
  };
}

function resize() {
  const mobile = isMobilePerformanceMode();

  dpr = mobile ? 1 : Math.min(devicePixelRatio || 1, 2);

  canvas.width = Math.floor(innerWidth * dpr);
  canvas.height = Math.floor(innerHeight * dpr);
  canvas.style.width = `${innerWidth}px`;
  canvas.style.height = `${innerHeight}px`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  if (!mobile) {
    minimap.width = Math.floor(minimap.clientWidth * dpr);
    minimap.height = Math.floor(minimap.clientHeight * dpr);
    miniCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
}

addEventListener("resize", resize);
resize();

socket.on("init", (payload) => {
  myId = payload.id;
  worldRadius = payload.worldRadius;
  asteroids = payload.asteroids || [];
});

socket.on("state", (payload) => {
  if (payload.worldRadius) worldRadius = payload.worldRadius;
  const receivedAt = performance.now();
  for (const next of payload.players) {
    const previous = playerRenderCache.get(next.id);
    const visual = previous ? renderPlayer(previous.data) : next;
    playerRenderCache.set(next.id, {
      data: next,
      from: visual,
      receivedAt
    });
  }
  for (const id of playerRenderCache.keys()) {
    if (!payload.players.some((player) => player.id === id)) {
      playerRenderCache.delete(id);
    }
  }
  state = payload;
  updateHud();
});

socket.on("serverFull", (payload = {}) => {
  const brand = startPanel.querySelector(".brand");
  if (brand) brand.textContent = "Arena full";
  nameInput.placeholder = `${payload.maxPlayers || 50} players already in`;
  nameInput.disabled = true;
  playButton.textContent = "Try again soon";
  playButton.disabled = true;
});

function me() {
  return state.players.find((p) => p.id === myId);
}

function worldToScreen(x, y) {
  return {
    x: (x - camera.x) * camera.zoom + innerWidth / 2,
    y: (y - camera.y) * camera.zoom + innerHeight / 2
  };
}

function isPlayerCoreNearViewport(player, quality) {
  if (!player) return false;
  if (player.id === myId) return true;

  const p = worldToScreen(player.x, player.y);

  const margin = quality.mobile
    ? 180
    : quality.heavyScene
      ? 260
      : 420;

  return !(
    p.x < -margin ||
    p.y < -margin ||
    p.x > innerWidth + margin ||
    p.y > innerHeight + margin
  );
}

function isPlayerWorthConsidering(player, self, quality) {
  if (!player || !player.alive) return false;
  if (player.id === myId) return true;

  if (isPlayerCoreNearViewport(player, quality)) return true;

  if (!self) return false;

  const dx = player.x - self.x;
  const dy = player.y - self.y;
  const distance = Math.hypot(dx, dy);

  const maxDistance = quality.mobile
    ? 2600
    : quality.heavyScene
      ? 3200
      : 4600;

  return distance < maxDistance && !quality.heavyScene;
}

function screenToWorld(x, y) {
  return {
    x: (x - innerWidth / 2) / camera.zoom + camera.x,
    y: (y - innerHeight / 2) / camera.zoom + camera.y
  };
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function lerp(a, b, t) {
  return a + (b - a) * t;
}

function lerpAngle(a, b, t) {
  let delta = ((b - a + Math.PI) % (Math.PI * 2)) - Math.PI;
  if (delta < -Math.PI) delta += Math.PI * 2;
  return a + delta * t;
}

function renderPlayer(player) {
  const cached = playerRenderCache.get(player.id);
  if (!cached) return player;
  const t = clamp((performance.now() - cached.receivedAt) / 85, 0, 1);
  const level = player.level || levelFor(player.drones);
  const orbitSpeed = (level >= 6 ? 2.9 : 2.15) + Math.min(player.drones, 45) * 0.012;
  const elapsed = Math.max(0, performance.now() - cached.receivedAt) / 1000;
  return {
    ...player,
    x: lerp(cached.from.x, player.x, t),
    y: lerp(cached.from.y, player.y, t),
    angle: lerpAngle(cached.from.angle || 0, player.angle || 0, t),
    orbit: lerpAngle(cached.from.orbit || 0, player.orbit || 0, t) + orbitSpeed * elapsed
  };
}

function levelFor(drones) {
  const progress = clamp((Math.max(1, drones) - 1) / 9999, 0, 1);
  return clamp(1 + Math.floor(Math.sqrt(progress) * 99), 1, 100);
}

function updateHud() {
  const player = me();
  if (!player) return;
  deadPanel.classList.toggle("hidden", player.alive || !playing);
  levelStat.textContent = player.level;
  droneStat.textContent = player.maxDrones && player.maxDrones > player.drones ? `${player.drones}/${player.maxDrones}` : player.drones;

  if (player.drones < 150 && player.needleReady) pulseStat.textContent = "Dash";
  else if (player.drones < 150 && player.needleCooldown > 0) pulseStat.textContent = `Dash ${(player.needleCooldown / 1000).toFixed(1)}s`;
  else if (player.level < 3) pulseStat.textContent = "Locked";
  else if (player.pulseCooldown > 0) pulseStat.textContent = `${player.pulseCooldown.toFixed(1)}s`;
  else pulseStat.textContent = "Ready";

  const shouldPickStyle = playing && player.alive && player.level >= 4 && !player.styleChosen;
  stylePanel.classList.toggle("hidden", !shouldPickStyle);

  leaderboardEl.innerHTML = "";
  const isMobileHud = window.matchMedia("(pointer: coarse), (max-width: 760px)").matches;
  const leaderboardRows = isMobileHud ? state.leaderboard.slice(0, 5) : state.leaderboard;

  for (const row of leaderboardRows) {
    const li = document.createElement("li");
    li.className = row.id === myId ? "you" : "";
    const bounty = row.bountyRank ? " *" : "";
    li.innerHTML = `<span>#${row.rank}</span><span class="name">${escapeHtml(row.name)}${bounty}</span><strong class="score">${row.score}</strong><span class="swarm">${row.drones}</span>`;
    leaderboardEl.appendChild(li);
  }

  if (state.storm.active) {
    stormText.textContent = `Reactor storm active: ${state.storm.endsIn}s`;
  } else {
    stormText.textContent = `Reactor storm in ${state.storm.nextIn}s`;
  }
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#039;"
  })[char]);
}

playButton.addEventListener("click", () => {
  playing = true;
  cameraLockedToPlayer = false;
  startPanel.classList.add("hidden");
  socket.emit("hello", { name: nameInput.value, skin: selectedSkin });
});

respawnButton.addEventListener("click", () => {
  cameraLockedToPlayer = false;
  socket.emit("respawn", { name: nameInput.value, skin: selectedSkin });
});

nameInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") playButton.click();
});

for (const button of stylePanel.querySelectorAll("button")) {
  button.addEventListener("click", () => {
    socket.emit("style", button.dataset.style);
    stylePanel.classList.add("hidden");
  });
}

for (const button of skinPicker.querySelectorAll(".skin")) {
  button.addEventListener("click", () => {
    selectedSkin = button.dataset.skin;
    for (const option of skinPicker.querySelectorAll(".skin")) option.classList.toggle("active", option === button);
  });
}

addEventListener("mousemove", (event) => {
  mouse.x = event.clientX;
  mouse.y = event.clientY;
});

addEventListener("mousedown", (event) => {
  if (event.button === 0) {
    mouse.down = true;
    boostHeld = true;
  }
});

addEventListener("mouseup", (event) => {
  if (event.button === 0) {
    mouse.down = false;
    boostHeld = false;
  }
});

addEventListener("keydown", (event) => {
  if (event.code === "Space") {
    event.preventDefault();
    socket.emit("pulse");
  }
});

canvas.addEventListener("contextmenu", (event) => event.preventDefault());

mobileBoost.addEventListener("touchstart", (event) => {
  event.preventDefault();
  event.stopPropagation();
  boostHeld = true;
}, { passive: false });
mobileBoost.addEventListener("touchend", (event) => {
  event.preventDefault();
  event.stopPropagation();
  boostHeld = false;
}, { passive: false });
mobileBoost.addEventListener("touchcancel", (event) => {
  event.preventDefault();
  event.stopPropagation();
  boostHeld = false;
}, { passive: false });
mobilePulse.addEventListener("touchstart", (event) => {
  event.preventDefault();
  event.stopPropagation();
  socket.emit("pulse");
}, { passive: false });

function resetStick() {
  activeStickTouchId = null;
  touchVector = null;
  stickKnob.style.transform = "translate(0, 0)";
}

function getTrackedTouch(event) {
  if (activeStickTouchId === null) {
    return event.changedTouches[0] || event.touches[0] || null;
  }

  for (const touch of event.touches) {
    if (touch.identifier === activeStickTouchId) return touch;
  }

  for (const touch of event.changedTouches) {
    if (touch.identifier === activeStickTouchId) return touch;
  }

  return null;
}

function updateStickFromTouch(touch) {
  if (!touch) return;

  const rect = stickBase.getBoundingClientRect();
  const cx = rect.left + rect.width / 2;
  const cy = rect.top + rect.height / 2;
  const dx = touch.clientX - cx;
  const dy = touch.clientY - cy;
  const d = Math.hypot(dx, dy) || 1;
  const max = 40;
  const nx = dx / d;
  const ny = dy / d;
  const amount = Math.min(d, max);

  stickKnob.style.transform = `translate(${nx * amount}px, ${ny * amount}px)`;
  touchVector = {
    x: nx * Math.min(d / max, 1),
    y: ny * Math.min(d / max, 1)
  };
}

function handleStickStart(event) {
  event.preventDefault();
  event.stopPropagation();

  const touch = event.changedTouches[0] || event.touches[0];
  if (!touch) return;

  activeStickTouchId = touch.identifier;
  updateStickFromTouch(touch);
}

function handleStickMove(event) {
  event.preventDefault();
  event.stopPropagation();

  const touch = getTrackedTouch(event);
  if (!touch) return;

  updateStickFromTouch(touch);
}

function handleStickEnd(event) {
  event.preventDefault();
  event.stopPropagation();

  if (activeStickTouchId === null) {
    resetStick();
    return;
  }

  for (const touch of event.changedTouches) {
    if (touch.identifier === activeStickTouchId) {
      resetStick();
      return;
    }
  }
}

stickBase.addEventListener("touchstart", handleStickStart, { passive: false });
stickBase.addEventListener("touchmove", handleStickMove, { passive: false });
stickBase.addEventListener("touchend", handleStickEnd, { passive: false });
stickBase.addEventListener("touchcancel", handleStickEnd, { passive: false });

addEventListener("blur", () => {
  resetStick();
  boostHeld = false;
});

document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    resetStick();
    boostHeld = false;
  }
});

document.addEventListener("touchmove", (event) => {
  if (playing && isMobilePerformanceMode()) {
    event.preventDefault();
  }
}, { passive: false });

setInterval(() => {
  const player = me();
  if (!player || !player.alive || !playing) return;

  const mobile = isMobilePerformanceMode();

  if (touchVector) {
    target.x = player.x + touchVector.x * 440;
    target.y = player.y + touchVector.y * 440;
  } else if (mobile) {
    target.x = player.x;
    target.y = player.y;
  } else {
    const world = screenToWorld(mouse.x, mouse.y);
    target.x = world.x;
    target.y = world.y;
  }

  socket.emit("input", {
    targetX: target.x,
    targetY: target.y,
    boosting: boostHeld
  });
}, 1000 / 30);

let lastMobileDraw = 0;

function draw() {
  requestAnimationFrame(draw);

  const now = performance.now();
  const mobile = isMobilePerformanceMode();

  if (mobile && now - lastMobileDraw < 33) {
    return;
  }

  if (mobile) {
    lastMobileDraw = now;
  }

  const quality = getRenderQuality();

  const player = me() ? renderPlayer(me()) : null;
  if (player) {
    const outerRadius = player.drones > 1 ? droneDistance(player, player.drones - 1) + 70 : 130;

    const targetScreenRadius = quality.mobile
      ? clamp(Math.min(innerWidth, innerHeight) * 0.19, 80, 130)
      : clamp(Math.min(innerWidth, innerHeight) * 0.28, 150, 260);

    const desiredZoom = quality.mobile
      ? clamp(targetScreenRadius / outerRadius, 0.04, 0.72)
      : clamp(targetScreenRadius / outerRadius, 0.055, 0.95);

    if (!cameraLockedToPlayer) {
      camera.x = player.x;
      camera.y = player.y;
      camera.zoom = desiredZoom;
      cameraLockedToPlayer = true;
    } else {
      const zoomEase = desiredZoom < camera.zoom ? 0.07 : 0.04;
      camera.zoom += (desiredZoom - camera.zoom) * zoomEase;
      camera.x += (player.x - camera.x) * 0.14;
      camera.y += (player.y - camera.y) * 0.14;
    }
  }

  ctx.clearRect(0, 0, innerWidth, innerHeight);
  drawBackground(quality);
  drawArena();
  drawShards(quality);
  drawPlayers(quality);
  drawPlayerPings(quality);
  if (quality.drawMinimap) {
    drawMinimap();
  }
  drawVignette();
}

function drawBackground(quality) {
  const gradient = ctx.createRadialGradient(innerWidth / 2, innerHeight / 2, 0, innerWidth / 2, innerHeight / 2, Math.max(innerWidth, innerHeight));
  gradient.addColorStop(0, "#101827");
  gradient.addColorStop(0.55, "#080b14");
  gradient.addColorStop(1, "#03050a");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, innerWidth, innerHeight);

  if (!quality.drawStars) {
    return;
  }

  ctx.save();
  ctx.globalCompositeOperation = "lighter";
  for (const star of stars) {
    const p = worldToScreen(star.x, star.y);
    if (p.x < -10 || p.y < -10 || p.x > innerWidth + 10 || p.y > innerHeight + 10) continue;
    ctx.globalAlpha = star.a;
    ctx.fillStyle = "#ffffff";
    ctx.beginPath();
    ctx.arc(p.x, p.y, star.r, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.restore();
}

function drawArena() {
  const center = worldToScreen(0, 0);
  const radius = worldRadius * camera.zoom;

  ctx.save();
  ctx.strokeStyle = "rgba(89, 243, 255, 0.22)";
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.arc(center.x, center.y, radius, 0, Math.PI * 2);
  ctx.stroke();

  for (let r = 400; r < worldRadius; r += 400) {
    ctx.strokeStyle = r % 800 === 0 ? "rgba(255, 255, 255, 0.055)" : "rgba(255, 255, 255, 0.035)";
    ctx.beginPath();
    ctx.arc(center.x, center.y, r * camera.zoom, 0, Math.PI * 2);
    ctx.stroke();
  }

  const reactorPulse = 1 + Math.sin(performance.now() / 420) * 0.08;
  const reactorGlow = ctx.createRadialGradient(center.x, center.y, 0, center.x, center.y, 180 * camera.zoom);
  reactorGlow.addColorStop(0, "rgba(255, 230, 109, 0.62)");
  reactorGlow.addColorStop(0.42, "rgba(255, 79, 216, 0.22)");
  reactorGlow.addColorStop(1, "rgba(255, 79, 216, 0)");
  ctx.fillStyle = reactorGlow;
  ctx.beginPath();
  ctx.arc(center.x, center.y, 220 * camera.zoom * reactorPulse, 0, Math.PI * 2);
  ctx.fill();

  ctx.strokeStyle = state.storm.active ? "rgba(255, 230, 109, 0.72)" : "rgba(255, 79, 216, 0.42)";
  ctx.lineWidth = 3;
  ctx.beginPath();
  ctx.arc(center.x, center.y, 88 * camera.zoom * reactorPulse, 0, Math.PI * 2);
  ctx.stroke();

  if (state.storm.active) {
    const wave = (120 + state.storm.progress * 950) * camera.zoom;
    ctx.strokeStyle = "rgba(255, 230, 109, 0.58)";
    ctx.lineWidth = 5;
    ctx.beginPath();
    ctx.arc(center.x, center.y, wave, 0, Math.PI * 2);
    ctx.stroke();
  }
  ctx.restore();
}

function drawAsteroids() {
  ctx.save();
  for (const asteroid of asteroids) {
    const p = worldToScreen(asteroid.x, asteroid.y);
    const r = asteroid.r * camera.zoom;
    if (p.x + r < 0 || p.y + r < 0 || p.x - r > innerWidth || p.y - r > innerHeight) continue;
    ctx.fillStyle = "rgba(142, 155, 178, 0.28)";
    ctx.strokeStyle = "rgba(216, 232, 255, 0.18)";
    ctx.lineWidth = 2;
    ctx.beginPath();
    const points = 10;
    for (let i = 0; i < points; i++) {
      const a = (i / points) * Math.PI * 2 + asteroid.seed * 6 + performance.now() / 9000 * asteroid.spin;
      const wobble = 0.78 + Math.sin(i * 12.989 + asteroid.seed * 20) * 0.18;
      const x = p.x + Math.cos(a) * r * wobble;
      const y = p.y + Math.sin(a) * r * wobble;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.closePath();
    ctx.fill();
    ctx.stroke();
  }
  ctx.restore();
}

function drawShards(quality) {
  ctx.save();
  ctx.globalCompositeOperation = quality.useGlow ? "lighter" : "source-over";
  let drawn = 0;

  for (const shard of state.shards) {
    if (drawn >= quality.shardBudget) break;
    const p = worldToScreen(shard.x, shard.y);
    if (p.x < -20 || p.y < -20 || p.x > innerWidth + 20 || p.y > innerHeight + 20) continue;
    const isDroneLoot = shard.kind === "drone";
    const lootScale = isDroneLoot ? Math.min(2.1, 1 + Math.log10(Math.max(1, shard.value / 10)) * 0.34) : 1;
    const r = (isDroneLoot ? 10 * lootScale : shard.rare ? 8 : 5) * camera.zoom;
    ctx.shadowBlur = quality.useGlow ? (isDroneLoot ? 22 : shard.rare ? 18 : 10) : 0;
    ctx.shadowColor = isDroneLoot ? "#ffb84d" : shard.rare ? "#ffe66d" : "#59f3ff";
    ctx.fillStyle = isDroneLoot ? "#ffb84d" : shard.rare ? "#ffe66d" : "#59f3ff";
    ctx.beginPath();
    ctx.arc(p.x, p.y, Math.max(2.4, r), 0, Math.PI * 2);
    ctx.fill();
    if (isDroneLoot && r > 4) {
      ctx.strokeStyle = "rgba(255, 255, 255, 0.72)";
      ctx.lineWidth = Math.max(1, 1.5 * camera.zoom);
      ctx.beginPath();
      ctx.arc(p.x, p.y, Math.max(3.4, r * 0.58), 0, Math.PI * 2);
      ctx.stroke();
    }

    drawn++;
  }
  ctx.restore();
}

function drawPlayers(quality) {
  const selfRaw = me();
  const self = selfRaw ? renderPlayer(selfRaw) : null;
  const alive = state.players.filter((player) => player.alive);

  if (!self) {
    const fallback = alive
      .filter((player) => isPlayerCoreNearViewport(player, quality))
      .slice(0, quality.maxPlayersDrawn);

    for (const player of fallback) {
      drawPlayer(renderPlayer(player), quality);
    }

    return;
  }

  const selfEntry = alive.find((player) => player.id === myId);

  const others = alive
    .filter((player) => player.id !== myId)
    .filter((player) => isPlayerCoreNearViewport(player, quality))
    .map((player) => {
      const dx = player.x - self.x;
      const dy = player.y - self.y;
      return {
        player,
        distance: Math.hypot(dx, dy)
      };
    })
    .sort((a, b) => {
      if (a.player.bountyRank && !b.player.bountyRank) return -1;
      if (!a.player.bountyRank && b.player.bountyRank) return 1;
      return a.distance - b.distance;
    })
    .slice(0, Math.max(0, quality.maxPlayersDrawn - 1))
    .map((entry) => entry.player);

  const ordered = selfEntry ? [...others, selfEntry] : others;

  for (const player of ordered) {
    drawPlayer(renderPlayer(player), quality);
  }
}

function drawPlayerPings(quality) {
  if (!quality.drawPings) return;

  const self = me() ? renderPlayer(me()) : null;
  if (!self || !self.alive) return;

  const edge = Math.min(innerWidth, innerHeight) * 0.46;
  const nearby = state.players
    .filter((player) => player.alive && player.id !== myId)
    .map((player) => {
      const rendered = renderPlayer(player);
      const dx = rendered.x - self.x;
      const dy = rendered.y - self.y;
      return { player: rendered, dx, dy, d: Math.hypot(dx, dy) || 1 };
    })
    .filter((entry) => entry.d < 3200)
    .sort((a, b) => a.d - b.d)
    .slice(0, 5);

  ctx.save();
  ctx.globalCompositeOperation = "lighter";
  for (const entry of nearby) {
    const screen = worldToScreen(entry.player.x, entry.player.y);
    if (screen.x > 18 && screen.x < innerWidth - 18 && screen.y > 18 && screen.y < innerHeight - 18) continue;

    const angle = Math.atan2(entry.dy, entry.dx);
    const x = innerWidth / 2 + Math.cos(angle) * edge;
    const y = innerHeight / 2 + Math.sin(angle) * edge;
    const size = entry.player.bountyRank ? 13 : 9;

    ctx.translate(x, y);
    ctx.rotate(angle);
    ctx.fillStyle = entry.player.bountyRank ? "#ffe66d" : entry.player.color;
    ctx.shadowBlur = 16;
    ctx.shadowColor = ctx.fillStyle;
    ctx.beginPath();
    ctx.moveTo(size, 0);
    ctx.lineTo(-size * 0.72, -size * 0.62);
    ctx.lineTo(-size * 0.45, 0);
    ctx.lineTo(-size * 0.72, size * 0.62);
    ctx.closePath();
    ctx.fill();
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  ctx.restore();
}

function playerPalette(player) {
  return Array.isArray(player.palette) && player.palette.length ? player.palette : [player.color || "#55f7ff"];
}

function droneColor(player, index) {
  const palette = playerPalette(player);
  if (palette.length === 1) return palette[0];
  return palette[Math.abs((index + Math.floor((player.orbit || 0) * 2)) % palette.length)];
}

function drawMinimap() {
  const width = minimap.clientWidth;
  const height = minimap.clientHeight;
  const cx = width / 2;
  const cy = height / 2;
  const radius = Math.min(width, height) / 2 - 9;
  const self = me();

  miniCtx.clearRect(0, 0, width, height);
  miniCtx.save();
  miniCtx.globalCompositeOperation = "source-over";

  const bg = miniCtx.createRadialGradient(cx, cy, 4, cx, cy, radius);
  bg.addColorStop(0, "rgba(89, 243, 255, 0.08)");
  bg.addColorStop(1, "rgba(5, 8, 18, 0.72)");
  miniCtx.fillStyle = bg;
  miniCtx.beginPath();
  miniCtx.arc(cx, cy, radius, 0, Math.PI * 2);
  miniCtx.fill();

  miniCtx.strokeStyle = "rgba(89, 243, 255, 0.34)";
  miniCtx.lineWidth = 1.5;
  miniCtx.beginPath();
  miniCtx.arc(cx, cy, radius, 0, Math.PI * 2);
  miniCtx.stroke();

  miniCtx.strokeStyle = "rgba(255, 255, 255, 0.06)";
  miniCtx.lineWidth = 1;
  for (let i = 1; i <= 2; i++) {
    miniCtx.beginPath();
    miniCtx.arc(cx, cy, radius * i / 3, 0, Math.PI * 2);
    miniCtx.stroke();
  }

  miniCtx.globalCompositeOperation = "lighter";
  for (const raw of state.players) {
    if (!raw.alive) continue;
    const player = renderPlayer(raw);
    const px = cx + clamp(player.x / worldRadius, -1, 1) * radius;
    const py = cy + clamp(player.y / worldRadius, -1, 1) * radius;
    const isMe = player.id === myId;
    const dot = clamp(2.2 + Math.sqrt(Math.max(1, player.drones)) * 0.18, 2.5, isMe ? 8 : 6.5);

    miniCtx.fillStyle = isMe ? "#ffffff" : player.titanTier && player.titanTier !== "normal" ? "#ffe66d" : player.bountyRank ? "#ffe66d" : player.color;
    miniCtx.shadowBlur = isMe ? 10 : 6;
    miniCtx.shadowColor = miniCtx.fillStyle;
    miniCtx.beginPath();
    miniCtx.arc(px, py, dot, 0, Math.PI * 2);
    miniCtx.fill();

    if (isMe && self) {
      miniCtx.strokeStyle = player.color;
      miniCtx.lineWidth = 2;
      miniCtx.beginPath();
      miniCtx.arc(px, py, dot + 3, 0, Math.PI * 2);
      miniCtx.stroke();
    }
    if (!isMe && player.titanTier && player.titanTier !== "normal") {
      miniCtx.strokeStyle = "rgba(255, 230, 109, 0.85)";
      miniCtx.lineWidth = 1.5;
      miniCtx.beginPath();
      miniCtx.arc(px, py, dot + 4, 0, Math.PI * 2);
      miniCtx.stroke();
    }
  }

  miniCtx.restore();
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
    return 42 + t * maxRadius + Math.sin((player.orbit || 0) * 1.7 + index) * 22;
  }
  const ringSize = 36;
  const rings = Math.max(1, Math.ceil(count / ringSize));
  const layer = Math.floor(index / ringSize);
  return 54 + ((layer + 0.75) / rings) * maxRadius;
}

function droneAngle(player, index) {
  const count = Math.max(1, player.drones);
  const orbit = player.orbit || 0;
  if (player.style === "blade") {
    const arms = 6;
    const arm = index % arms;
    const step = Math.floor(index / arms);
    return orbit + arm * Math.PI * 2 / arms + step * 0.028;
  }
  if (player.style === "cloud") {
    return orbit * (0.8 + (index % 5) * 0.03) + index * 2.399;
  }
  const ringSize = 36;
  const position = index % ringSize;
  const layer = Math.floor(index / ringSize);
  return orbit + (position / Math.min(ringSize, count - layer * ringSize)) * Math.PI * 2 + layer * 0.19;
}

function drawPlayer(player, quality) {
  const core = worldToScreen(player.x, player.y);
  const isMe = player.id === myId;
  const palette = playerPalette(player);
  const color = player.color || palette[0];

  if (!isMe && !isPlayerCoreNearViewport(player, quality)) {
    return;
  }

  ctx.save();
  ctx.globalCompositeOperation = "lighter";

  if (player.bountyRank) {
    ctx.strokeStyle = player.bountyRank === 1 ? "rgba(255, 80, 112, 0.78)" : "rgba(255, 230, 109, 0.62)";
    ctx.lineWidth = 4;
    ctx.setLineDash([12, 10]);
    ctx.beginPath();
    ctx.arc(core.x, core.y, (86 + Math.sqrt(player.drones) * 18) * camera.zoom, 0, Math.PI * 2);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  if (player.titanTier && player.titanTier !== "normal") {
    const outer = (player.drones > 1 ? droneDistance(player, player.drones - 1) + 42 : 120) * camera.zoom;
    ctx.strokeStyle = player.titanTier === "god" ? "rgba(255, 79, 216, 0.86)" : player.titanTier === "mega" ? "rgba(255, 230, 109, 0.7)" : "rgba(255, 129, 85, 0.62)";
    ctx.lineWidth = Math.max(2, 4 * camera.zoom);
    ctx.beginPath();
    ctx.arc(core.x, core.y, outer, 0, Math.PI * 2);
    ctx.stroke();
  }

  if (player.boosting) {
    ctx.strokeStyle = "rgba(255, 230, 109, 0.36)";
    ctx.lineWidth = 14 * camera.zoom;
    ctx.beginPath();
    ctx.arc(core.x, core.y, (46 + Math.sqrt(player.drones) * 9) * camera.zoom, 0, Math.PI * 2);
    ctx.stroke();
  }

  drawSwarmMass(player, core, color, isMe, quality);

  const maxDots = Math.min(player.drones, isMe ? quality.selfDotBudget : quality.otherDotBudget);

  const stride = Math.max(1, Math.floor(player.drones / Math.max(1, maxDots)));
  ctx.shadowBlur = quality.useGlow ? (player.drones > 420 ? 9 : 16) : 0;

  for (let i = 0; i < player.drones; i += stride) {
    const dotColor = droneColor(player, i);
    const angle = droneAngle(player, i);
    const distance = droneDistance(player, i);
    const p = worldToScreen(
      player.x + Math.cos(angle) * distance,
      player.y + Math.sin(angle) * distance
    );
    ctx.shadowBlur = quality.useGlow ? 16 : 0;
    ctx.shadowColor = dotColor;
    ctx.fillStyle = dotColor;
    ctx.beginPath();
    ctx.arc(p.x, p.y, Math.max(3.5, 8 * camera.zoom), 0, Math.PI * 2);
    ctx.fill();
  }

  drawWeakSpots(player, quality);

  if (player.pulse > 0) {
    const radius = Math.max(0, 240 * (1 - clamp(player.pulse / 0.32, 0, 1))) * camera.zoom;
    ctx.strokeStyle = "rgba(248, 251, 255, 0.62)";
    ctx.lineWidth = 5;
    ctx.beginPath();
    ctx.arc(core.x, core.y, radius, 0, Math.PI * 2);
    ctx.stroke();
  }

  const coreFlash = player.exposed ? 1 : (player.titanTier === "mega" || player.titanTier === "god") ? 0.35 + Math.max(0, Math.sin(performance.now() / 220)) * 0.28 : 0;
  const coreGradient = ctx.createRadialGradient(core.x, core.y, 0, core.x, core.y, (40 + coreFlash * 30) * camera.zoom);
  coreGradient.addColorStop(0, "#ffffff");
  coreGradient.addColorStop(0.36, color);
  coreGradient.addColorStop(1, "rgba(0, 0, 0, 0)");
  ctx.fillStyle = coreGradient;
  ctx.shadowBlur = quality.useGlow ? 24 : 0;
  ctx.shadowColor = color;
  ctx.beginPath();
  ctx.arc(core.x, core.y, (31 + coreFlash * 16) * camera.zoom, 0, Math.PI * 2);
  ctx.fill();

  ctx.shadowBlur = 0;
  ctx.fillStyle = "#ffffff";
  ctx.beginPath();
  ctx.arc(core.x, core.y, Math.max(6, 12 * camera.zoom), 0, Math.PI * 2);
  ctx.fill();

  ctx.globalCompositeOperation = "source-over";
  ctx.fillStyle = isMe ? "#59f3ff" : "rgba(248, 251, 255, 0.84)";
  ctx.font = "700 12px Inter, sans-serif";
  ctx.textAlign = "center";
  ctx.fillText(player.name, core.x, core.y - 38 * camera.zoom);

  ctx.restore();
}

function drawSwarmMass(player, core, color, isMe, quality) {
  if (player.drones <= 420) return;

  const maxDistance = Math.max(1, droneDistance(player, player.drones - 1) * camera.zoom);
  const palette = playerPalette(player);
  const alphaScale = quality.useGlow ? 1 : 0.55;
  ctx.save();
  ctx.globalCompositeOperation = quality.useGlow ? "lighter" : "source-over";
  ctx.shadowBlur = 0;

  if (player.style === "blade") {
    const arms = 6;
    ctx.lineWidth = Math.max(5, 10 * camera.zoom);
    for (let arm = 0; arm < arms; arm++) {
      const a = (player.orbit || 0) + arm * Math.PI * 2 / arms;
      ctx.strokeStyle = hexToRgba(palette[arm % palette.length], (isMe ? 0.34 : 0.23) * alphaScale);
      ctx.beginPath();
      ctx.moveTo(core.x + Math.cos(a) * 54 * camera.zoom, core.y + Math.sin(a) * 54 * camera.zoom);
      ctx.lineTo(core.x + Math.cos(a) * maxDistance, core.y + Math.sin(a) * maxDistance);
      ctx.stroke();
    }
    ctx.restore();
    return;
  }

  if (player.style === "cloud") {
    const glow = ctx.createRadialGradient(core.x, core.y, 20 * camera.zoom, core.x, core.y, maxDistance);
    glow.addColorStop(0, hexToRgba(palette[0], 0.2 * alphaScale));
    glow.addColorStop(0.45, hexToRgba(palette[1 % palette.length], (isMe ? 0.18 : 0.12) * alphaScale));
    glow.addColorStop(0.78, hexToRgba(palette[2 % palette.length], (isMe ? 0.11 : 0.08) * alphaScale));
    glow.addColorStop(1, hexToRgba(palette[0], 0));
    ctx.fillStyle = glow;
    ctx.beginPath();
    ctx.arc(core.x, core.y, maxDistance, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
    return;
  }

  const bands = quality.swarmBands;
  ctx.lineWidth = Math.max(1.2, 2.2 * camera.zoom);
  for (let i = 1; i <= bands; i++) {
    const r = (54 + (i / bands) * (58 + Math.sqrt(player.drones) * 31)) * camera.zoom;
    ctx.strokeStyle = hexToRgba(palette[i % palette.length], (0.08 + i / bands * (isMe ? 0.18 : 0.12)) * alphaScale);
    ctx.beginPath();
    ctx.arc(core.x, core.y, r, 0, Math.PI * 2);
    ctx.stroke();
  }
  ctx.restore();
}

function drawWeakSpots(player, quality) {
  if (!Array.isArray(player.weakSpots) || !player.weakSpots.length) return;

  ctx.save();
  ctx.globalCompositeOperation = "lighter";
  for (const spot of player.weakSpots) {
    const p = worldToScreen(spot.x, spot.y);
    const r = Math.max(8, spot.r * camera.zoom);
    if (p.x < -60 || p.y < -60 || p.x > innerWidth + 60 || p.y > innerHeight + 60) continue;

    if (spot.broken) {
      ctx.strokeStyle = "rgba(255, 255, 255, 0.24)";
      ctx.lineWidth = Math.max(2, 3 * camera.zoom);
      ctx.setLineDash([6, 7]);
      ctx.beginPath();
      ctx.arc(p.x, p.y, r * 1.25, 0, Math.PI * 2);
      ctx.stroke();
      ctx.setLineDash([]);
      continue;
    }

    const hpRatio = clamp(spot.hp / 3, 0, 1);
    const color = hpRatio > 0.66 ? "#ffe66d" : hpRatio > 0.33 ? "#ff8155" : "#ff4f70";
  ctx.shadowBlur = quality.useGlow ? 22 : 0;
    ctx.shadowColor = color;
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
    ctx.fill();

    ctx.strokeStyle = "rgba(255, 255, 255, 0.82)";
    ctx.lineWidth = Math.max(1.5, 2 * camera.zoom);
    ctx.beginPath();
    ctx.arc(p.x, p.y, r * 1.45, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * hpRatio);
    ctx.stroke();
  }
  ctx.restore();
}

function hexToRgba(hex, alpha) {
  const clean = hex.replace("#", "");
  const value = parseInt(clean.length === 3 ? clean.split("").map((c) => c + c).join("") : clean, 16);
  const r = (value >> 16) & 255;
  const g = (value >> 8) & 255;
  const b = value & 255;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function drawVignette() {
  const gradient = ctx.createRadialGradient(innerWidth / 2, innerHeight / 2, Math.min(innerWidth, innerHeight) * 0.25, innerWidth / 2, innerHeight / 2, Math.max(innerWidth, innerHeight) * 0.75);
  gradient.addColorStop(0, "rgba(0, 0, 0, 0)");
  gradient.addColorStop(1, "rgba(0, 0, 0, 0.42)");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, innerWidth, innerHeight);
}

draw();
