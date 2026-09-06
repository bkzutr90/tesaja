require('dotenv').config();
const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*').split(',');

// ---------------------------------------------------------------------------
// STATE
// ---------------------------------------------------------------------------
const state = {
  connected: false,
  connecting: false,
  username: null,
  roomInfo: null,
  lastError: null,

  stats: {
    likes: 0,
    followers: 0,
    coins: 0,
    shares: 0,
    comments: 0,
    viewers: 0,
  },

  // userId -> { userId, uniqueId, nickname, profilePicture, value }
  likeLeaderboard: {},
  giftLeaderboard: {},

  comments: [], // { userId, uniqueId, nickname, profilePicture, comment, ts }

  settings: {
    likeLeaderboardSize: 5,
    giftLeaderboardSize: 5,
  },

  goals: {
    like:     { enabled: true,  target: 10000, current: 0 },
    follower: { enabled: true,  target: 100,   current: 0 },
    coin:     { enabled: true,  target: 5000,  current: 0 },
    share:    { enabled: false, target: 100,   current: 0 },
  },

  subathon: {
    running: false,
    remainingSeconds: 3600,
    // seconds added per unit of each event type
    rules: {
      like:     0,     // seconds per like
      follower: 10,    // seconds per new follower
      coin:     1,     // seconds per coin (gift diamond)
      share:    30,    // seconds per share
    },
  },
};

let tiktokConnection = null;
let subathonInterval = null;

// ---------------------------------------------------------------------------
// HELPERS
// ---------------------------------------------------------------------------
function broadcast() {
  const payload = JSON.stringify({ type: 'state', data: state });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(payload);
  });
}

function upsertLeaderboard(board, user, incrementValue) {
  const id = user.userId || user.uniqueId;
  if (!board[id]) {
    board[id] = {
      userId: id,
      uniqueId: user.uniqueId,
      nickname: user.nickname || user.uniqueId,
      profilePicture: (user.profilePictureUrl) || (user.profilePicture && user.profilePicture.urls && user.profilePicture.urls[0]) || '',
      value: 0,
    };
  }
  board[id].value += incrementValue;
}

function topN(board, n) {
  return Object.values(board)
    .sort((a, b) => b.value - a.value)
    .slice(0, n);
}

function addComment(user, text) {
  state.comments.unshift({
    userId: user.userId || user.uniqueId,
    uniqueId: user.uniqueId,
    nickname: user.nickname || user.uniqueId,
    profilePicture: (user.profilePictureUrl) || (user.profilePicture && user.profilePicture.urls && user.profilePicture.urls[0]) || '',
    comment: text,
    ts: Date.now(),
  });
  if (state.comments.length > 100) state.comments.length = 100;
  state.stats.comments += 1;
}

function addSubathonSeconds(seconds) {
  if (!seconds) return;
  state.subathon.remainingSeconds += seconds;
}

function startSubathonTicker() {
  if (subathonInterval) clearInterval(subathonInterval);
  subathonInterval = setInterval(() => {
    if (!state.subathon.running) return;
    if (state.subathon.remainingSeconds > 0) {
      state.subathon.remainingSeconds -= 1;
      broadcast();
    } else {
      state.subathon.running = false;
      broadcast();
    }
  }, 1000);
}

// ---------------------------------------------------------------------------
// TIKTOK CONNECTOR
// ---------------------------------------------------------------------------
async function connectToTikTok(username) {
  if (tiktokConnection) {
    try { tiktokConnection.disconnect(); } catch (e) {}
    tiktokConnection = null;
  }

  state.username = username;
  state.connecting = true;
  state.lastError = null;
  broadcast();

  tiktokConnection = new WebcastPushConnection(username, {
    enableExtendedGiftInfo: true,
  });

  try {
    const roomInfo = await tiktokConnection.connect();
    state.connected = true;
    state.connecting = false;
    state.roomInfo = {
      roomId: roomInfo.roomId,
      title: roomInfo?.roomInfo?.title || null,
    };
    broadcast();
  } catch (err) {
    state.connected = false;
    state.connecting = false;
    state.lastError = String(err && err.message ? err.message : err);
    broadcast();
    return;
  }

  // ---- EVENTS ----
  tiktokConnection.on('chat', (data) => {
    addComment(data, data.comment);
    broadcast();
  });

  tiktokConnection.on('like', (data) => {
    const inc = data.likeCount || 1;
    state.stats.likes += inc;
    state.goals.like.current += inc;
    upsertLeaderboard(state.likeLeaderboard, data, inc);
    addSubathonSeconds(inc * state.subathon.rules.like);
    broadcast();
  });

  tiktokConnection.on('gift', (data) => {
    // Only count gifts once they are "finished" (streak ended) to avoid
    // double counting combo/streakable gifts.
    const isStreakable = data.giftType === 1;
    if (isStreakable && !data.repeatEnd) return;

    const diamonds = (data.diamondCount || 0) * (data.repeatCount || 1);
    state.stats.coins += diamonds;
    state.goals.coin.current += diamonds;
    upsertLeaderboard(state.giftLeaderboard, data, diamonds);
    addSubathonSeconds(diamonds * state.subathon.rules.coin);
    broadcast();
  });

  tiktokConnection.on('social', (data) => {
    if (data.displayType && data.displayType.includes('follow')) {
      state.stats.followers += 1;
      state.goals.follower.current += 1;
      addSubathonSeconds(state.subathon.rules.follower);
      broadcast();
    }
  });

  tiktokConnection.on('share', () => {
    state.stats.shares += 1;
    state.goals.share.current += 1;
    addSubathonSeconds(state.subathon.rules.share);
    broadcast();
  });

  tiktokConnection.on('roomUser', (data) => {
    if (typeof data.viewerCount === 'number') {
      state.stats.viewers = data.viewerCount;
      broadcast();
    }
  });

  tiktokConnection.on('streamEnd', () => {
    state.connected = false;
    broadcast();
  });

  tiktokConnection.on('disconnected', () => {
    state.connected = false;
    broadcast();
  });
}

function disconnectFromTikTok() {
  if (tiktokConnection) {
    try { tiktokConnection.disconnect(); } catch (e) {}
    tiktokConnection = null;
  }
  state.connected = false;
  state.connecting = false;
  broadcast();
}

// ---------------------------------------------------------------------------
// HTTP + WEBSOCKET SERVER
// ---------------------------------------------------------------------------
const app = express();
app.use(cors({ origin: ALLOWED_ORIGINS.includes('*') ? '*' : ALLOWED_ORIGINS }));
app.use(express.json());

// Full current state (used by the dashboard on first load and by Roblox Studio polling)
app.get('/api/state', (req, res) => {
  res.json({
    ...state,
    likeLeaderboard: topN(state.likeLeaderboard, state.settings.likeLeaderboardSize),
    giftLeaderboard: topN(state.giftLeaderboard, state.settings.giftLeaderboardSize),
  });
});

// Lightweight endpoint, ideal for Roblox HttpService polling every few seconds
app.get('/api/roblox', (req, res) => {
  res.json({
    connected: state.connected,
    stats: state.stats,
    goals: state.goals,
    subathon: state.subathon,
    topLikers: topN(state.likeLeaderboard, 5),
    topGifters: topN(state.giftLeaderboard, 5),
    latestComments: state.comments.slice(0, 5),
  });
});

app.post('/api/connect', async (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: 'username is required' });
  connectToTikTok(username.replace('@', ''));
  res.json({ ok: true });
});

app.post('/api/disconnect', (req, res) => {
  disconnectFromTikTok();
  res.json({ ok: true });
});

app.post('/api/settings', (req, res) => {
  const { likeLeaderboardSize, giftLeaderboardSize } = req.body;
  if (likeLeaderboardSize) state.settings.likeLeaderboardSize = Math.max(1, Math.min(50, likeLeaderboardSize));
  if (giftLeaderboardSize) state.settings.giftLeaderboardSize = Math.max(1, Math.min(50, giftLeaderboardSize));
  broadcast();
  res.json({ ok: true, settings: state.settings });
});

app.post('/api/goals', (req, res) => {
  // body: { like: { enabled, target }, follower: {...}, coin: {...}, share: {...}, reset: true/false }
  for (const key of ['like', 'follower', 'coin', 'share']) {
    if (req.body[key]) {
      if (typeof req.body[key].enabled === 'boolean') state.goals[key].enabled = req.body[key].enabled;
      if (typeof req.body[key].target === 'number') state.goals[key].target = req.body[key].target;
      if (req.body.reset) state.goals[key].current = 0;
    }
  }
  broadcast();
  res.json({ ok: true, goals: state.goals });
});

app.post('/api/subathon/config', (req, res) => {
  const { rules, remainingSeconds } = req.body;
  if (rules) Object.assign(state.subathon.rules, rules);
  if (typeof remainingSeconds === 'number') state.subathon.remainingSeconds = remainingSeconds;
  broadcast();
  res.json({ ok: true, subathon: state.subathon });
});

app.post('/api/subathon/start', (req, res) => {
  state.subathon.running = true;
  broadcast();
  res.json({ ok: true });
});

app.post('/api/subathon/pause', (req, res) => {
  state.subathon.running = false;
  broadcast();
  res.json({ ok: true });
});

app.post('/api/subathon/add', (req, res) => {
  const { seconds } = req.body;
  addSubathonSeconds(Number(seconds) || 0);
  broadcast();
  res.json({ ok: true, remainingSeconds: state.subathon.remainingSeconds });
});

app.post('/api/reset-leaderboards', (req, res) => {
  for (const k of Object.keys(state.likeLeaderboard)) delete state.likeLeaderboard[k];
  for (const k of Object.keys(state.giftLeaderboard)) delete state.giftLeaderboard[k];
  broadcast();
  res.json({ ok: true });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'state', data: state }));
});

startSubathonTicker();

server.listen(PORT, () => {
  console.log(`TikTok dashboard backend listening on port ${PORT}`);
  if (process.env.TIKTOK_USERNAME) {
    connectToTikTok(process.env.TIKTOK_USERNAME);
  }
});
