require('dotenv').config();
const path = require('path');
const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

const PORT = process.env.PORT || 8080;

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------
const state = {
  connected: false,
  username: null,
  stats: { likes: 0, followers: 0, shares: 0, diamonds: 0, viewers: 0 },
  likeLeaderboard: new Map(), // uniqueId -> { uniqueId, nickname, profilePicture, likes }
  giftLeaderboard: new Map(), // uniqueId -> { uniqueId, nickname, profilePicture, diamonds }
  goal: {
    type: 'likes', // likes | followers | coins | share
    label: 'Like Goal',
    target: 1000,
    current: 0,
  },
  subathon: {
    active: false,
    endsAt: null, // epoch ms
    rules: {
      secondsPerLike: 0, // e.g. 0.1 = 10 likes -> 1s
      secondsPerFollow: 10,
      secondsPerShare: 15,
      secondsPerDiamond: 1, // seconds added per diamond (gift value)
    },
  },
  comments: [], // { uniqueId, nickname, profilePicture, comment, ts }
};

let tiktokConnection = null;

function broadcast(type, payload) {
  const msg = JSON.stringify({ type, payload });
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(msg);
  });
}

function publicState() {
  return {
    connected: state.connected,
    username: state.username,
    stats: state.stats,
    goal: state.goal,
    subathon: state.subathon,
    likeLeaderboard: [...state.likeLeaderboard.values()].sort((a, b) => b.likes - a.likes),
    giftLeaderboard: [...state.giftLeaderboard.values()].sort((a, b) => b.diamonds - a.diamonds),
    comments: state.comments.slice(-50),
  };
}

function pushGoalProgress(type, amount) {
  if (state.goal.type === type) {
    state.goal.current += amount;
    broadcast('goal', state.goal);
  }
}

function addSubathonSeconds(seconds) {
  if (!state.subathon.active || !seconds) return;
  state.subathon.endsAt += seconds * 1000;
  broadcast('subathon', state.subathon);
}

// ---------------------------------------------------------------------------
// TikTok connection
// ---------------------------------------------------------------------------
async function connectTikTok(username) {
  if (tiktokConnection) {
    try { tiktokConnection.disconnect(); } catch (_) {}
  }

  tiktokConnection = new WebcastPushConnection(username);

  tiktokConnection.on('chat', (data) => {
    const entry = {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      profilePicture: data.profilePictureUrl,
      comment: data.comment,
      // 0 = belum follow, 1 = following, 2 = friends (saling follow)
      followRole: typeof data.followRole === 'number' ? data.followRole : 0,
      ts: Date.now(),
    };
    state.comments.push(entry);
    if (state.comments.length > 100) state.comments.shift();
    broadcast('comment', entry);
  });

  tiktokConnection.on('like', (data) => {
    const inc = data.likeCount || 1;
    state.stats.likes += inc;

    const key = data.uniqueId;
    const existing = state.likeLeaderboard.get(key) || {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      profilePicture: data.profilePictureUrl,
      likes: 0,
    };
    existing.likes += inc;
    state.likeLeaderboard.set(key, existing);

    pushGoalProgress('likes', inc);
    addSubathonSeconds(inc * state.subathon.rules.secondsPerLike);
    broadcast('likeLeaderboard', publicState().likeLeaderboard);
    broadcast('stats', state.stats);
  });

  tiktokConnection.on('gift', (data) => {
    // Streakable gifts fire repeatedly while the streak continues; only
    // count the final tally once the streak ends (or if it's not streakable).
    const isStreakable = data.giftType === 1;
    if (isStreakable && !data.repeatEnd) return;

    const diamonds = (data.diamondCount || 0) * (data.repeatCount || 1);
    state.stats.diamonds += diamonds;

    const key = data.uniqueId;
    const existing = state.giftLeaderboard.get(key) || {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      profilePicture: data.profilePictureUrl,
      diamonds: 0,
    };
    existing.diamonds += diamonds;
    state.giftLeaderboard.set(key, existing);

    pushGoalProgress('coins', diamonds);
    addSubathonSeconds(diamonds * state.subathon.rules.secondsPerDiamond);
    broadcast('giftLeaderboard', publicState().giftLeaderboard);
    broadcast('stats', state.stats);
  });

  tiktokConnection.on('follow', () => {
    state.stats.followers += 1;
    pushGoalProgress('followers', 1);
    addSubathonSeconds(state.subathon.rules.secondsPerFollow);
    broadcast('stats', state.stats);
  });

  tiktokConnection.on('share', () => {
    state.stats.shares += 1;
    pushGoalProgress('share', 1);
    addSubathonSeconds(state.subathon.rules.secondsPerShare);
    broadcast('stats', state.stats);
  });

  tiktokConnection.on('roomUser', (data) => {
    state.stats.viewers = data.viewerCount || state.stats.viewers;
    broadcast('stats', state.stats);
  });

  tiktokConnection.on('streamEnd', () => {
    state.connected = false;
    broadcast('connection', { connected: false, username: state.username });
  });

  tiktokConnection.on('disconnected', () => {
    state.connected = false;
    broadcast('connection', { connected: false, username: state.username });
  });

  await tiktokConnection.connect();
  state.connected = true;
  state.username = username;
  broadcast('connection', { connected: true, username });
}

// ---------------------------------------------------------------------------
// REST API
// ---------------------------------------------------------------------------
app.get('/api/state', (req, res) => res.json(publicState()));

app.post('/api/connect', async (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: 'username is required' });
  try {
    await connectTikTok(username.replace('@', '').trim());
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/disconnect', (req, res) => {
  if (tiktokConnection) tiktokConnection.disconnect();
  state.connected = false;
  broadcast('connection', { connected: false, username: state.username });
  res.json({ ok: true });
});

app.post('/api/goal', (req, res) => {
  const { type, target, label, resetCurrent } = req.body;
  if (type) state.goal.type = type;
  if (typeof target === 'number') state.goal.target = target;
  if (label) state.goal.label = label;
  if (resetCurrent) state.goal.current = 0;
  broadcast('goal', state.goal);
  res.json(state.goal);
});

app.post('/api/goal/reset', (req, res) => {
  state.goal.current = 0;
  broadcast('goal', state.goal);
  res.json(state.goal);
});

app.post('/api/subathon/start', (req, res) => {
  const { initialSeconds, rules } = req.body;
  state.subathon.active = true;
  state.subathon.endsAt = Date.now() + (initialSeconds || 3600) * 1000;
  if (rules) Object.assign(state.subathon.rules, rules);
  broadcast('subathon', state.subathon);
  res.json(state.subathon);
});

app.post('/api/subathon/addtime', (req, res) => {
  const { seconds } = req.body;
  addSubathonSeconds(seconds || 0);
  res.json(state.subathon);
});

app.post('/api/subathon/stop', (req, res) => {
  state.subathon.active = false;
  broadcast('subathon', state.subathon);
  res.json(state.subathon);
});

app.post('/api/leaderboards/reset', (req, res) => {
  state.likeLeaderboard.clear();
  state.giftLeaderboard.clear();
  broadcast('likeLeaderboard', []);
  broadcast('giftLeaderboard', []);
  res.json({ ok: true });
});

// Simple polling endpoint tailored for Roblox Studio (HttpService has no
// native WebSocket support), returns a lean payload.
app.get('/api/roblox/state', (req, res) => {
  const p = publicState();
  res.json({
    connected: p.connected,
    stats: p.stats,
    goal: p.goal,
    subathon: p.subathon,
    topLikers: p.likeLeaderboard.slice(0, 5),
    topGifters: p.giftLeaderboard.slice(0, 5),
    latestComments: p.comments.slice(-5),
  });
});

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'state', payload: publicState() }));
});

server.listen(PORT, () => {
  console.log(`TikTok dashboard server running on http://localhost:${PORT}`);
});
