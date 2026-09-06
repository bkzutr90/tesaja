require('dotenv').config();

const http = require('http');
const express = require('express');
const cors = require('cors');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

const PORT = process.env.PORT || 3001;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*')
  .split(',')
  .map((s) => s.trim());

const app = express();
app.use(
  cors({
    origin: ALLOWED_ORIGINS.includes('*') ? true : ALLOWED_ORIGINS,
  })
);
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

// ---------------------------------------------------------------------------
// In-memory state for the currently tracked room. One room at a time, which
// matches how a single dashboard instance is used in practice. If you need to
// track several streamers at once, key this by username and adapt the WS
// broadcast to include the room it belongs to.
// ---------------------------------------------------------------------------
let tiktokConnection = null;
let state = freshState();

function freshState() {
  return {
    username: null,
    roomId: null,
    connected: false,
    viewerCount: 0,
    totalLikes: 0,
    totalDiamonds: 0,
    totalFollowers: 0,
    totalShares: 0,
    totalComments: 0,
    startedAt: null,
    lastError: null,
  };
}

function broadcast(type, payload) {
  const message = JSON.stringify({ type, payload });
  wss.clients.forEach((client) => {
    if (client.readyState === client.OPEN) {
      client.send(message);
    }
  });
}

function broadcastState() {
  broadcast('state', state);
}

async function connectToUser(username) {
  // Tear down any existing connection first.
  await disconnectCurrent();

  state = freshState();
  state.username = username;

  tiktokConnection = new WebcastPushConnection(username, {
    processInitialData: false,
    enableExtendedGiftInfo: true,
    // If TikTok's signing service starts rate-limiting anonymous connections,
    // get a free key at https://www.eulerstream.com and uncomment below.
    // signApiKey: process.env.SIGN_API_KEY,
  });

  registerConnectionEvents(tiktokConnection);

  const roomInfo = await tiktokConnection.connect();
  state.connected = true;
  state.roomId = roomInfo.roomId;
  state.startedAt = Date.now();
  broadcast('connected', { username, roomId: roomInfo.roomId });
  broadcastState();
  return roomInfo;
}

async function disconnectCurrent() {
  if (tiktokConnection) {
    try {
      tiktokConnection.disconnect();
    } catch (err) {
      // ignore — connection may already be closed
    }
    tiktokConnection = null;
  }
  if (state.connected) {
    state.connected = false;
    broadcastState();
  }
}

function registerConnectionEvents(connection) {
  connection.on('streamEnd', () => {
    state.connected = false;
    broadcast('streamEnd', {});
    broadcastState();
  });

  connection.on('disconnected', () => {
    state.connected = false;
    broadcast('disconnected', {});
    broadcastState();
  });

  connection.on('error', (err) => {
    state.lastError = String((err && err.message) || err);
    broadcast('error', { message: state.lastError });
  });

  connection.on('roomUser', (data) => {
    if (typeof data.viewerCount === 'number') {
      state.viewerCount = data.viewerCount;
      broadcast('roomUser', { viewerCount: state.viewerCount });
    }
  });

  connection.on('chat', (data) => {
    state.totalComments += 1;
    broadcast('chat', {
      userId: data.userId,
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      comment: data.comment,
      profilePictureUrl: data.profilePictureUrl,
    });
  });

  connection.on('like', (data) => {
    const likeDelta = data.likeCount || 1;
    state.totalLikes = data.totalLikeCount || state.totalLikes + likeDelta;
    broadcast('like', {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      likeCount: likeDelta,
      totalLikeCount: state.totalLikes,
    });
  });

  connection.on('member', (data) => {
    broadcast('member', {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl,
    });
  });

  connection.on('social', (data) => {
    const label = String(data.label || '').toLowerCase();
    const isFollow = label.includes('follow');
    const isShare = label.includes('shar');
    if (isFollow) state.totalFollowers += 1;
    if (isShare) state.totalShares += 1;
    broadcast('social', {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl,
      kind: isFollow ? 'follow' : isShare ? 'share' : 'social',
    });
    broadcastState();
  });

  connection.on('subscribe', (data) => {
    broadcast('subscribe', {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
    });
  });

  connection.on('gift', (data) => {
    // Streakable gifts (giftType === 1) fire repeatedly while the sender holds
    // the button down. Only count the diamonds once the streak is finalized,
    // but still broadcast every tick so the UI can animate the combo.
    const isStreakable = data.giftType === 1;
    const finalized = !isStreakable || data.repeatEnd === true;
    const diamondValue = (data.diamondCount || 0) * (data.repeatCount || 1);

    if (finalized) {
      state.totalDiamonds += diamondValue;
    }

    broadcast('gift', {
      uniqueId: data.uniqueId,
      nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl,
      giftName: data.giftName,
      giftPictureUrl: data.giftPictureUrl,
      diamondCount: data.diamondCount,
      repeatCount: data.repeatCount,
      finalized,
      totalDiamondsThisGift: diamondValue,
    });

    if (finalized) broadcastState();
  });
}

// ---------------------------------------------------------------------------
// REST API
// ---------------------------------------------------------------------------
app.get('/api/health', (_req, res) => res.json({ ok: true }));

app.get('/api/status', (_req, res) => res.json(state));

app.post('/api/connect', async (req, res) => {
  const username = String(req.body.username || '').trim().replace(/^@/, '');
  if (!username) {
    return res.status(400).json({ error: 'username is required' });
  }
  try {
    const roomInfo = await connectToUser(username);
    res.json({ ok: true, username, roomId: roomInfo.roomId });
  } catch (err) {
    state.lastError = String((err && err.message) || err);
    res.status(502).json({
      error: 'Failed to connect. Is this user live right now?',
      detail: state.lastError,
    });
  }
});

app.post('/api/disconnect', async (_req, res) => {
  await disconnectCurrent();
  res.json({ ok: true });
});

wss.on('connection', (socket) => {
  // Send current state immediately so a newly opened dashboard tab
  // is in sync without waiting for the next live event.
  socket.send(JSON.stringify({ type: 'state', payload: state }));
});

server.listen(PORT, () => {
  console.log(`TikTok live dashboard backend listening on port ${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT}/ws`);
});
