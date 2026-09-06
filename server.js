require('dotenv').config();
const path = require('path');
const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent } = require('tiktok-live-connector');

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
    autoIncrease: false, // kalau true, target otomatis naik saat tercapai
    increment: 1000, // besar kenaikan target tiap kali auto-increase terpicu
    timesReached: 0, // counter monoton: berapa kali goal ini sudah tercapai
    _crossed: false, // internal, dipakai deteksi crossing saat autoIncrease OFF
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

    if (state.goal.autoIncrease && state.goal.increment > 0) {
      // Auto-increase: begitu current >= target, target dinaikkan sejumlah
      // `increment` (bisa berkali-kali kalau amount-nya besar/lompat jauh),
      // TANPA mereset current — progress lanjut terus dari angka sekarang.
      // timesReached dihitung PER kelipatan yang terlewati, supaya lompatan
      // besar (mis. like batch 9 -> 25 dengan target 10) tetap terhitung
      // 2x tercapai, bukan cuma 1x — ini yang bikin Roblox cuma spawn 1 mobil
      // padahal seharusnya 2.
      while (state.goal.current >= state.goal.target) {
        state.goal.target += state.goal.increment;
        state.goal.timesReached += 1;
      }
    } else {
      // Mode manual (tanpa auto-increase): target tidak berubah, jadi cukup
      // deteksi crossing sekali sampai direset. _crossed mencegah
      // timesReached nambah terus tiap event selama current masih di atas
      // target yang sama.
      if (state.goal.current >= state.goal.target) {
        if (!state.goal._crossed) {
          state.goal._crossed = true;
          state.goal.timesReached += 1;
        }
      } else {
        state.goal._crossed = false;
      }
    }

    broadcast('goal', state.goal);
  }
}

function addSubathonSeconds(seconds) {
  if (!state.subathon.active || !seconds) return;
  state.subathon.endsAt += seconds * 1000;
  broadcast('subathon', state.subathon);
}

// v2 API: avatar & user fields sudah nested di bawah `user`, bukan flat lagi.
function extractAvatar(user) {
  if (!user) return '';
  return (
    (user.avatarThumb && Array.isArray(user.avatarThumb.urlList) && user.avatarThumb.urlList[0]) ||
    (user.profilePicture && Array.isArray(user.profilePicture.urls) && user.profilePicture.urls[0]) ||
    user.profilePictureUrl ||
    ''
  );
}

// ---------------------------------------------------------------------------
// TikTok connection
// ---------------------------------------------------------------------------
async function connectTikTok(username) {
  if (tiktokConnection) {
    try {
      tiktokConnection.removeAllListeners();
      tiktokConnection.disconnect();
    } catch (_) {}
  }

  // v2: constructor-nya TikTokLiveConnection, bukan WebcastPushConnection.
  // PENTING: selalu kirim objek options eksplisit (walau kosong) — beberapa
  // versi 2.x crash ("Cannot read properties of undefined (reading
  // 'processInitialData')") kalau parameter kedua tidak diberikan sama sekali.
  tiktokConnection = new TikTokLiveConnection(username, {
    processInitialData: false,
    // Kalau kamu sudah punya API key gratis dari eulerstream.com, isi di sini
    // atau lewat env var SIGN_API_KEY. Tanpa ini kamu kena rate-limit tier
    // gratis yang kadang bikin response sign server tidak lengkap.
    signApiKey: process.env.SIGN_API_KEY || undefined,
  });

  const CHAT_EVENT = (typeof WebcastEvent !== 'undefined' && WebcastEvent.CHAT) || 'chat';
  const GIFT_EVENT = (typeof WebcastEvent !== 'undefined' && WebcastEvent.GIFT) || 'gift';
  const LIKE_EVENT = (typeof WebcastEvent !== 'undefined' && WebcastEvent.LIKE) || 'like';
  const FOLLOW_EVENT = (typeof WebcastEvent !== 'undefined' && WebcastEvent.FOLLOW) || 'follow';
  const SHARE_EVENT = (typeof WebcastEvent !== 'undefined' && WebcastEvent.SHARE) || 'share';
  const ROOM_USER_EVENT = (typeof WebcastEvent !== 'undefined' && WebcastEvent.ROOM_USER) || 'roomUser';
  const STREAM_END_EVENT = (typeof WebcastEvent !== 'undefined' && WebcastEvent.STREAM_END) || 'streamEnd';
  const DISCONNECTED_EVENT = (typeof WebcastEvent !== 'undefined' && WebcastEvent.DISCONNECTED) || 'disconnected';

  tiktokConnection.on(CHAT_EVENT, (data) => {
    if (process.env.DEBUG_TIKTOK) console.log('[DEBUG chat]', JSON.stringify(data));
    const uniqueId = data.user?.uniqueId || data.user?.displayId || 'unknown';
    // v2: status follow bukan lagi flat `data.followRole` (field itu sudah
    // tidak ada di API v2 dan selalu undefined) melainkan nested di
    // `data.userIdentity.isFollowerOfAnchor` (boolean). Ini penyebab kenapa
    // walker tidak pernah spawn - followRole selalu ke-default 0.
    const isFollower = !!(data.userIdentity && data.userIdentity.isFollowerOfAnchor);
    const entry = {
      uniqueId,
      nickname: data.user?.nickname || uniqueId,
      profilePicture: extractAvatar(data.user),
      comment: data.comment ?? data.content ?? '',
      // 0 = belum follow, 1 = following (dipertahankan sebagai angka supaya
      // kompatibel dengan Lua yang mengecek `comment.followRole >= 1`)
      followRole: isFollower ? 1 : 0,
      ts: Date.now(),
    };
    state.comments.push(entry);
    if (state.comments.length > 100) state.comments.shift();
    broadcast('comment', entry);
  });

  tiktokConnection.on(LIKE_EVENT, (data) => {
    if (process.env.DEBUG_TIKTOK) console.log('[DEBUG like]', JSON.stringify(data));
    // v2: nama field batch like bisa beda-beda tergantung sub-versi
    // (likeCount / count / totalLikeCount). Ambil yang pertama tersedia.
    const inc = data.likeCount || data.count || data.totalLikeCount || 1;
    state.stats.likes += inc;

    const key = data.user?.uniqueId || data.user?.displayId || 'unknown';
    const existing = state.likeLeaderboard.get(key) || {
      uniqueId: key,
      nickname: data.user?.nickname || key,
      profilePicture: extractAvatar(data.user),
      likes: 0,
    };
    existing.likes += inc;
    state.likeLeaderboard.set(key, existing);

    pushGoalProgress('likes', inc);
    addSubathonSeconds(inc * state.subathon.rules.secondsPerLike);
    broadcast('likeLeaderboard', publicState().likeLeaderboard);
    broadcast('stats', state.stats);
  });

  tiktokConnection.on(GIFT_EVENT, (data) => {
    // Streakable gifts fire repeatedly while the streak continues; only
    // count the final tally once the streak ends (or if it's not streakable).
    const giftType = data.giftDetails?.giftType ?? data.giftType;
    const isStreakable = giftType === 1;
    if (isStreakable && !data.repeatEnd) return;

    const diamondCount = data.giftDetails?.diamondCount ?? data.diamondCount ?? 0;
    const diamonds = diamondCount * (data.repeatCount || 1);
    state.stats.diamonds += diamonds;

    const key = data.user?.uniqueId || data.user?.displayId || 'unknown';
    const existing = state.giftLeaderboard.get(key) || {
      uniqueId: key,
      nickname: data.user?.nickname || key,
      profilePicture: extractAvatar(data.user),
      diamonds: 0,
    };
    existing.diamonds += diamonds;
    state.giftLeaderboard.set(key, existing);

    pushGoalProgress('coins', diamonds);
    addSubathonSeconds(diamonds * state.subathon.rules.secondsPerDiamond);
    broadcast('giftLeaderboard', publicState().giftLeaderboard);
    broadcast('stats', state.stats);
  });

  tiktokConnection.on(FOLLOW_EVENT, () => {
    state.stats.followers += 1;
    pushGoalProgress('followers', 1);
    addSubathonSeconds(state.subathon.rules.secondsPerFollow);
    broadcast('stats', state.stats);
  });

  tiktokConnection.on(SHARE_EVENT, () => {
    state.stats.shares += 1;
    pushGoalProgress('share', 1);
    addSubathonSeconds(state.subathon.rules.secondsPerShare);
    broadcast('stats', state.stats);
  });

  tiktokConnection.on(ROOM_USER_EVENT, (data) => {
    state.stats.viewers = data.viewerCount || state.stats.viewers;
    broadcast('stats', state.stats);
  });

  tiktokConnection.on(STREAM_END_EVENT, () => {
    state.connected = false;
    broadcast('connection', { connected: false, username: state.username });
  });

  tiktokConnection.on(DISCONNECTED_EVENT, () => {
    state.connected = false;
    broadcast('connection', { connected: false, username: state.username });
  });

  tiktokConnection.on('error', (err) => {
    console.error('[TikTok error]', err && err.info ? err.info : err);
  });

  const roomInfo = await tiktokConnection.connect();
  state.connected = true;
  state.username = username;
  broadcast('connection', { connected: true, username, roomId: roomInfo?.roomId });
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
  const { type, target, label, resetCurrent, autoIncrease, increment } = req.body;
  if (type) state.goal.type = type;
  if (typeof target === 'number') state.goal.target = target;
  if (label) state.goal.label = label;
  if (typeof autoIncrease === 'boolean') state.goal.autoIncrease = autoIncrease;
  if (typeof increment === 'number' && increment > 0) {
    state.goal.increment = increment;
  } else if (typeof target === 'number') {
    // default: besar kenaikan sama dengan target yang baru di-set,
    // kecuali user secara eksplisit mengisi kolom increment sendiri
    state.goal.increment = target;
  }
  if (resetCurrent) {
    state.goal.current = 0;
    state.goal.timesReached = 0;
    state.goal._crossed = false;
  }
  broadcast('goal', state.goal);
  res.json(state.goal);
});

app.post('/api/goal/reset', (req, res) => {
  state.goal.current = 0;
  state.goal.timesReached = 0;
  state.goal._crossed = false;
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
