require('dotenv').config({ path: require('path').join(__dirname, 'backend.env') });
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const crypto = require('crypto');
const webpush = require('web-push');
const net = require('net');
const fs = require('fs');
const path = require('path');
const helmet = require('helmet');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const dbModule = require('./database');
const { db, getSetting, setSetting, logPunishment, logWarning, logMessage, logAudit } = dbModule;

const app = express();
const server = http.createServer(app);

// Trust proxy
app.set('trust proxy', 1);

const wssPlugin = new WebSocket.Server({ noServer: true, path: '/ws-plugin' });
const wssPanel = new WebSocket.Server({ noServer: true, path: '/ws-panel' });

const PORT = process.env.PORT || 3344;
const HOST = process.env.HOST || '0.0.0.0';
const JWT_SECRET = process.env.JWT_SECRET || crypto.randomBytes(32).toString('hex');
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');

const DB_PATH = process.env.DATABASE_PATH || './database.db';

// ---------- SECURITY MIDDLEWARE ----------
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
            scriptSrcAttr: ["'unsafe-inline'"],
            styleSrc: ["'self'", "'unsafe-inline'"],
            styleSrcAttr: ["'unsafe-inline'"],
            imgSrc: ["'self'", "data:", "https:"],
            connectSrc: ["'self'", "ws:", "wss:", "http:", "https:"],
            fontSrc: ["'self'", "https:", "data:"],
            objectSrc: ["'none'"],
            mediaSrc: ["'self'"],
            frameSrc: ["'none'"]
        }
    },
    crossOriginEmbedderPolicy: false,
    crossOriginResourcePolicy: { policy: "cross-origin" }
}));

app.use(cors({
    origin: process.env.CORS_ORIGIN || '*',
    credentials: true
}));

app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));

// ---------- STATIC FILES ----------
app.use(express.static(__dirname, {
    index: 'index.html',
    extensions: ['html']
}));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// ---------- RATE LIMITING ----------
const apiLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 1000,
    message: { error: 'Too many requests' },
    standardHeaders: true,
    legacyHeaders: false,
});

const loginLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 10,
    message: { error: 'Too many login attempts' },
    standardHeaders: true,
    legacyHeaders: false,
});

app.use('/api/', apiLimiter);
app.use('/api/auth/login', loginLimiter);

// ---------- FILE STORAGE ----------
const SAVE_DIR = path.join(__dirname, 'save');
const MESSAGES_FILE = path.join(SAVE_DIR, 'messages.json');
const MESSAGES_READ_FILE = path.join(SAVE_DIR, 'messages-read.json');
const BACKUP_DIR = path.join(SAVE_DIR, 'backups');
const DB_BACKUP_DIR = path.join(BACKUP_DIR, 'db');

if (!fs.existsSync(SAVE_DIR)) fs.mkdirSync(SAVE_DIR, { recursive: true });
if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true });
if (!fs.existsSync(DB_BACKUP_DIR)) fs.mkdirSync(DB_BACKUP_DIR, { recursive: true });
if (!fs.existsSync(MESSAGES_FILE)) fs.writeFileSync(MESSAGES_FILE, JSON.stringify({ messages: [], total: 0 }, null, 2));

function getTodayString() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
}

if (!fs.existsSync(MESSAGES_READ_FILE)) fs.writeFileSync(MESSAGES_READ_FILE, JSON.stringify({ date: getTodayString(), messages: [], total: 0 }, null, 2));

function saveMessageToFile(messageData) {
  try {
    const allData = JSON.parse(fs.readFileSync(MESSAGES_FILE, 'utf8'));
    allData.messages.push(messageData);
    allData.total = allData.messages.length;
    if (allData.messages.length > 10000) {
      allData.messages = allData.messages.slice(-10000);
      allData.total = allData.messages.length;
    }
    fs.writeFileSync(MESSAGES_FILE, JSON.stringify(allData, null, 2));
    
    const readData = JSON.parse(fs.readFileSync(MESSAGES_READ_FILE, 'utf8'));
    const today = getTodayString();
    if (readData.date !== today) {
      readData.date = today;
      readData.messages = [];
      readData.total = 0;
    }
    readData.messages.push(messageData);
    readData.total = readData.messages.length;
    fs.writeFileSync(MESSAGES_READ_FILE, JSON.stringify(readData, null, 2));
  } catch (e) {
    console.error('[ERROR] Failed to save message to file:', e.message);
  }
}

function createBackup() {
    const today = getTodayString();
    if (fs.existsSync(MESSAGES_FILE)) {
        fs.copyFileSync(MESSAGES_FILE, path.join(BACKUP_DIR, `messages-${today}.json`));
    }
    if (fs.existsSync(MESSAGES_READ_FILE)) {
        fs.copyFileSync(MESSAGES_READ_FILE, path.join(BACKUP_DIR, `messages-read-${today}.json`));
    }
}

setInterval(() => {
    const now = new Date();
    if (now.getHours() === 0 && now.getMinutes() === 0 && now.getSeconds() < 30) {
        createBackup();
        fs.writeFileSync(MESSAGES_READ_FILE, JSON.stringify({ date: getTodayString(), messages: [], total: 0 }, null, 2));
        console.log('[INFO] Daily backup created and messages-read.json rotated');
    }
}, 30000);

// ---------- DB BACKUP (co godzinę) ----------
setInterval(() => {
    try {
        const today = getTodayString();
        const backupPath = path.join(DB_BACKUP_DIR, `database-${today}.db`);
        if (!fs.existsSync(backupPath)) {
            fs.copyFileSync(DB_PATH, backupPath);
            console.log(`[BACKUP] DB backup created: ${backupPath}`);
        }
        // Usuń backupy starsze niż 30 dni
        const files = fs.readdirSync(DB_BACKUP_DIR).filter(f => f.endsWith('.db'));
        const cutoff = Date.now() - (30 * 86400 * 1000);
        for (const f of files) {
            const stat = fs.statSync(path.join(DB_BACKUP_DIR, f));
            if (stat.mtimeMs < cutoff) {
                fs.unlinkSync(path.join(DB_BACKUP_DIR, f));
                console.log(`[BACKUP] Removed old backup: ${f}`);
            }
        }
    } catch (e) {
        console.error('[ERROR] DB backup:', e.message);
    }
}, 3600 * 1000);

// ---------- AUTO-CLEANUP (co 24h, 90 dni) ----------
setInterval(() => {
    try {
        const cutoff = Math.floor(Date.now()/1000) - (90 * 86400);
        const deletedMsgs = db.prepare('DELETE FROM messages WHERE timestamp < ?').run(cutoff);
        const deletedAudit = db.prepare('DELETE FROM audit_logs WHERE timestamp < ?').run(cutoff);
        if (deletedMsgs.changes > 0 || deletedAudit.changes > 0) {
            console.log(`[CLEANUP] Usunięto ${deletedMsgs.changes} wiadomości, ${deletedAudit.changes} logów (starsze niż 90 dni)`);
        }
    } catch (e) {
        console.error('[ERROR] Auto-cleanup:', e.message);
    }
}, 24 * 3600 * 1000);

// ---------- LOGGING ----------
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    if (req.path.startsWith('/api/')) {
      console.log(`[API] ${new Date().toISOString()} ${req.method} ${req.path} ${res.statusCode} ${duration}ms`);
    }
  });
  next();
});

// ---------- COOKIE PARSER ----------
function parseCookies(req) {
  const list = {};
  const rc = req.headers.cookie;
  if (rc) {
    rc.split(';').forEach(cookie => {
      const parts = cookie.split('=');
      list[parts.shift().trim()] = decodeURIComponent(parts.join('='));
    });
  }
  return list;
}
app.use((req, res, next) => { req.cookies = parseCookies(req); next(); });

// ---------- ENCRYPTION ----------
function encryptSecret(secret) {
  try {
    const key = crypto.createHash('sha256').update(SESSION_SECRET).digest();
    const iv = crypto.randomBytes(16);
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    let encrypted = cipher.update(secret, 'utf8', 'hex');
    encrypted += cipher.final('hex');
    return `${iv.toString('hex')}:${cipher.getAuthTag().toString('hex')}:${encrypted}`;
  } catch (e) { return null; }
}

function decryptSecret(encrypted) {
  try {
    const [ivHex, authTagHex, data] = encrypted.split(':');
    const key = crypto.createHash('sha256').update(SESSION_SECRET).digest();
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(ivHex, 'hex'));
    decipher.setAuthTag(Buffer.from(authTagHex, 'hex'));
    let decrypted = decipher.update(data, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  } catch (e) { return null; }
}

// ---------- AUTH MIDDLEWARE ----------
function authenticateToken(req, res, next) {
  const token = req.cookies?.token || req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ error: 'Invalid token' });
    req.user = user;
    next();
  });
}

function requireAdmin(req, res, next) {
  authenticateToken(req, res, () => {
    const admin = db.prepare('SELECT * FROM admins WHERE id = ?').get(req.user.id);
    if (!admin) return res.status(403).json({ error: 'Admin not found' });
    req.admin = admin;
    next();
  });
}

// ---------- WEBSOCKET ----------
const pluginConnections = new Map();
const panelClients = new Set();
const pendingRequests = new Map(); // serverId -> [requests]

function getFilterConfig() {
  return db.prepare('SELECT word, action, duration_ms FROM filter_words WHERE enabled=1').all()
    .map(r => ({ word: r.word, action: r.action, duration_ms: r.duration_ms || 0 }));
}

function broadcastFilterUpdate() {
  const config = getFilterConfig();
  broadcastToPlugins({ 
    type: 'FILTER_UPDATE', 
    filter_words: config.map(f => f.word),
    filter_config: config
  });
}

function broadcastToPlugins(message) {
  const data = JSON.stringify(message);
  for (const conn of pluginConnections.values()) {
    if (conn.connected && conn.ws.readyState === WebSocket.OPEN) conn.ws.send(data);
  }
}

function broadcastToPanel(message) {
  const data = JSON.stringify(message);
  for (const client of panelClients) {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}

// WebSocket upgrade handler
server.on('upgrade', (req, socket, head) => {
  const pathname = new URL(req.url, `http://${req.headers.host}`).pathname;
  console.log(`[WS] Upgrade request: ${pathname}`);
  
  if (pathname === '/ws-plugin') {
    wssPlugin.handleUpgrade(req, socket, head, (ws) => {
      wssPlugin.emit('connection', ws, req);
    });
  } else if (pathname === '/ws-panel') {
    wssPanel.handleUpgrade(req, socket, head, (ws) => {
      wssPanel.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});

// ============================================================
// PLUGIN WEBSOCKET — token w AUTH (fallback URL)
// ============================================================
wssPlugin.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const urlServerId = url.searchParams.get('serverId');
  const urlToken = url.searchParams.get('token');
  
  console.log(`[PLUGIN WS] Connection attempt - urlServerId: ${urlServerId || 'none'}, urlToken: ${urlToken ? 'provided' : 'missing'}`);
  
  ws.isAlive = true;
  let authenticated = false;
  let connInfo = null;
  let serverId = null;
  
  const authTimeout = setTimeout(() => {
    if (!authenticated) {
      console.log('[PLUGIN WS] Auth timeout — closing');
      ws.close(4001, 'Auth timeout');
    }
  }, 10000);
  
  ws.on('pong', () => { 
    ws.isAlive = true; 
    if (connInfo) connInfo.lastSeen = Date.now(); 
  });
  
  ws.on('message', (data) => {
    let message;
    try {
      message = JSON.parse(data.toString());
    } catch (e) {
      console.error('[ERROR] Invalid JSON from plugin:', e.message);
      return;
    }
    
    // === OBSŁUGA AUTH ===
    if (message.type === 'AUTH' && !authenticated) {
      const authServerId = message.server_id || urlServerId;
      const authToken = message.token || urlToken;
      
      if (!authServerId || !authToken) {
        console.log('[PLUGIN WS] REJECTED - Missing credentials in AUTH');
        ws.close(4001, 'Missing credentials');
        return;
      }
      
      const serverRecord = db.prepare('SELECT * FROM servers WHERE id = ? AND api_token = ?').get(authServerId, authToken);
      
      if (!serverRecord) {
        console.log(`[PLUGIN WS] REJECTED - Invalid credentials for ${authServerId}`);
        ws.close(4002, 'Invalid credentials');
        return;
      }
      
      // Sukces autoryzacji
      authenticated = true;
      clearTimeout(authTimeout);
      serverId = authServerId;
      ws.serverId = serverId;
      
      // Auto-sync nazwy serwera
      if (message.server_name && message.server_name !== serverRecord.name) {
        try {
          db.prepare('UPDATE servers SET name = ? WHERE id = ?').run(message.server_name, serverId);
          console.log(`[PLUGIN WS] Updated server name: ${serverRecord.name} → ${message.server_name}`);
          serverRecord.name = message.server_name;
        } catch (e) {
          console.error('[ERROR] Failed to update server name:', e.message);
        }
      }
      
      console.log(`[PLUGIN WS] ACCEPTED - ${serverId} (${serverRecord.name})`);
      
      connInfo = {
        ws, connected: true, playerCount: 0, lastSeen: Date.now(),
        serverName: serverRecord.name,
        pluginVersion: message.plugin_version || null,
        minecraftVersion: message.minecraft_version || null
      };
      pluginConnections.set(serverId, connInfo);
      
      // Wyślij sync
      sendSyncData(serverId, connInfo);
      
      // Wyślij zakolejkowane requesty (retry offline)
      const pending = pendingRequests.get(serverId) || [];
      for (const req of pending) {
        try {
          connInfo.ws.send(JSON.stringify({
            type: req.type + '_REQUEST',
            event_id: crypto.randomUUID(),
            server_id: serverId,
            timestamp: Date.now(),
            payload: { player_name: req.playerName, duration: req.duration, reason: req.reason, source: req.source }
          }));
        } catch (e) {
          console.error('[ERROR] Failed to send pending request:', e.message);
        }
      }
      if (pending.length > 0) {
        console.log(`[QUEUE] Sent ${pending.length} pending requests to ${serverId}`);
        pendingRequests.delete(serverId);
      }
      
      broadcastToPanel({ type: 'SERVER_STATUS', server_id: serverId, status: 'ONLINE', server_name: serverRecord.name });
      return;
    }
    
    // === INNE WIADOMOŚCI — wymagają autoryzacji ===
    if (!authenticated) {
      console.log('[PLUGIN WS] Ignoring message before AUTH');
      return;
    }
    
    handlePluginMessage(serverId, message, connInfo);
  });
  
  ws.on('close', () => {
    clearTimeout(authTimeout);
    if (connInfo) {
      connInfo.connected = false;
      ws.isAlive = false;
      console.log(`[PLUGIN WS] Disconnected: ${serverId}`);
      broadcastToPanel({ type: 'SERVER_STATUS', server_id: serverId, status: 'OFFLINE' });
    }
  });
  
  ws.on('error', (err) => {
    console.error(`[ERROR] WebSocket plugin error for ${serverId || 'unknown'}:`, err.message);
  });
});

// ============================================================
// SYNC DATA
// ============================================================
function sendSyncData(serverId, connInfo) {
  try {
    const filterConfig = getFilterConfig();
    const filterWords = filterConfig.map(f => f.word);
    
    const now = Math.floor(Date.now()/1000);
    const activeMutes = db.prepare(`SELECT * FROM punishments WHERE action='MUTE' AND active=1 AND (expires_at IS NULL OR expires_at > ?)`).all(now);
    const activeBans = db.prepare(`SELECT * FROM punishments WHERE action='BAN' AND active=1 AND (expires_at IS NULL OR expires_at > ?)`).all(now);
    const automessages = db.prepare('SELECT * FROM automessages WHERE enabled=1').all();
    
    if (connInfo.ws.readyState === WebSocket.OPEN) {
      connInfo.ws.send(JSON.stringify({
        type: 'SYNC_RESPONSE',
        mutes: activeMutes.map(p => ({ 
          player_uuid: p.player_uuid, 
          until: p.expires_at ? p.expires_at * 1000 : 9223372036854775807, 
          reason: p.reason, 
          moderator: p.moderator 
        })),
        bans: activeBans.map(p => ({ 
          player_uuid: p.player_uuid,
          until: p.expires_at ? p.expires_at * 1000 : 9223372036854775807,
          reason: p.reason,
          moderator: p.moderator
        })),
        filter_words: filterWords,
        filter_config: filterConfig,
        automessages: automessages.map(a => ({
          id: a.id,
          name: a.name,
          content: a.content,
          interval_seconds: a.interval_seconds,
          enabled: a.enabled
        }))
      }));
      console.log(`[PLUGIN WS] Sync sent to ${serverId}: ${filterConfig.length} filters, ${activeMutes.length} mutes, ${activeBans.length} bans, ${automessages.length} automessages`);
    }
  } catch (e) {
    console.error('[ERROR] sendSyncData:', e.message);
  }
}

// ============================================================
// HANDLE PLUGIN MESSAGE
// ============================================================
function handlePluginMessage(serverId, msg, connInfo) {
  const type = msg.type;
  if (!type) return;
  connInfo.lastSeen = Date.now();
  
  switch (type) {
    case 'AUTH':
      if (msg.server_name) connInfo.serverName = msg.server_name;
      if (msg.plugin_version) connInfo.pluginVersion = msg.plugin_version;
      if (msg.minecraft_version) connInfo.minecraftVersion = msg.minecraft_version;
      break;
      
    case 'PING':
      if (connInfo.ws.readyState === WebSocket.OPEN) {
        connInfo.ws.send(JSON.stringify({ type: 'PONG', timestamp: Date.now() }));
      }
      break;
      
    case 'CHAT_MESSAGE': {
      const payload = msg.payload || {};
      const timestamp = Math.floor(Date.now()/1000);
      logMessage(serverId, payload.player_uuid, payload.player_name, payload.message, timestamp);
      
      const messageData = {
        server_id: serverId,
        player_uuid: payload.player_uuid,
        player_name: payload.player_name,
        message: payload.message,
        timestamp: Date.now(),
        date: getTodayString()
      };
      saveMessageToFile(messageData);
      
      if (payload.player_uuid) {
        db.prepare('INSERT INTO players (uuid, name, first_seen, last_seen) VALUES (?, ?, ?, ?) ON CONFLICT(uuid) DO UPDATE SET name=excluded.name, last_seen=excluded.last_seen')
          .run(payload.player_uuid, payload.player_name, timestamp, timestamp);
      }
      
      broadcastToPanel({
        type: 'CHAT_MESSAGE',
        server_id: serverId,
        player_name: payload.player_name,
        player_uuid: payload.player_uuid,
        message: payload.message,
        timestamp: Date.now()
      });
      break;
    }
      
    case 'PLAYER_JOIN': {
      const payload = msg.payload || {};
      const timestamp = Math.floor(Date.now()/1000);
      db.prepare('INSERT INTO players (uuid, name, first_seen, last_seen) VALUES (?, ?, ?, ?) ON CONFLICT(uuid) DO UPDATE SET name=excluded.name, last_seen=excluded.last_seen')
        .run(payload.player_uuid, payload.player_name, timestamp, timestamp);
      
      // playerCount z pluginu (dokładny) lub inkrementacja
      if (msg.player_count !== undefined) {
        connInfo.playerCount = msg.player_count;
      } else {
        connInfo.playerCount = (connInfo.playerCount || 0) + 1;
      }
      
      broadcastToPanel({ type: 'PLAYER_JOIN', server_id: serverId, player_name: payload.player_name, player_uuid: payload.player_uuid, timestamp: Date.now() });
      break;
    }
      
    case 'PLAYER_QUIT': {
      const payload = msg.payload || {};
      if (msg.player_count !== undefined) {
        connInfo.playerCount = msg.player_count;
      } else if (connInfo.playerCount > 0) {
        connInfo.playerCount--;
      }
      broadcastToPanel({ type: 'PLAYER_QUIT', server_id: serverId, player_name: payload.player_name, player_uuid: payload.player_uuid, timestamp: Date.now() });
      break;
    }
      
    case 'MUTE': case 'UNMUTE': case 'WARN': case 'KICK':
    case 'BAN': case 'UNBAN': case 'CLEAR_CHAT':
    case 'CHAT_LOCK': case 'CHAT_UNLOCK': {
      const payload = msg.payload || {};
      const action = type;
      const id = msg.event_id || crypto.randomUUID();
      const now = Math.floor(Date.now()/1000);
      
      if (action === 'MUTE' || action === 'BAN') {
        const duration = payload.duration_ms || 0;
        const expiresAt = duration > 0 ? now + Math.floor(duration / 1000) : null;
        logPunishment({
          id, player_uuid: payload.player_uuid, player_name: payload.player_name,
          action, moderator: payload.source || 'SYSTEM', reason: payload.reason,
          server_id: serverId, source: payload.source || 'MINECRAFT',
          duration_ms: duration, expires_at: expiresAt, active: 1
        });
      } else if (action === 'UNMUTE' || action === 'UNBAN') {
        db.prepare(`UPDATE punishments SET active=0 WHERE player_uuid=? AND action=? AND active=1`)
          .run(payload.player_uuid, action === 'UNMUTE' ? 'MUTE' : 'BAN');
        logPunishment({
          id, player_uuid: payload.player_uuid, player_name: payload.player_name,
          action, moderator: payload.source || 'SYSTEM', reason: payload.reason,
          server_id: serverId, source: payload.source || 'MINECRAFT', active: 0
        });
      } else if (action === 'WARN') {
        logWarning({
          id, player_uuid: payload.player_uuid, player_name: payload.player_name,
          reason: payload.reason, moderator: payload.source || 'SYSTEM',
          server_id: serverId, source: payload.source || 'MINECRAFT', created_at: now
        });
      } else {
        logPunishment({
          id, player_uuid: payload.player_uuid, player_name: payload.player_name,
          action, moderator: payload.source || 'SYSTEM', reason: payload.reason,
          server_id: serverId, source: payload.source || 'MINECRAFT', active: 0
        });
      }
      
      broadcastToPanel({
        type: 'MODERATION_ACTION', action, server_id: serverId,
        player_name: payload.player_name, reason: payload.reason,
        source: payload.source, timestamp: Date.now()
      });
      
      // Discord webhook dla warnów i innych akcji
      const discordUrl = getSetting('discord_webhook_url');
      if (discordUrl) {
        const emoji = action === 'WARN' ? '⚠️' : (action === 'BAN' ? '🔨' : '🚨');
        sendDiscordWebhook(discordUrl, {
          title: `${emoji} ${action}`,
          description: `**Player:** ${payload.player_name}\n**Server:** ${serverId}\n**Moderator:** ${payload.source || 'SYSTEM'}\n**Reason:** ${payload.reason || 'N/A'}`
        });
      }
      break;
    }
      
    case 'SLOWMODE_CHANGED': {
      const payload = msg.payload || {};
      console.log(`[PLUGIN] Slowmode changed on ${serverId}: ${payload.seconds}s`);
      broadcastToPanel({
        type: 'SLOWMODE_CHANGED',
        server_id: serverId,
        seconds: payload.seconds,
        timestamp: Date.now()
      });
      break;
    }

    case 'BROADCAST': {
      const payload = msg.payload || {};
      console.log(`[PLUGIN] Broadcast on ${serverId}: ${payload.message}`);
      broadcastToPanel({
        type: 'BROADCAST',
        server_id: serverId,
        message: payload.message,
        sender: payload.sender,
        timestamp: Date.now()
      });
      break;
    }
      
    case 'SYNC_REQUEST': {
      sendSyncData(serverId, connInfo);
      break;
    }
      
    default:
      console.log(`[WARN] Unknown plugin message type: ${type}`);
  }
}

// PANEL WEBSOCKET
wssPanel.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const token = url.searchParams.get('token');
  if (!token) { ws.close(4001, 'Missing token'); return; }
  try {
    const user = jwt.verify(token, JWT_SECRET);
    ws.user = user;
    ws.messageCount = 0;
    ws.lastReset = Date.now();
    panelClients.add(ws);
    console.log(`[PANEL WS] Client connected: ${user.username}`);
    
    const servers = db.prepare('SELECT * FROM servers').all();
    const serverStatus = servers.map(s => {
      const conn = pluginConnections.get(s.id);
      return { id: s.id, name: s.name, online: conn ? conn.connected : false, playerCount: conn ? (conn.playerCount || 0) : 0 };
    });
    ws.send(JSON.stringify({ type: 'INITIAL_DATA', servers: serverStatus, timestamp: Date.now() }));
  } catch (e) { ws.close(4002, 'Invalid token'); return; }
  ws.on('close', () => panelClients.delete(ws));
});

// Rate-limit helper dla panelu
function checkPanelRateLimit(ws) {
    const now = Date.now();
    if (now - ws.lastReset > 1000) {
        ws.messageCount = 0;
        ws.lastReset = now;
    }
    ws.messageCount++;
    return ws.messageCount <= 30; // max 30/s
}

// ---------- API ROUTES ----------

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Missing credentials' });
  try {
    const admin = db.prepare('SELECT * FROM admins WHERE username = ?').get(username);
    if (!admin) return res.status(401).json({ error: 'Invalid credentials' });
    if (bcrypt.compareSync(password, admin.password_hash)) {
      const token = jwt.sign({ id: admin.id, username: admin.username }, JWT_SECRET, { expiresIn: '12h' });
      logAudit(admin.id, 'LOGIN', 'User logged in', req.ip);
      res.json({ token, user: { id: admin.id, username: admin.username } });
    } else {
      res.status(401).json({ error: 'Invalid credentials' });
    }
  } catch (e) {
    console.error('[ERROR] Login error:', e);
    res.status(500).json({ error: 'Internal error' });
  }
});

app.post('/api/auth/logout', authenticateToken, (req, res) => {
  logAudit(req.user.id, 'LOGOUT', 'User logged out', req.ip);
  res.json({ ok: true });
});

app.get('/api/dashboard', requireAdmin, (req, res) => {
  try {
    const now = Math.floor(Date.now()/1000);
    const todayStart = Math.floor(new Date().setHours(0,0,0,0)/1000);
    const servers = db.prepare('SELECT * FROM servers').all();
    const onlineServers = servers.filter(s => pluginConnections.has(s.id) && pluginConnections.get(s.id).connected);
    const activeMutes = db.prepare(`SELECT COUNT(*) as cnt FROM punishments WHERE action='MUTE' AND active=1 AND (expires_at IS NULL OR expires_at > ?)`).get(now).cnt;
    const activeBans = db.prepare(`SELECT COUNT(*) as cnt FROM punishments WHERE action='BAN' AND active=1 AND (expires_at IS NULL OR expires_at > ?)`).get(now).cnt;
    const warningsToday = db.prepare('SELECT COUNT(*) as cnt FROM warnings WHERE created_at >= ?').get(todayStart).cnt;
    const messagesToday = db.prepare('SELECT COUNT(*) as cnt FROM messages WHERE timestamp >= ?').get(todayStart).cnt;
    const onlinePlayers = Array.from(pluginConnections.values()).reduce((sum, conn) => sum + (conn.connected ? (conn.playerCount || 0) : 0), 0);
    const recentPunishments = db.prepare('SELECT * FROM punishments ORDER BY created_at DESC LIMIT 10').all();
    const recentMessages = db.prepare('SELECT * FROM messages ORDER BY timestamp DESC LIMIT 10').all();
    res.json({
      stats: { onlineServers: onlineServers.length, totalServers: servers.length, onlinePlayers, messagesToday, activeMutes, activeBans, warningsToday },
      recentPunishments,
      recentMessages
    });
  } catch (e) {
    console.error('[ERROR] Dashboard error:', e);
    res.status(500).json({ error: 'Internal error' });
  }
});

// ---------- HEALTHCHECK ----------
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        uptime: Math.floor(process.uptime()),
        pluginConnections: pluginConnections.size,
        panelClients: panelClients.size,
        memory: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB',
        pendingRequests: Array.from(pendingRequests.values()).reduce((sum, arr) => sum + arr.length, 0),
        timestamp: Date.now()
    });
});

// ---------- SERVERS ----------
app.get('/api/servers', requireAdmin, (req, res) => {
  try {
    const servers = db.prepare('SELECT * FROM servers').all();
    const result = servers.map(s => {
      const conn = pluginConnections.get(s.id);
      return { ...s, online: conn ? conn.connected : false, playerCount: conn ? (conn.playerCount || 0) : 0 };
    });
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers', requireAdmin, (req, res) => {
  const { name, ip, port, rcon_enabled, rcon_port, rcon_password, query_enabled, query_port } = req.body;
  if (!name || !ip || !port) return res.status(400).json({ error: 'Missing fields' });
  const id = 'srv_' + crypto.randomBytes(4).toString('hex');
  const api_token = crypto.randomBytes(32).toString('hex');
  try {
    db.prepare(`INSERT INTO servers (id, name, ip, port, rcon_enabled, rcon_port, rcon_password_encrypted, query_enabled, query_port, api_token) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, name, ip, port, rcon_enabled ? 1 : 0, rcon_port || null, rcon_password ? encryptSecret(rcon_password) : null, query_enabled ? 1 : 0, query_port || null, api_token);
    logAudit(req.user.id, 'SERVER_ADD', `Added server ${name}`, req.ip);
    res.json({ id, api_token });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/servers/:id', requireAdmin, (req, res) => {
  const { name, ip, port } = req.body;
  try {
    const existing = db.prepare('SELECT * FROM servers WHERE id = ?').get(req.params.id);
    if (!existing) return res.status(404).json({ error: 'Server not found' });
    db.prepare('UPDATE servers SET name=?, ip=?, port=? WHERE id=?')
      .run(name || existing.name, ip || existing.ip, port || existing.port, req.params.id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/servers/:id', requireAdmin, (req, res) => {
  try {
    db.prepare('DELETE FROM servers WHERE id = ?').run(req.params.id);
    if (pluginConnections.has(req.params.id)) pluginConnections.delete(req.params.id);
    logAudit(req.user.id, 'SERVER_DELETE', `Deleted server ${req.params.id}`, req.ip);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- CHAT ----------
app.get('/api/servers/:id/chat', requireAdmin, (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);
  try {
    const messages = db.prepare('SELECT * FROM messages WHERE server_id = ? ORDER BY timestamp DESC LIMIT ?').all(req.params.id, limit);
    res.json(messages.reverse());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:id/chat', requireAdmin, (req, res) => {
  const { message } = req.body;
  if (!message || message.length > 256) return res.status(400).json({ error: 'Invalid message' });
  const conn = pluginConnections.get(req.params.id);
  if (!conn || !conn.connected) return res.status(400).json({ error: 'Server offline' });
  conn.ws.send(JSON.stringify({ type: 'CHAT_MESSAGE', server_id: req.params.id, message, source: 'PANEL' }));
  logAudit(req.user.id, 'CHAT_SEND', `Sent message to ${req.params.id}`, req.ip);
  res.json({ ok: true });
});

// ---------- PLAYERS ----------
app.get('/api/players', requireAdmin, (req, res) => {
  const search = req.query.search || '';
  const limit = Math.min(parseInt(req.query.limit) || 100, 500);
  try {
    let players;
    if (search) {
      players = db.prepare('SELECT * FROM players WHERE name LIKE ? OR uuid LIKE ? LIMIT ?').all(`%${search}%`, `%${search}%`, limit);
    } else {
      players = db.prepare('SELECT * FROM players ORDER BY last_seen DESC LIMIT ?').all(limit);
    }
    res.json(players);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/players/:id', requireAdmin, (req, res) => {
  try {
    const player = db.prepare('SELECT * FROM players WHERE uuid = ? OR name = ?').get(req.params.id, req.params.id);
    if (!player) return res.status(404).json({ error: 'Player not found' });
    const punishments = db.prepare('SELECT * FROM punishments WHERE player_uuid = ? ORDER BY created_at DESC LIMIT 50').all(player.uuid);
    const warnings = db.prepare('SELECT * FROM warnings WHERE player_uuid = ? ORDER BY created_at DESC LIMIT 50').all(player.uuid);
    const messages = db.prepare('SELECT * FROM messages WHERE player_uuid = ? ORDER BY timestamp DESC LIMIT 50').all(player.uuid);
    res.json({ ...player, punishments, warnings, messages });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- MODERATION ----------
function sendModerationRequest(serverId, type, playerName, duration, reason, source = 'WEB') {
  const conn = pluginConnections.get(serverId);
  if (!conn || !conn.connected) {
    // Zakolejkuj dla offline serwera
    if (!pendingRequests.has(serverId)) pendingRequests.set(serverId, []);
    pendingRequests.get(serverId).push({ type, playerName, duration, reason, source, ts: Date.now() });
    console.log(`[QUEUE] Queued ${type} for offline server ${serverId}`);
    return false;
  }
  const event = {
    type: type + '_REQUEST',
    event_id: crypto.randomUUID(),
    server_id: serverId,
    timestamp: Date.now(),
    payload: { player_name: playerName, duration: duration || 0, reason: reason || '', source }
  };
  conn.ws.send(JSON.stringify(event));
  return true;
}

app.post('/api/moderation/:action', requireAdmin, (req, res) => {
  const { action } = req.params;
  const { server_id, player_name, duration, reason } = req.body;
  if (!server_id || !player_name) return res.status(400).json({ error: 'Missing fields' });
  
  const validActions = ['mute', 'unmute', 'warn', 'kick', 'ban', 'unban'];
  if (!validActions.includes(action.toLowerCase())) return res.status(400).json({ error: 'Invalid action' });
  
  const success = sendModerationRequest(server_id, action.toUpperCase(), player_name, duration || 0, reason || '', 'WEB');
  logAudit(req.user.id, `${action.toUpperCase()}_REQUEST`, `${action} ${player_name}`, req.ip);
  res.json({ ok: true, pending: !success });
});

app.post('/api/moderation/clear-chat', requireAdmin, (req, res) => {
  const { server_id } = req.body;
  if (!server_id) return res.status(400).json({ error: 'Missing server_id' });
  const conn = pluginConnections.get(server_id);
  if (!conn || !conn.connected) return res.status(400).json({ error: 'Server offline' });
  conn.ws.send(JSON.stringify({ type: 'CLEAR_CHAT_REQUEST', server_id }));
  res.json({ ok: true });
});

app.post('/api/moderation/chat-lock', requireAdmin, (req, res) => {
  const { server_id, locked, duration, reason } = req.body;
  if (!server_id || locked === undefined) return res.status(400).json({ error: 'Missing fields' });
  const conn = pluginConnections.get(server_id);
  if (!conn || !conn.connected) return res.status(400).json({ error: 'Server offline' });
  const type = locked ? 'CHAT_LOCK_REQUEST' : 'CHAT_UNLOCK_REQUEST';
  conn.ws.send(JSON.stringify({ type, server_id, duration: duration || 0, reason: reason || '' }));
  res.json({ ok: true });
});

app.post('/api/moderation/slowmode', requireAdmin, (req, res) => {
  const { server_id, seconds } = req.body;
  if (!server_id || seconds === undefined) return res.status(400).json({ error: 'Missing fields' });
  const conn = pluginConnections.get(server_id);
  if (!conn || !conn.connected) return res.status(400).json({ error: 'Server offline' });
  conn.ws.send(JSON.stringify({ type: 'SLOWMODE_REQUEST', server_id, seconds: parseInt(seconds) || 0 }));
  logAudit(req.user.id, 'SLOWMODE_REQUEST', `Slowmode ${seconds}s on ${server_id}`, req.ip);
  res.json({ ok: true });
});

app.post('/api/moderation/broadcast', requireAdmin, (req, res) => {
  const { server_id, message } = req.body;
  if (!server_id || !message) return res.status(400).json({ error: 'Missing fields' });
  const conn = pluginConnections.get(server_id);
  if (!conn || !conn.connected) return res.status(400).json({ error: 'Server offline' });
  conn.ws.send(JSON.stringify({ type: 'BROADCAST_REQUEST', server_id, message }));
  logAudit(req.user.id, 'BROADCAST_REQUEST', `Broadcast to ${server_id}`, req.ip);
  res.json({ ok: true });
});

// ---------- LOGS ----------
app.get('/api/logs', requireAdmin, (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 100, 500);
  try {
    res.json(db.prepare('SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT ?').all(limit));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- AUTOMESSAGES ----------
app.get('/api/automessages', requireAdmin, (req, res) => {
  try { res.json(db.prepare('SELECT * FROM automessages').all()); } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/automessages', requireAdmin, (req, res) => {
  const { name, content, interval_seconds, enabled, servers } = req.body;
  if (!content || !interval_seconds) return res.status(400).json({ error: 'Missing fields' });
  try {
    const info = db.prepare('INSERT INTO automessages (name, content, interval_seconds, enabled, servers) VALUES (?, ?, ?, ?, ?)')
      .run(name || '', content, interval_seconds, enabled ? 1 : 0, servers ? servers.join(',') : 'ALL');
    logAudit(req.user.id, 'AUTOMESSAGE_ADD', `Added automessage: ${content}`, req.ip);
    
    const automessages = db.prepare('SELECT * FROM automessages WHERE enabled=1').all();
    broadcastToPlugins({
      type: 'FILTER_UPDATE',
      filter_config: getFilterConfig(),
      filter_words: getFilterConfig().map(f => f.word),
      automessages: automessages.map(a => ({
        id: a.id, name: a.name, content: a.content,
        interval_seconds: a.interval_seconds, enabled: a.enabled
      }))
    });
    
    res.json({ id: info.lastInsertRowid });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/automessages/:id', requireAdmin, (req, res) => {
  const { name, content, interval_seconds, enabled } = req.body;
  try {
    const existing = db.prepare('SELECT * FROM automessages WHERE id = ?').get(req.params.id);
    if (!existing) return res.status(404).json({ error: 'Automessage not found' });
    db.prepare('UPDATE automessages SET name=?, content=?, interval_seconds=?, enabled=? WHERE id=?')
      .run(name || existing.name, content || existing.content, interval_seconds || existing.interval_seconds, enabled !== undefined ? (enabled ? 1 : 0) : existing.enabled, req.params.id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/automessages/:id', requireAdmin, (req, res) => {
  try { 
    db.prepare('DELETE FROM automessages WHERE id = ?').run(req.params.id); 
    logAudit(req.user.id, 'AUTOMESSAGE_DELETE', `Deleted automessage ${req.params.id}`, req.ip);
    res.json({ ok: true }); 
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- FILTERS ----------
app.get('/api/filters', requireAdmin, (req, res) => {
  try { res.json(db.prepare('SELECT * FROM filter_words ORDER BY created_at DESC').all()); } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/filters', requireAdmin, (req, res) => {
  const { word, action, enabled, duration_ms } = req.body;
  if (!word) return res.status(400).json({ error: 'Missing word' });
  
  const validActions = ['REPLACE', 'BLOCK', 'WARN', 'MUTE', 'BAN', 'KICK'];
  const filterAction = validActions.includes(action) ? action : 'REPLACE';
  
  let finalDuration = 0;
  if (filterAction === 'MUTE') finalDuration = duration_ms || 300000;
  else if (filterAction === 'BAN') finalDuration = duration_ms || 86400000;
  
  try {
    db.prepare('INSERT INTO filter_words (word, action, enabled, duration_ms) VALUES (?, ?, ?, ?)')
      .run(word, filterAction, enabled !== undefined ? (enabled ? 1 : 0) : 1, finalDuration);
    broadcastFilterUpdate();
    logAudit(req.user.id, 'FILTER_ADD', `Added filter: ${word} (${filterAction}, ${finalDuration}ms)`, req.ip);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/filters/:id', requireAdmin, (req, res) => {
  const { word, action, enabled, duration_ms } = req.body;
  try {
    const existing = db.prepare('SELECT * FROM filter_words WHERE id = ?').get(req.params.id);
    if (!existing) return res.status(404).json({ error: 'Filter not found' });
    
    const validActions = ['REPLACE', 'BLOCK', 'WARN', 'MUTE', 'BAN', 'KICK'];
    const filterAction = validActions.includes(action) ? action : existing.action;
    
    let finalDuration = duration_ms !== undefined ? duration_ms : existing.duration_ms;
    if (filterAction === 'MUTE' && (!finalDuration || finalDuration <= 0)) finalDuration = 300000;
    if (filterAction === 'BAN' && (!finalDuration || finalDuration <= 0)) finalDuration = 86400000;
    if (filterAction === 'REPLACE' || filterAction === 'BLOCK' || filterAction === 'WARN' || filterAction === 'KICK') finalDuration = 0;
    
    db.prepare('UPDATE filter_words SET word=?, action=?, enabled=?, duration_ms=? WHERE id=?')
      .run(word || existing.word, filterAction, enabled !== undefined ? (enabled ? 1 : 0) : existing.enabled, finalDuration, req.params.id);
    
    broadcastFilterUpdate();
    logAudit(req.user.id, 'FILTER_EDIT', `Edited filter ${req.params.id}`, req.ip);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/filters/:id', requireAdmin, (req, res) => {
  try {
    db.prepare('DELETE FROM filter_words WHERE id = ?').run(req.params.id);
    broadcastFilterUpdate();
    logAudit(req.user.id, 'FILTER_DELETE', `Deleted filter ${req.params.id}`, req.ip);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- ADMINS ----------
app.get('/api/admins', requireAdmin, (req, res) => {
  try { 
    res.json(db.prepare('SELECT id, username, minecraft_nick, notify_minecraft, notify_webpush, notify_discord, created_at FROM admins').all()); 
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admins', requireAdmin, (req, res) => {
  const { username, password, minecraft_nick, notify_minecraft, notify_webpush, notify_discord } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Missing fields' });
  if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters' });
  const hash = bcrypt.hashSync(password, 10);
  try {
    db.prepare('INSERT INTO admins (username, password_hash, minecraft_nick, notify_minecraft, notify_webpush, notify_discord) VALUES (?, ?, ?, ?, ?, ?)')
      .run(username, hash, minecraft_nick || '', notify_minecraft ? 1 : 0, notify_webpush ? 1 : 0, notify_discord ? 1 : 0);
    logAudit(req.user.id, 'ADMIN_ADD', `Added admin ${username}`, req.ip);
    res.json({ ok: true });
  } catch (e) {
    console.error('[ERROR] Adding admin:', e.message);
    if (e.message.includes('UNIQUE')) {
      return res.status(400).json({ error: 'Username already exists' });
    }
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/admins/:id', requireAdmin, (req, res) => {
  const adminId = parseInt(req.params.id);
  if (adminId === req.user.id) return res.status(400).json({ error: 'Cannot delete yourself' });
  try {
    db.prepare('DELETE FROM admins WHERE id = ?').run(adminId);
    logAudit(req.user.id, 'ADMIN_DELETE', `Deleted admin ${adminId}`, req.ip);
    res.json({ ok: true });
  } catch (e) { 
    console.error('[ERROR] Deleting admin:', e.message);
    res.status(500).json({ error: e.message }); 
  }
});

// ---------- SETTINGS ----------
app.get('/api/settings', requireAdmin, (req, res) => {
  try {
    const settings = db.prepare('SELECT * FROM settings').all();
    const obj = {};
    settings.forEach(s => obj[s.key] = s.value);
    res.json(obj);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/settings', requireAdmin, (req, res) => {
  try {
    for (const [key, value] of Object.entries(req.body)) {
      db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').run(key, value);
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- MESSAGE FILES ----------
app.get('/api/messages/files', requireAdmin, (req, res) => {
  try {
    const allData = JSON.parse(fs.readFileSync(MESSAGES_FILE, 'utf8'));
    const readData = JSON.parse(fs.readFileSync(MESSAGES_READ_FILE, 'utf8'));
    res.json({
      messages_file: { total: allData.total || 0, size_kb: Math.round(fs.statSync(MESSAGES_FILE).size / 1024) },
      messages_read_file: { date: readData.date, total: readData.total || 0, size_kb: Math.round(fs.statSync(MESSAGES_READ_FILE).size / 1024) }
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/messages/file/:type', requireAdmin, (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
  try {
    if (req.params.type === 'all') {
      const data = JSON.parse(fs.readFileSync(MESSAGES_FILE, 'utf8'));
      res.json({ file: 'messages.json', messages: data.messages.slice(-limit).reverse(), total: data.total });
    } else if (req.params.type === 'today') {
      const data = JSON.parse(fs.readFileSync(MESSAGES_READ_FILE, 'utf8'));
      res.json({ file: 'messages-read.json', date: data.date, messages: data.messages.slice(-limit).reverse(), total: data.total });
    } else res.status(400).json({ error: 'Invalid type' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/messages/backups', requireAdmin, (req, res) => {
  try {
    const files = fs.readdirSync(BACKUP_DIR).filter(f => f.endsWith('.json')).map(f => ({
      name: f,
      size_kb: Math.round(fs.statSync(path.join(BACKUP_DIR, f)).size / 1024)
    }));
    res.json(files);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/messages/db-backups', requireAdmin, (req, res) => {
  try {
    const files = fs.readdirSync(DB_BACKUP_DIR).filter(f => f.endsWith('.db')).map(f => ({
      name: f,
      size_kb: Math.round(fs.statSync(path.join(DB_BACKUP_DIR, f)).size / 1024)
    }));
    res.json(files);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- SERVICE WORKER ----------
app.get('/sw.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(`
self.addEventListener('push', function(event) {
  let data = { title: 'Neptune Admin', body: 'New notification' };
  if (event.data) {
    try { data = event.data.json(); } catch (e) { data.body = event.data.text(); }
  }
  event.waitUntil(self.registration.showNotification(data.title, { body: data.body }));
});
self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  event.waitUntil(clients.openWindow('/'));
});
  `);
});

// ---------- DISCORD ----------
async function sendDiscordWebhook(url, embed) {
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ embeds: [embed] })
    });
    if (!response.ok) console.error('[ERROR] Discord webhook failed:', response.status);
  } catch (e) {
    console.error('[ERROR] Discord webhook error:', e.message);
  }
}

// ---------- WEB PUSH ----------
function setupWebPush() {
  const publicKey = getSetting('web_push_public_key');
  const privateKey = getSetting('web_push_private_key');
  if (publicKey && privateKey) {
    webpush.setVapidDetails('mailto:admin@example.com', publicKey, privateKey);
  }
}
setupWebPush();

async function sendWebPush(subscription, payload) {
  try {
    await webpush.sendNotification(subscription, JSON.stringify(payload));
  } catch (e) {
    console.error('[ERROR] Web Push failed:', e.message);
  }
}

async function sendAdminNotifications(action, playerName, reason, serverId) {
  const admins = db.prepare('SELECT * FROM admins').all();
  for (const admin of admins) {
    if (admin.notify_webpush) {
      const subs = db.prepare('SELECT * FROM subscriptions WHERE admin_id = ?').all(admin.id);
      for (const sub of subs) {
        const subscription = { endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } };
        sendWebPush(subscription, {
          title: '🚨 Neptune Admin',
          body: `${playerName} został ${action.toLowerCase()}.\nSerwer: ${serverId}\nPowód: ${reason || 'Brak'}`
        });
      }
    }
  }
}

// ---------- RCON ----------
function sendRCON(serverId, command) {
  return new Promise((resolve, reject) => {
    const serverRecord = db.prepare('SELECT * FROM servers WHERE id = ?').get(serverId);
    if (!serverRecord || !serverRecord.rcon_enabled) return reject(new Error('RCON not enabled'));
    const password = decryptSecret(serverRecord.rcon_password_encrypted);
    if (!password) return reject(new Error('Failed to decrypt RCON password'));
    const socket = new net.Socket();
    let requestId = Math.floor(Math.random() * 100000);
    let response = '';
    let authenticated = false;
    
    socket.connect(serverRecord.rcon_port, serverRecord.ip, () => {
      socket.write(createRconPacket(requestId, 3, password));
    });
    
    socket.on('data', (data) => {
      if (!authenticated && data.length >= 12) {
        const type = data.readInt32LE(4);
        if (type === 2) {
          authenticated = true;
          requestId++;
          socket.write(createRconPacket(requestId, 2, command));
        }
      } else if (authenticated && data.length >= 12) {
        const id = data.readInt32LE(0);
        const type = data.readInt32LE(4);
        if (type === 0 && id === requestId) {
          response = data.slice(12).toString('utf8').trim();
          socket.destroy();
          resolve(response);
        }
      }
    });
    
    socket.on('error', reject);
    socket.setTimeout(5000, () => reject(new Error('RCON timeout')));
  });
}

function createRconPacket(id, type, payload) {
  const length = Buffer.byteLength(payload, 'utf8') + 10;
  const buffer = Buffer.alloc(length + 4);
  buffer.writeInt32LE(length, 0);
  buffer.writeInt32LE(id, 4);
  buffer.writeInt32LE(type, 8);
  buffer.write(payload, 12, 'utf8');
  buffer.writeInt16LE(0, 12 + Buffer.byteLength(payload, 'utf8'));
  return buffer;
}

app.post('/api/servers/:id/rcon', requireAdmin, async (req, res) => {
  const { command } = req.body;
  if (!command) return res.status(400).json({ error: 'Missing command' });
  try {
    const response = await sendRCON(req.params.id, command);
    logAudit(req.user.id, 'RCON_COMMAND', `RCON: ${command}`, req.ip);
    res.json({ ok: true, response });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------- ERROR HANDLING ----------
app.use('/api/', (req, res) => {
  res.status(404).json({ error: 'API endpoint not found' });
});

app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// ---------- HEARTBEAT ----------
setInterval(() => {
  const now = Date.now();
  for (const [serverId, conn] of pluginConnections) {
    if (conn.connected && now - conn.lastSeen > 120000) {
      console.log(`[HEARTBEAT] Timeout for ${serverId}`);
      conn.ws.terminate();
      conn.connected = false;
      broadcastToPanel({ type: 'SERVER_STATUS', server_id: serverId, status: 'OFFLINE' });
    }
  }
}, 30000);

// ---------- GRACEFUL SHUTDOWN ----------
function shutdown(signal) {
  console.log(`[SHUTDOWN] ${signal} received, closing...`);
  for (const conn of pluginConnections.values()) {
    try { conn.ws.close(1001, 'Server shutting down'); } catch (e) {}
  }
  for (const client of panelClients) {
    try { client.close(1001, 'Server shutting down'); } catch (e) {}
  }
  server.close(() => {
    console.log('[SHUTDOWN] Server closed');
    try { db.close(); } catch (e) {}
    process.exit(0);
  });
  setTimeout(() => {
    console.log('[SHUTDOWN] Force exit');
    process.exit(1);
  }, 5000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// ---------- START ----------
server.listen(PORT, HOST, () => {
  console.log(`[INFO] Neptune Admin running on http://${HOST}:${PORT}`);
  console.log(`[INFO] WebSocket paths: /ws-plugin, /ws-panel`);
  console.log(`[INFO] Message files: save/messages.json, save/messages-read.json`);
  console.log(`[INFO] Backup dir: save/backups/ (DB: save/backups/db/)`);
  console.log(`[INFO] Healthcheck: GET /health`);
});
