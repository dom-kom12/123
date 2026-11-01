const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const bcrypt = require('bcryptjs');

const DB_PATH = process.env.DATABASE_PATH || './database.db';
const db = new Database(DB_PATH);

db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// Initialize tables
db.exec(`
CREATE TABLE IF NOT EXISTS admins (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  minecraft_nick TEXT,
  notify_minecraft INTEGER DEFAULT 1,
  notify_webpush INTEGER DEFAULT 0,
  notify_discord INTEGER DEFAULT 0,
  created_at INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS servers (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  ip TEXT NOT NULL,
  port INTEGER NOT NULL,
  rcon_enabled INTEGER DEFAULT 0,
  rcon_port INTEGER,
  rcon_password_encrypted TEXT,
  query_enabled INTEGER DEFAULT 0,
  query_port INTEGER,
  api_token TEXT NOT NULL UNIQUE,
  created_at INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS players (
  uuid TEXT PRIMARY KEY,
  name TEXT,
  first_seen INTEGER,
  last_seen INTEGER
);

CREATE TABLE IF NOT EXISTS punishments (
  id TEXT PRIMARY KEY,
  player_uuid TEXT,
  player_name TEXT,
  action TEXT NOT NULL,
  moderator TEXT,
  reason TEXT,
  server_id TEXT,
  source TEXT,
  duration_ms INTEGER,
  expires_at INTEGER,
  created_at INTEGER,
  active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS warnings (
  id TEXT PRIMARY KEY,
  player_uuid TEXT,
  player_name TEXT,
  reason TEXT,
  moderator TEXT,
  server_id TEXT,
  source TEXT,
  created_at INTEGER,
  expires_at INTEGER
);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  server_id TEXT,
  player_uuid TEXT,
  player_name TEXT,
  message TEXT,
  timestamp INTEGER
);

CREATE TABLE IF NOT EXISTS automessages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  content TEXT,
  interval_seconds INTEGER,
  enabled INTEGER DEFAULT 1,
  servers TEXT,
  created_at INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS filter_words (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  word TEXT NOT NULL,
  action TEXT DEFAULT 'REPLACE',
  enabled INTEGER DEFAULT 1,
  duration_ms INTEGER DEFAULT 300000,
  created_at INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  admin_id INTEGER,
  endpoint TEXT,
  p256dh TEXT,
  auth TEXT,
  created_at INTEGER DEFAULT (strftime('%s','now')),
  FOREIGN KEY (admin_id) REFERENCES admins(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS audit_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  admin_id INTEGER,
  action TEXT,
  details TEXT,
  ip TEXT,
  timestamp INTEGER DEFAULT (strftime('%s','now'))
);
`);

// Migration: add duration_ms column to filter_words if it doesn't exist
try {
  const columns = db.prepare("PRAGMA table_info(filter_words)").all();
  const hasDuration = columns.some(c => c.name === 'duration_ms');
  if (!hasDuration) {
    db.exec(`ALTER TABLE filter_words ADD COLUMN duration_ms INTEGER DEFAULT 300000;`);
    console.log('[DB] Migration: added duration_ms column to filter_words');
  }
} catch (e) {
  console.error('[DB] Migration error:', e.message);
}

// Indexes
db.exec(`
CREATE INDEX IF NOT EXISTS idx_punishments_player ON punishments(player_uuid);
CREATE INDEX IF NOT EXISTS idx_punishments_action ON punishments(action);
CREATE INDEX IF NOT EXISTS idx_punishments_created ON punishments(created_at);
CREATE INDEX IF NOT EXISTS idx_warnings_player ON warnings(player_uuid);
CREATE INDEX IF NOT EXISTS idx_messages_server ON messages(server_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_logs_timestamp ON audit_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_filter_words_enabled ON filter_words(enabled);
`);

// Default settings
const defaultSettings = [
  ['discord_webhook_url', ''],
  ['web_push_public_key', ''],
  ['web_push_private_key', ''],
  ['session_timeout_minutes', '60'],
];

const insertSetting = db.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)');
defaultSettings.forEach(([key, value]) => insertSetting.run(key, value));

// Default admin
const adminExists = db.prepare('SELECT id FROM admins WHERE username = ?').get('admin');
if (!adminExists) {
  const hash = bcrypt.hashSync('admin123', 10);
  db.prepare('INSERT INTO admins (username, password_hash, minecraft_nick) VALUES (?, ?, ?)')
    .run('admin', hash, 'Admin');
  console.log('[DB] Created default admin account: admin / admin123');
}

module.exports = {
  db,
  getSetting: (key) => {
    const row = db.prepare('SELECT value FROM settings WHERE key = ?').get(key);
    return row ? row.value : null;
  },
  setSetting: (key, value) => {
    db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').run(key, value);
  },
  logPunishment: (punishment) => {
    const stmt = db.prepare(`
      INSERT INTO punishments (id, player_uuid, player_name, action, moderator, reason, server_id, source, duration_ms, expires_at, created_at, active)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    return stmt.run(
      punishment.id, punishment.player_uuid, punishment.player_name, punishment.action,
      punishment.moderator, punishment.reason, punishment.server_id, punishment.source,
      punishment.duration_ms || 0, punishment.expires_at || null,
      punishment.created_at || Math.floor(Date.now()/1000),
      punishment.active !== undefined ? punishment.active : 1
    );
  },
  logWarning: (warning) => {
    const stmt = db.prepare(`
      INSERT INTO warnings (id, player_uuid, player_name, reason, moderator, server_id, source, created_at, expires_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    return stmt.run(
      warning.id, warning.player_uuid, warning.player_name, warning.reason,
      warning.moderator, warning.server_id, warning.source,
      warning.created_at || Math.floor(Date.now()/1000), warning.expires_at || null
    );
  },
  logMessage: (serverId, playerUuid, playerName, message, timestamp) => {
    db.prepare('INSERT INTO messages (server_id, player_uuid, player_name, message, timestamp) VALUES (?, ?, ?, ?, ?)')
      .run(serverId, playerUuid, playerName, message, timestamp);
  },
  logAudit: (adminId, action, details, ip) => {
    db.prepare('INSERT INTO audit_logs (admin_id, action, details, ip) VALUES (?, ?, ?, ?)')
      .run(adminId, action, details, ip || null);
  }
};
