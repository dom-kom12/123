package com.neptune.chat;

import net.luckperms.api.LuckPerms;
import net.luckperms.api.model.user.User;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.OfflinePlayer;
import org.bukkit.Sound;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerChatEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;
import org.bukkit.scheduler.BukkitTask;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NeptuneChat extends JavaPlugin implements Listener {
    private static NeptuneChat instance;
    private LuckPerms luckPerms;
    private FileConfiguration config;

    // ========== LOCAL DATABASE ==========
    private Connection localDb;
    private final Object dbLock = new Object();

    // ========== WEBSOCKET ==========
    private WebSocketClient wsClient;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final Queue<Map<String, Object>> eventQueue = new ConcurrentLinkedQueue<>();
    private int reconnectAttempts = 0;

    // ========== STATE ==========
    private final Set<UUID> mutedPlayers = ConcurrentHashMap.newKeySet();
    private final Map<UUID, MuteInfo> activeMutes = new ConcurrentHashMap<>();

    // ========== BANS ==========
    private final Set<UUID> bannedPlayers = ConcurrentHashMap.newKeySet();
    private final Map<UUID, BanInfo> activeBans = new ConcurrentHashMap<>();

    private final Map<UUID, List<WarnInfo>> warnings = new ConcurrentHashMap<>();
    private final Map<UUID, Integer> capsWarnings = new ConcurrentHashMap<>();
    private final Map<UUID, Deque<Long>> messageTimestamps = new ConcurrentHashMap<>();
    private final Map<UUID, String> lastMessages = new ConcurrentHashMap<>();
    private final Set<String> filterWords = ConcurrentHashMap.newKeySet();
    private final Map<String, FilterConfig> filterConfigs = new ConcurrentHashMap<>();
    private final Map<UUID, String> playerColors = new ConcurrentHashMap<>();
    private final Map<UUID, String> playerNames = new ConcurrentHashMap<>();

    // ========== SLOWMODE ==========
    private final Map<UUID, Long> lastMessageTime = new ConcurrentHashMap<>();
    private int slowmodeSeconds = 0;

    // ========== VPN CACHE ==========
    private final Map<String, VpnCacheEntry> vpnCache = new ConcurrentHashMap<>();
    private record VpnCacheEntry(boolean isVpn, long checkedAt) {}

    // ========== AUTORESPONDER ==========
    private final Map<String, Long> autoResponderCooldowns = new ConcurrentHashMap<>();
    private record AutoResponder(String trigger, String response, int cooldownSeconds) {}

    // ========== NOTES / TAGS ==========
    private final Map<UUID, List<NoteInfo>> notes = new ConcurrentHashMap<>();
    private final Map<UUID, Set<String>> tags = new ConcurrentHashMap<>();
    private record NoteInfo(String id, String text, String author, long createdAt) {}

    // ========== PLAYTIME / SEEN ==========
    private final Map<UUID, Long> playerFirstSeen = new ConcurrentHashMap<>();
    private final Map<UUID, Long> playerLastSeen = new ConcurrentHashMap<>();
    private final Map<UUID, Long> playerPlaytime = new ConcurrentHashMap<>();
    private final Map<UUID, Long> playerSessionStart = new ConcurrentHashMap<>();

    // ========== CHAT LOCK ==========
    private boolean chatLocked = false;
    private long chatLockUntil = 0;
    private BukkitTask chatLockTask;
    private String chatLockReason = "";

    // ========== AUTOMESSAGES ==========
    private BukkitTask autoMessageTask;
    private final List<AutoMessage> autoMessages = new CopyOnWriteArrayList<>();
    private int autoMessageIndex = 0;

    // ========== TASKS ==========
    private BukkitTask heartbeatTask;
    private BukkitTask reconnectTask;
    private BukkitTask muteCheckTask;
    private BukkitTask banCheckTask;
    private BukkitTask autoSaveTask;

    // ========== RECORDS ==========
    private record MuteInfo(long until, String reason, String moderator) {}
    private record BanInfo(long until, String reason, String moderator) {}
    private record WarnInfo(String id, String reason, String moderator, long createdAt) {}
    private record FilterConfig(String word, String action, long durationMs) {}
    private record AutoMessage(String name, String content, int intervalSeconds, boolean enabled) {}

    // ========== PATTERNS ==========
    private static final Pattern URL_PATTERN = Pattern.compile(
            "(https?://|www\\.)[\\w\\-]+\\.[a-z]{2,}[^\\s]*", Pattern.CASE_INSENSITIVE);
    private static final Pattern IP_PATTERN = Pattern.compile(
            "\\b(?:\\d{1,3}\\.){3}\\d{1,3}(?::\\d{1,5})?\\b");
    private static final Pattern HEX_PATTERN = Pattern.compile("&#([A-Fa-f0-9]{6})");
    private static final Pattern GRADIENT_PATTERN = Pattern.compile(
            "<gradient:#([A-Fa-f0-9]{6}):#([A-Fa-f0-9]{6})>([^<]+)</gradient>");
    private static final Pattern INVISIBLE_PATTERN = Pattern.compile(
            "[\\u200B-\\u200F\\u2028-\\u202F\\u2060-\\u206F\\uFEFF]");
    private static final Pattern SPOOFING_PATTERN = Pattern.compile("[\\u0400-\\u04FF]");
    private static final Pattern MENTION_PATTERN = Pattern.compile("@([A-Za-z0-9_]{3,16})");

    @Override
    public void onEnable() {
        instance = this;
        saveDefaultConfig();
        config = getConfig();

        initLocalDatabase();

        if (getServer().getPluginManager().getPlugin("LuckPerms") != null) {
            try {
                luckPerms = net.luckperms.api.LuckPermsProvider.get();
                getLogger().info("LuckPerms found.");
            } catch (Exception e) {
                getLogger().warning("LuckPerms found but API unavailable: " + e.getMessage());
            }
        } else {
            getLogger().warning("LuckPerms not found! Using OP checks.");
        }

        getServer().getPluginManager().registerEvents(this, this);
        registerCommands();

        loadFiltersFromConfig();
        loadChatLockState();
        loadWarningsFromDb();
        loadBansFromDb();
        loadPlayerColorsFromDb();
        loadPlayerNamesFromDb();
        loadAutoMessagesFromConfig();
        loadNotesFromDb();
        loadTagsFromDb();
        loadPlaytimeFromDb();

        chatLocked = config.getBoolean("chat.lock-default", false);
        slowmodeSeconds = config.getInt("chat.slowmode-seconds", 0);

        if (Bukkit.getPluginManager().getPlugin("PlaceholderAPI") != null) {
            try {
                new NeptuneExpansion(this).register();
                getLogger().info("PlaceholderAPI expansion registered.");
            } catch (Throwable t) {
                getLogger().warning("Failed to register PAPI expansion: " + t.getMessage());
            }
        }

        if (config.getBoolean("metrics", true)) {
            try {
                new org.bstats.bukkit.Metrics(this, 25000);
                getLogger().info("bStats enabled.");
            } catch (Throwable t) {
                getLogger().warning("bStats failed: " + t.getMessage());
            }
        }

        if (config.getBoolean("backend.enabled", true)) {
            wsClient = new WebSocketClient();
            wsClient.connect();
            startHeartbeat();
        }

        muteCheckTask = new BukkitRunnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                Iterator<Map.Entry<UUID, MuteInfo>> it = activeMutes.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<UUID, MuteInfo> entry = it.next();
                    if (entry.getValue().until() != Long.MAX_VALUE && entry.getValue().until() <= now) {
                        mutedPlayers.remove(entry.getKey());
                        it.remove();
                        String name = playerNames.getOrDefault(entry.getKey(), "Unknown");
                        sendActionEvent("UNMUTE", entry.getKey(), name, "Mute expired", "SYSTEM");
                    }
                }
            }
        }.runTaskTimerAsynchronously(this, 20L * 30, 20L * 30);

        banCheckTask = new BukkitRunnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                Iterator<Map.Entry<UUID, BanInfo>> it = activeBans.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<UUID, BanInfo> entry = it.next();
                    if (entry.getValue().until() != Long.MAX_VALUE && entry.getValue().until() <= now) {
                        bannedPlayers.remove(entry.getKey());
                        deleteBanFromDb(entry.getKey());
                        it.remove();
                        String name = playerNames.getOrDefault(entry.getKey(), "Unknown");
                        sendActionEvent("UNBAN", entry.getKey(), name, "Ban expired", "SYSTEM");
                    }
                }
            }
        }.runTaskTimerAsynchronously(this, 20L * 30, 20L * 30);

        autoSaveTask = new BukkitRunnable() {
            @Override
            public void run() {
                savePlayerColorsToDb();
                savePlayerNamesToDb();
                savePlaytimeToDb();
            }
        }.runTaskTimerAsynchronously(this, 20L * 300, 20L * 300);

        getLogger().info("NeptuneChat v" + getDescription().getVersion() + " enabled. Filter words: " + filterWords.size());
    }

    @Override
    public void onDisable() {
        for (Player p : Bukkit.getOnlinePlayers()) {
            Long start = playerSessionStart.get(p.getUniqueId());
            if (start != null) {
                playerPlaytime.merge(p.getUniqueId(), System.currentTimeMillis() - start, Long::sum);
            }
        }

        savePlayerColorsToDb();
        savePlayerNamesToDb();
        saveChatLockState();
        savePlaytimeToDb();

        if (wsClient != null) wsClient.close();
        if (autoMessageTask != null) autoMessageTask.cancel();
        if (heartbeatTask != null) heartbeatTask.cancel();
        if (reconnectTask != null) reconnectTask.cancel();
        if (chatLockTask != null) chatLockTask.cancel();
        if (muteCheckTask != null) muteCheckTask.cancel();
        if (banCheckTask != null) banCheckTask.cancel();
        if (autoSaveTask != null) autoSaveTask.cancel();

        try {
            if (localDb != null && !localDb.isClosed()) {
                localDb.close();
            }
        } catch (SQLException e) {
            getLogger().warning("Error closing local DB: " + e.getMessage());
        }

        getLogger().info("NeptuneChat disabled.");
    }

    // ============================================================
    //                   PUBLIC API (PlaceholderAPI)
    // ============================================================
    public boolean isMuted(UUID uuid) { return mutedPlayers.contains(uuid); }
    public String getMuteTimeFormatted(UUID uuid) {
        MuteInfo info = activeMutes.get(uuid);
        if (info == null) return "";
        return info.until() == Long.MAX_VALUE ? "perm" : formatDuration(info.until() - System.currentTimeMillis());
    }
    public boolean isBanned(UUID uuid) { return bannedPlayers.contains(uuid); }
    public String getBanTimeFormatted(UUID uuid) {
        BanInfo info = activeBans.get(uuid);
        if (info == null) return "";
        return info.until() == Long.MAX_VALUE ? "perm" : formatDuration(info.until() - System.currentTimeMillis());
    }
    public int getWarningsCount(UUID uuid) { return warnings.getOrDefault(uuid, List.of()).size(); }
    public String getRawColor(UUID uuid) { return playerColors.getOrDefault(uuid, "&f"); }
    public boolean isChatLocked() { return chatLocked; }
    public int getSlowmodeSeconds() { return slowmodeSeconds; }
    public int getNotesCount(UUID uuid) { return notes.getOrDefault(uuid, List.of()).size(); }
    public int getTagsCount(UUID uuid) { return tags.getOrDefault(uuid, Set.of()).size(); }
    public String getPlaytimeFormatted(UUID uuid) {
        Long pt = playerPlaytime.get(uuid);
        if (pt == null) return "0s";
        return formatDuration(pt);
    }
    public String getLastSeenFormatted(UUID uuid) {
        Long ls = playerLastSeen.get(uuid);
        if (ls == null) return "nigdy";
        long diff = System.currentTimeMillis() - ls;
        if (diff < 60000) return "teraz";
        return formatDuration(diff) + " temu";
    }

    // ============================================================
    //                   LOCAL DATABASE
    // ============================================================
    private void initLocalDatabase() {
        try {
            File dbFile = new File(getDataFolder(), "localdata.db");
            if (!getDataFolder().exists()) getDataFolder().mkdirs();

            localDb = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());

            try (Statement stmt = localDb.createStatement()) {
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS player_colors (" +
                        "uuid TEXT PRIMARY KEY, player_name TEXT, color TEXT NOT NULL, updated_at INTEGER)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS player_names (" +
                        "uuid TEXT PRIMARY KEY, player_name TEXT NOT NULL, first_seen INTEGER, last_seen INTEGER)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS warnings (" +
                        "id TEXT PRIMARY KEY, player_uuid TEXT, player_name TEXT, reason TEXT, " +
                        "moderator TEXT, source TEXT, created_at INTEGER)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS history (" +
                        "id TEXT PRIMARY KEY, player_uuid TEXT, player_name TEXT, action TEXT, " +
                        "reason TEXT, moderator TEXT, source TEXT, created_at INTEGER)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS notes (" +
                        "id TEXT PRIMARY KEY, player_uuid TEXT, text TEXT, author TEXT, created_at INTEGER)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS tags (" +
                        "player_uuid TEXT, tag TEXT, added_by TEXT, created_at INTEGER, PRIMARY KEY(player_uuid, tag))");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS playtime (" +
                        "uuid TEXT PRIMARY KEY, playtime_ms INTEGER, first_seen INTEGER, last_seen INTEGER)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS bans (" +
                        "uuid TEXT PRIMARY KEY, player_name TEXT, reason TEXT, " +
                        "moderator TEXT, source TEXT, until_ms INTEGER, created_at INTEGER)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS chat_state (key TEXT PRIMARY KEY, value TEXT)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)");

                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_colors_uuid ON player_colors(uuid)");
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_names_uuid ON player_names(uuid)");
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_warnings_uuid ON warnings(player_uuid)");
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_history_uuid ON history(player_uuid)");
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_notes_uuid ON notes(player_uuid)");
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_tags_uuid ON tags(player_uuid)");
                stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_bans_uuid ON bans(uuid)");
            }

            getLogger().info("Local database initialized: " + dbFile.getAbsolutePath());
        } catch (SQLException e) {
            getLogger().severe("Failed to initialize local database: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ---------- PLAYER COLORS ----------
    private void loadPlayerColorsFromDb() {
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT uuid, color FROM player_colors")) {
                int count = 0;
                while (rs.next()) {
                    try {
                        UUID uuid = UUID.fromString(rs.getString("uuid"));
                        String color = rs.getString("color");
                        if (color != null && !color.isEmpty()) {
                            playerColors.put(uuid, color);
                            count++;
                        }
                    } catch (Exception e) {
                        getLogger().warning("Bad color row: " + e.getMessage());
                    }
                }
                getLogger().info("Loaded " + count + " player colors from local DB.");
            } catch (SQLException e) {
                getLogger().warning("Failed to load player colors: " + e.getMessage());
            }
        }
    }

    private void savePlayerColorsToDb() {
        synchronized (dbLock) {
            try {
                localDb.setAutoCommit(false);
                try (PreparedStatement ps = localDb.prepareStatement(
                        "INSERT INTO player_colors (uuid, player_name, color, updated_at) VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT(uuid) DO UPDATE SET player_name=excluded.player_name, color=excluded.color, updated_at=excluded.updated_at")) {
                    long now = System.currentTimeMillis();
                    for (Map.Entry<UUID, String> entry : playerColors.entrySet()) {
                        ps.setString(1, entry.getKey().toString());
                        ps.setString(2, playerNames.getOrDefault(entry.getKey(), "Unknown"));
                        ps.setString(3, entry.getValue());
                        ps.setLong(4, now);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                localDb.commit();
            } catch (SQLException e) {
                try { localDb.rollback(); } catch (SQLException ignored) { }
                getLogger().warning("Failed to save player colors: " + e.getMessage());
            } finally {
                try { localDb.setAutoCommit(true); } catch (SQLException ignored) { }
            }
        }
    }

    private void savePlayerColorToDb(UUID uuid, String color) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "INSERT INTO player_colors (uuid, player_name, color, updated_at) VALUES (?, ?, ?, ?) " +
                            "ON CONFLICT(uuid) DO UPDATE SET player_name=excluded.player_name, color=excluded.color, updated_at=excluded.updated_at")) {
                ps.setString(1, uuid.toString());
                ps.setString(2, playerNames.getOrDefault(uuid, "Unknown"));
                ps.setString(3, color);
                ps.setLong(4, System.currentTimeMillis());
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to save player color: " + e.getMessage());
            }
        }
    }

    private void deletePlayerColorFromDb(UUID uuid) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement("DELETE FROM player_colors WHERE uuid = ?")) {
                ps.setString(1, uuid.toString());
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to delete player color: " + e.getMessage());
            }
        }
    }

    // ---------- PLAYER NAMES ----------
    private void loadPlayerNamesFromDb() {
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT uuid, player_name FROM player_names")) {
                int count = 0;
                while (rs.next()) {
                    try {
                        UUID uuid = UUID.fromString(rs.getString("uuid"));
                        String name = rs.getString("player_name");
                        if (name != null && !name.isEmpty()) {
                            playerNames.put(uuid, name);
                            count++;
                        }
                    } catch (Exception e) {
                        getLogger().warning("Bad name row: " + e.getMessage());
                    }
                }
                getLogger().info("Loaded " + count + " player names from local DB.");
            } catch (SQLException e) {
                getLogger().warning("Failed to load player names: " + e.getMessage());
            }
        }
    }

    private void savePlayerNamesToDb() {
        synchronized (dbLock) {
            try {
                localDb.setAutoCommit(false);
                try (PreparedStatement ps = localDb.prepareStatement(
                        "INSERT INTO player_names (uuid, player_name, first_seen, last_seen) VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT(uuid) DO UPDATE SET player_name=excluded.player_name, last_seen=excluded.last_seen")) {
                    long now = System.currentTimeMillis();
                    for (Map.Entry<UUID, String> entry : playerNames.entrySet()) {
                        ps.setString(1, entry.getKey().toString());
                        ps.setString(2, entry.getValue());
                        ps.setLong(3, now);
                        ps.setLong(4, now);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                localDb.commit();
            } catch (SQLException e) {
                try { localDb.rollback(); } catch (SQLException ignored) { }
                getLogger().warning("Failed to save player names: " + e.getMessage());
            } finally {
                try { localDb.setAutoCommit(true); } catch (SQLException ignored) { }
            }
        }
    }

    private void savePlayerNameToDb(UUID uuid, String name) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "INSERT INTO player_names (uuid, player_name, first_seen, last_seen) VALUES (?, ?, ?, ?) " +
                            "ON CONFLICT(uuid) DO UPDATE SET player_name=excluded.player_name, last_seen=excluded.last_seen")) {
                long now = System.currentTimeMillis();
                ps.setString(1, uuid.toString());
                ps.setString(2, name);
                ps.setLong(3, now);
                ps.setLong(4, now);
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to save player name: " + e.getMessage());
            }
        }
    }

    // ---------- WARNINGS ----------
    private void saveWarningToDb(WarnInfo warn, UUID playerUuid, String playerName) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "INSERT OR REPLACE INTO warnings (id, player_uuid, player_name, reason, moderator, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, warn.id());
                ps.setString(2, playerUuid.toString());
                ps.setString(3, playerName);
                ps.setString(4, warn.reason());
                ps.setString(5, warn.moderator());
                ps.setString(6, warn.moderator());
                ps.setLong(7, warn.createdAt());
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to save warning: " + e.getMessage());
            }
        }
    }

    private void loadWarningsFromDb() {
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM warnings ORDER BY created_at DESC")) {
                int count = 0;
                while (rs.next()) {
                    try {
                        UUID uuid = UUID.fromString(rs.getString("player_uuid"));
                        WarnInfo warn = new WarnInfo(
                                rs.getString("id"),
                                rs.getString("reason"),
                                rs.getString("moderator"),
                                rs.getLong("created_at")
                        );
                        warnings.computeIfAbsent(uuid, k -> new ArrayList<>()).add(warn);
                        count++;
                    } catch (Exception e) {
                        getLogger().warning("Bad warning row: " + e.getMessage());
                    }
                }
                getLogger().info("Loaded " + count + " warnings from local DB.");
            } catch (SQLException e) {
                getLogger().warning("Failed to load warnings: " + e.getMessage());
            }
        }
    }

    // ---------- BANS ----------
    private void loadBansFromDb() {
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM bans")) {
                long now = System.currentTimeMillis();
                int count = 0;
                while (rs.next()) {
                    try {
                        UUID uuid = UUID.fromString(rs.getString("uuid"));
                        long until = rs.getLong("until_ms");
                        if (until == Long.MAX_VALUE || until > now) {
                            activeBans.put(uuid, new BanInfo(until,
                                    rs.getString("reason"), rs.getString("moderator")));
                            bannedPlayers.add(uuid);
                            count++;
                        } else {
                            try (PreparedStatement ps = localDb.prepareStatement(
                                    "DELETE FROM bans WHERE uuid = ?")) {
                                ps.setString(1, uuid.toString());
                                ps.executeUpdate();
                            }
                        }
                    } catch (Exception e) {
                        getLogger().warning("Bad ban row: " + e.getMessage());
                    }
                }
                getLogger().info("Loaded " + count + " active bans from local DB.");
            } catch (SQLException e) {
                getLogger().warning("Failed to load bans: " + e.getMessage());
            }
        }
    }

    private void saveBanToDb(UUID uuid, String name, long until, String reason, String moderator) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "INSERT OR REPLACE INTO bans " +
                            "(uuid, player_name, reason, moderator, source, until_ms, created_at) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, uuid.toString());
                ps.setString(2, name);
                ps.setString(3, reason);
                ps.setString(4, moderator);
                ps.setString(5, moderator);
                ps.setLong(6, until);
                ps.setLong(7, System.currentTimeMillis());
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to save ban: " + e.getMessage());
            }
        }
    }

    private void deleteBanFromDb(UUID uuid) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "DELETE FROM bans WHERE uuid = ?")) {
                ps.setString(1, uuid.toString());
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to delete ban: " + e.getMessage());
            }
        }
    }

    // ---------- NOTES ----------
    private void loadNotesFromDb() {
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM notes ORDER BY created_at DESC")) {
                int count = 0;
                while (rs.next()) {
                    try {
                        UUID uuid = UUID.fromString(rs.getString("player_uuid"));
                        NoteInfo note = new NoteInfo(
                                rs.getString("id"),
                                rs.getString("text"),
                                rs.getString("author"),
                                rs.getLong("created_at")
                        );
                        notes.computeIfAbsent(uuid, k -> new ArrayList<>()).add(note);
                        count++;
                    } catch (Exception e) {
                        getLogger().warning("Bad note row: " + e.getMessage());
                    }
                }
                getLogger().info("Loaded " + count + " notes from local DB.");
            } catch (SQLException e) {
                getLogger().warning("Failed to load notes: " + e.getMessage());
            }
        }
    }

    private void saveNoteToDb(UUID uuid, NoteInfo note) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "INSERT OR REPLACE INTO notes (id, player_uuid, text, author, created_at) VALUES (?, ?, ?, ?, ?)")) {
                ps.setString(1, note.id());
                ps.setString(2, uuid.toString());
                ps.setString(3, note.text());
                ps.setString(4, note.author());
                ps.setLong(5, note.createdAt());
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to save note: " + e.getMessage());
            }
        }
    }

    private void deleteNoteFromDb(String noteId) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement("DELETE FROM notes WHERE id = ?")) {
                ps.setString(1, noteId);
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to delete note: " + e.getMessage());
            }
        }
    }

    // ---------- TAGS ----------
    private void loadTagsFromDb() {
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT player_uuid, tag FROM tags")) {
                int count = 0;
                while (rs.next()) {
                    try {
                        UUID uuid = UUID.fromString(rs.getString("player_uuid"));
                        String tag = rs.getString("tag");
                        if (tag != null && !tag.isEmpty()) {
                            tags.computeIfAbsent(uuid, k -> ConcurrentHashMap.newKeySet()).add(tag);
                            count++;
                        }
                    } catch (Exception e) {
                        getLogger().warning("Bad tag row: " + e.getMessage());
                    }
                }
                getLogger().info("Loaded " + count + " tags from local DB.");
            } catch (SQLException e) {
                getLogger().warning("Failed to load tags: " + e.getMessage());
            }
        }
    }

    private void saveTagToDb(UUID uuid, String tag, String addedBy) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "INSERT OR REPLACE INTO tags (player_uuid, tag, added_by, created_at) VALUES (?, ?, ?, ?)")) {
                ps.setString(1, uuid.toString());
                ps.setString(2, tag);
                ps.setString(3, addedBy);
                ps.setLong(4, System.currentTimeMillis());
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to save tag: " + e.getMessage());
            }
        }
    }

    private void deleteTagFromDb(UUID uuid, String tag) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement("DELETE FROM tags WHERE player_uuid = ? AND tag = ?")) {
                ps.setString(1, uuid.toString());
                ps.setString(2, tag);
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to delete tag: " + e.getMessage());
            }
        }
    }

    // ---------- PLAYTIME ----------
    private void loadPlaytimeFromDb() {
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT uuid, playtime_ms, first_seen, last_seen FROM playtime")) {
                int count = 0;
                while (rs.next()) {
                    try {
                        UUID uuid = UUID.fromString(rs.getString("uuid"));
                        playerPlaytime.put(uuid, rs.getLong("playtime_ms"));
                        playerFirstSeen.put(uuid, rs.getLong("first_seen"));
                        playerLastSeen.put(uuid, rs.getLong("last_seen"));
                        count++;
                    } catch (Exception e) {
                        getLogger().warning("Bad playtime row: " + e.getMessage());
                    }
                }
                getLogger().info("Loaded " + count + " playtime records from local DB.");
            } catch (SQLException e) {
                getLogger().warning("Failed to load playtime: " + e.getMessage());
            }
        }
    }

    private void savePlaytimeToDb() {
        synchronized (dbLock) {
            try {
                localDb.setAutoCommit(false);
                try (PreparedStatement ps = localDb.prepareStatement(
                        "INSERT INTO playtime (uuid, playtime_ms, first_seen, last_seen) VALUES (?, ?, ?, ?) " +
                                "ON CONFLICT(uuid) DO UPDATE SET playtime_ms=excluded.playtime_ms, last_seen=excluded.last_seen")) {
                    for (Map.Entry<UUID, Long> entry : playerPlaytime.entrySet()) {
                        ps.setString(1, entry.getKey().toString());
                        ps.setLong(2, entry.getValue());
                        ps.setLong(3, playerFirstSeen.getOrDefault(entry.getKey(), System.currentTimeMillis()));
                        ps.setLong(4, playerLastSeen.getOrDefault(entry.getKey(), System.currentTimeMillis()));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                localDb.commit();
            } catch (SQLException e) {
                try { localDb.rollback(); } catch (SQLException ignored) { }
                getLogger().warning("Failed to save playtime: " + e.getMessage());
            } finally {
                try { localDb.setAutoCommit(true); } catch (SQLException ignored) { }
            }
        }
    }

    // ---------- HISTORY ----------
    private void saveHistory(UUID uuid, String name, String action, String reason, String moderator, String source) {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "INSERT INTO history (id, player_uuid, player_name, action, reason, moderator, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, UUID.randomUUID().toString());
                ps.setString(2, uuid.toString());
                ps.setString(3, name);
                ps.setString(4, action);
                ps.setString(5, reason);
                ps.setString(6, moderator);
                ps.setString(7, source);
                ps.setLong(8, System.currentTimeMillis());
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to save history: " + e.getMessage());
            }
        }
    }

    private List<String[]> loadHistory(UUID uuid, String filterAction) {
        List<String[]> list = new ArrayList<>();
        synchronized (dbLock) {
            String sql = "SELECT action, reason, moderator, source, created_at FROM history WHERE player_uuid = ?";
            if (filterAction != null && !filterAction.equalsIgnoreCase("all")) {
                sql += " AND action = ?";
            }
            sql += " ORDER BY created_at DESC LIMIT 100";
            try (PreparedStatement ps = localDb.prepareStatement(sql)) {
                ps.setString(1, uuid.toString());
                if (filterAction != null && !filterAction.equalsIgnoreCase("all")) {
                    ps.setString(2, filterAction.toUpperCase());
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        list.add(new String[]{
                                rs.getString("action"),
                                rs.getString("reason"),
                                rs.getString("moderator"),
                                rs.getString("source"),
                                String.valueOf(rs.getLong("created_at"))
                        });
                    }
                }
            } catch (SQLException e) {
                getLogger().warning("Failed to load history: " + e.getMessage());
            }
        }
        return list;
    }

    // ---------- CHAT STATE ----------
    private void saveChatLockState() {
        synchronized (dbLock) {
            try (PreparedStatement ps = localDb.prepareStatement(
                    "INSERT OR REPLACE INTO chat_state (key, value) VALUES (?, ?)")) {
                ps.setString(1, "locked");
                ps.setString(2, String.valueOf(chatLocked));
                ps.executeUpdate();
                ps.setString(1, "until");
                ps.setString(2, String.valueOf(chatLockUntil));
                ps.executeUpdate();
                ps.setString(1, "reason");
                ps.setString(2, chatLockReason != null ? chatLockReason : "");
                ps.executeUpdate();
            } catch (SQLException e) {
                getLogger().warning("Failed to save chat lock state: " + e.getMessage());
            }
        }
    }

    private void loadChatLockState() {
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT key, value FROM chat_state")) {
                while (rs.next()) {
                    String key = rs.getString("key");
                    String value = rs.getString("value");
                    switch (key) {
                        case "locked" -> chatLocked = Boolean.parseBoolean(value);
                        case "until" -> { try { chatLockUntil = Long.parseLong(value); } catch (Exception ignored) { } }
                        case "reason" -> chatLockReason = value;
                    }
                }
            } catch (SQLException e) {
                getLogger().warning("Failed to load chat state: " + e.getMessage());
            }
        }
    }

    private Map<String, Integer> getDbStats() {
        Map<String, Integer> stats = new HashMap<>();
        synchronized (dbLock) {
            try (Statement stmt = localDb.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as c FROM player_colors");
                if (rs.next()) stats.put("player_colors", rs.getInt("c"));
                rs = stmt.executeQuery("SELECT COUNT(*) as c FROM player_names");
                if (rs.next()) stats.put("player_names", rs.getInt("c"));
                rs = stmt.executeQuery("SELECT COUNT(*) as c FROM warnings");
                if (rs.next()) stats.put("warnings", rs.getInt("c"));
                rs = stmt.executeQuery("SELECT COUNT(*) as c FROM history");
                if (rs.next()) stats.put("history", rs.getInt("c"));
                rs = stmt.executeQuery("SELECT COUNT(*) as c FROM notes");
                if (rs.next()) stats.put("notes", rs.getInt("c"));
                rs = stmt.executeQuery("SELECT COUNT(*) as c FROM tags");
                if (rs.next()) stats.put("tags", rs.getInt("c"));
                rs = stmt.executeQuery("SELECT COUNT(*) as c FROM bans");
                if (rs.next()) stats.put("bans", rs.getInt("c"));
            } catch (SQLException e) {
                getLogger().warning("Failed to get DB stats: " + e.getMessage());
            }
        }
        return stats;
    }

    // ============================================================
    //                       COMMANDS
    // ============================================================
    private void registerCommands() {
        setExecutor("mute", new MuteCommand());
        setExecutor("unmute", new UnmuteCommand());
        setExecutor("warn", new WarnCommand());
        setExecutor("warnings", new WarningsCommand());
        setExecutor("chat", new ChatCommand());
        setExecutor("neptune", new NeptuneCommand());
        setExecutor("history", new HistoryCommand());
    }

    private void setExecutor(String name, CommandExecutor executor) {
        var cmd = getCommand(name);
        if (cmd == null) {
            getLogger().warning("Command '" + name + "' not defined in plugin.yml!");
            return;
        }
        cmd.setExecutor(executor);
    }

    // ============================================================
    //                       FILTERS
    // ============================================================
    private void loadFiltersFromConfig() {
        List<String> configWords = config.getStringList("filter.words");
        for (String word : configWords) {
            if (word != null && !word.isEmpty()) {
                String lower = word.toLowerCase();
                filterWords.add(lower);
                filterConfigs.putIfAbsent(lower, new FilterConfig(word, "REPLACE", 0));
            }
        }
    }

    // ============================================================
    //                     AUTOMESSAGES
    // ============================================================
    private void loadAutoMessagesFromConfig() {
        if (!config.getBoolean("automessages.enabled", true)) return;

        List<String> messages = config.getStringList("automessages.messages");
        int interval = config.getInt("automessages.interval-seconds", 300);

        for (int i = 0; i < messages.size(); i++) {
            String content = messages.get(i);
            boolean exists = false;
            for (AutoMessage am : autoMessages) {
                if (am.content().equals(content)) { exists = true; break; }
            }
            if (!exists) {
                autoMessages.add(new AutoMessage("Config #" + (i + 1), content, interval, true));
            }
        }

        if (!autoMessages.isEmpty()) startAutoMessages();
    }

    private void startAutoMessages() {
        if (autoMessageTask != null) autoMessageTask.cancel();
        if (autoMessages.isEmpty()) return;

        autoMessageTask = new BukkitRunnable() {
            @Override
            public void run() {
                if (autoMessages.isEmpty()) return;

                List<AutoMessage> enabled = new ArrayList<>();
                for (AutoMessage am : autoMessages) {
                    if (am.enabled()) enabled.add(am);
                }
                if (enabled.isEmpty()) return;

                if (autoMessageIndex >= enabled.size()) autoMessageIndex = 0;
                AutoMessage msg = enabled.get(autoMessageIndex);
                autoMessageIndex++;

                broadcastFormatted(msg.content());
            }
        }.runTaskTimer(this, 20L * 60, 20L * 60);
    }

    private void reloadAutoMessages(List<Map<String, Object>> messagesList) {
        List<AutoMessage> configMessages = new ArrayList<>();
        for (AutoMessage am : autoMessages) {
            if (am.name().startsWith("Config #")) configMessages.add(am);
        }

        autoMessages.clear();
        autoMessages.addAll(configMessages);

        for (Map<String, Object> m : messagesList) {
            try {
                String name = (String) m.getOrDefault("name", "");
                String content = (String) m.get("content");
                Object intervalObj = m.get("interval_seconds");
                int interval = intervalObj instanceof Number ? ((Number) intervalObj).intValue() : 300;
                Object enabledObj = m.get("enabled");
                boolean enabled = enabledObj instanceof Number ? ((Number) enabledObj).intValue() == 1 : Boolean.TRUE.equals(enabledObj);

                if (content != null && !content.isEmpty()) {
                    autoMessages.add(new AutoMessage(name, content, interval, enabled));
                }
            } catch (Exception e) {
                getLogger().warning("Failed to parse automessage: " + e.getMessage());
            }
        }
        getLogger().info("AutoMessages synced: " + autoMessages.size() + " (config: " + configMessages.size() + ")");
        startAutoMessages();
    }

    // ============================================================
    //                    AUTORESPONDER
    // ============================================================
    private String checkAutoResponder(String message, Player player) {
        if (!config.getBoolean("autoresponder.enabled", false)) return null;
        ConfigurationSection section = config.getConfigurationSection("autoresponder.triggers");
        if (section == null) return null;

        String lower = message.toLowerCase();
        long now = System.currentTimeMillis();

        for (String key : section.getKeys(false)) {
            if (lower.contains(key.toLowerCase())) {
                long last = autoResponderCooldowns.getOrDefault(player.getUniqueId() + ":" + key, 0L);
                int cooldown = section.getInt(key + ".cooldown-seconds", 30);
                if (now - last < cooldown * 1000L) return null;
                autoResponderCooldowns.put(player.getUniqueId() + ":" + key, now);

                String response = section.getString(key + ".response", "");
                if (response.isEmpty()) return null;
                return response;
            }
        }
        return null;
    }

    // ============================================================
    //                        HELPERS
    // ============================================================
    private void broadcastFormatted(String message) {
        if (message == null) return;
        String colored = colorize(message);
        Bukkit.getScheduler().runTask(this, () -> Bukkit.broadcastMessage(colored));
    }

    private String colorize(String input) {
        if (input == null) return "";
        input = applyGradients(input);
        Matcher m = HEX_PATTERN.matcher(input);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String hex = m.group(1);
            StringBuilder replacement = new StringBuilder("§x");
            for (char c : hex.toCharArray()) replacement.append("§").append(c);
            m.appendReplacement(sb, replacement.toString());
        }
        m.appendTail(sb);
        return ChatColor.translateAlternateColorCodes('&', sb.toString());
    }

    private String applyGradients(String input) {
        Matcher matcher = GRADIENT_PATTERN.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            int sr = Integer.parseInt(matcher.group(1).substring(0,2), 16);
            int sg = Integer.parseInt(matcher.group(1).substring(2,4), 16);
            int sb2 = Integer.parseInt(matcher.group(1).substring(4,6), 16);
            int er = Integer.parseInt(matcher.group(2).substring(0,2), 16);
            int eg = Integer.parseInt(matcher.group(2).substring(2,4), 16);
            int eb = Integer.parseInt(matcher.group(2).substring(4,6), 16);
            matcher.appendReplacement(sb, applyGradient(matcher.group(3), sr, sg, sb2, er, eg, eb));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String applyGradient(String text, int startR, int startG, int startB, int endR, int endG, int endB) {
        StringBuilder out = new StringBuilder();
        int len = text.length();
        for (int i = 0; i < len; i++) {
            double t = len == 1 ? 0 : (double) i / (len - 1);
            int r = (int) (startR + (endR - startR) * t);
            int g = (int) (startG + (endG - startG) * t);
            int b = (int) (startB + (endB - startB) * t);
            out.append(String.format("§x§%02X§%02X§%02X", r, g, b));
            out.append(text.charAt(i));
        }
        return out.toString();
    }

    private boolean hasPermission(CommandSender sender, String permission) {
        if (sender.hasPermission(permission) || sender.isOp()) return true;
        if (luckPerms != null && sender instanceof Player) {
            try {
                User user = luckPerms.getUserManager().getUser(((Player) sender).getUniqueId());
                if (user != null) {
                    return user.getCachedData().getPermissionData().checkPermission(permission).asBoolean();
                }
            } catch (Exception e) {
                getLogger().warning("Perm check failed: " + e.getMessage());
            }
        }
        return false;
    }

    private boolean hasPermission(UUID uuid, String permission) {
        if (luckPerms == null) return false;
        try {
            User user = luckPerms.getUserManager().getUser(uuid);
            if (user == null) {
                user = luckPerms.getUserManager().loadUser(uuid).join();
            }
            return user != null && user.getCachedData().getPermissionData().checkPermission(permission).asBoolean();
        } catch (Exception e) {
            return false;
        }
    }

    private void sendMsg(CommandSender sender, String msg) {
        String prefix = config.getString("messages.prefix", "&7[&bNeptune&7] &r");
        sender.sendMessage(colorize(prefix + msg));
    }

    private String getMessage(String key, String defaultVal) {
        return config.getString("messages." + key, defaultVal);
    }

    private String getLuckPermsPrefix(Player player) {
        if (luckPerms == null) return "";
        try {
            User user = luckPerms.getUserManager().getUser(player.getUniqueId());
            if (user != null) {
                String prefix = user.getCachedData().getMetaData().getPrefix();
                if (prefix != null && !prefix.isEmpty()) {
                    return parseMiniMessage(prefix) + " ";
                }
            }
        } catch (Exception e) {
            getLogger().warning("Prefix fetch failed: " + e.getMessage());
        }
        return "";
    }

    private String getPrimaryGroup(Player player) {
        if (luckPerms == null) return "default";
        try {
            User user = luckPerms.getUserManager().getUser(player.getUniqueId());
            if (user != null) return user.getPrimaryGroup();
        } catch (Exception ignored) { }
        return "default";
    }

    private String parseMiniMessage(String input) {
        String result = input;
        result = result.replaceAll("<bold>", "&l").replaceAll("</bold>", "");
        result = result.replaceAll("<italic>", "&o").replaceAll("</italic>", "");
        result = result.replaceAll("<underline>", "&n").replaceAll("</underline>", "");
        result = result.replaceAll("<strikethrough>", "&m").replaceAll("</strikethrough>", "");
        result = result.replaceAll("<obfuscated>", "&k").replaceAll("</obfuscated>", "");
        result = result.replaceAll("<[^>]+>", "");
        return colorize(result);
    }

    private String getPlayerColor(Player player) {
        String color = playerColors.get(player.getUniqueId());
        if (color == null || color.isEmpty()) return "&f";
        if (color.startsWith("#") && color.matches("^#[A-Fa-f0-9]{6}$")) return color;
        if (!color.matches("^(&[0-9a-fk-or])+$")) {
            getLogger().warning("Invalid color for " + player.getName() + ": " + color + " — using &f");
            return "&f";
        }
        return color;
    }

    private String colorNameToCode(String name) {
        return switch (name.toLowerCase()) {
            case "black" -> "&0"; case "dark_blue" -> "&1"; case "dark_green" -> "&2";
            case "dark_aqua" -> "&3"; case "dark_red" -> "&4"; case "dark_purple" -> "&5";
            case "gold" -> "&6"; case "gray" -> "&7"; case "dark_gray" -> "&8";
            case "blue" -> "&9"; case "green" -> "&a"; case "aqua" -> "&b";
            case "red" -> "&c"; case "light_purple" -> "&d"; case "yellow" -> "&e";
            case "white" -> "&f"; default -> null;
        };
    }

    private boolean isValidHex(String s) {
        if (s == null) return false;
        String hex = s.startsWith("#") ? s.substring(1) : s;
        return hex.matches("^[A-Fa-f0-9]{6}$");
    }

    private String normalizeHex(String s) {
        return s.startsWith("#") ? s.toUpperCase() : ("#" + s.toUpperCase());
    }

    // ============================================================
    //                     VPN CHECK
    // ============================================================
    private boolean isVpn(String ip) {
        if (ip == null || ip.isEmpty()) return false;
        long cacheMs = config.getLong("antivpn.cache-minutes", 60) * 60_000L;
        VpnCacheEntry cached = vpnCache.get(ip);
        if (cached != null && System.currentTimeMillis() - cached.checkedAt() < cacheMs) {
            return cached.isVpn();
        }
        boolean result = false;
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(3))
                    .build();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://ip-api.com/json/" + ip + "?fields=proxy,hosting"))
                    .timeout(Duration.ofSeconds(3))
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            result = resp.body().contains("\"proxy\":true") || resp.body().contains("\"hosting\":true");
        } catch (Exception e) {
            getLogger().warning("VPN check failed for " + ip + ": " + e.getMessage());
        }
        vpnCache.put(ip, new VpnCacheEntry(result, System.currentTimeMillis()));
        return result;
    }

    // ============================================================
    //                     WEBSOCKET CLIENT
    // ============================================================
    private class WebSocketClient {
        private WebSocket ws;
        private final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
        private final AtomicInteger sendsThisSecond = new AtomicInteger();
        private volatile long lastSecond = System.currentTimeMillis() / 1000;

        public void connect() {
            String url = config.getString("backend.url");
            String serverId = config.getString("backend.server-id");
            String token = resolveToken();

            if (url == null || serverId == null || token == null ||
                    url.equals("wss://example.com/ws-plugin") ||
                    serverId.equals("CHANGE_ME") || token.equals("CHANGE_ME")) {
                getLogger().warning("Backend configuration incomplete.");
                return;
            }

            boolean tokenInAuth = config.getBoolean("backend.token-in-auth", true);

            try {
                URI uri = tokenInAuth
                        ? URI.create(url)
                        : URI.create(url + "?serverId=" + serverId + "&token=" + token);
                getLogger().info("Connecting to backend: " + url);

                client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .buildAsync(uri, new WebSocket.Listener() {
                            @Override
                            public void onOpen(WebSocket webSocket) {
                                ws = webSocket;
                                connected.set(true);
                                reconnectAttempts = 0;
                                getLogger().info("Connected to backend!");

                                Map<String, Object> auth = new HashMap<>();
                                auth.put("type", "AUTH");
                                auth.put("server_id", serverId);
                                if (tokenInAuth) auth.put("token", token);
                                auth.put("server_name", Bukkit.getServer().getName());
                                auth.put("plugin_version", getDescription().getVersion());
                                auth.put("minecraft_version", Bukkit.getVersion());
                                auth.put("server_version", Bukkit.getBukkitVersion());
                                send(auth);

                                while (!eventQueue.isEmpty()) {
                                    send(eventQueue.poll());
                                }

                                Map<String, Object> sync = new HashMap<>();
                                sync.put("type", "SYNC_REQUEST");
                                send(sync);
                                getLogger().info("Requested initial sync from backend.");

                                webSocket.request(1);
                            }

                            @Override
                            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                                try {
                                    handleBackendMessage(data.toString());
                                } catch (Exception e) {
                                    getLogger().warning("Error in onText: " + e.getMessage());
                                }
                                webSocket.request(1);
                                return CompletableFuture.completedFuture(null);
                            }

                            @Override
                            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                                connected.set(false);
                                getLogger().info("WebSocket closed: " + reason);
                                scheduleReconnect();
                                webSocket.request(1);
                                return CompletableFuture.completedFuture(null);
                            }

                            @Override
                            public void onError(WebSocket webSocket, Throwable error) {
                                connected.set(false);
                                getLogger().warning("WebSocket error: " + error.getMessage());
                                scheduleReconnect();
                            }
                        });
            } catch (Exception e) {
                getLogger().warning("Failed to connect: " + e.getMessage());
                scheduleReconnect();
            }
        }

        private void scheduleReconnect() {
            if (reconnectScheduled.get()) return;
            reconnectScheduled.set(true);
            long delay = Math.min(300, (long) Math.pow(2, Math.min(reconnectAttempts, 8))) * 20L;
            reconnectAttempts++;
            if (reconnectTask != null) reconnectTask.cancel();
            reconnectTask = new BukkitRunnable() {
                @Override
                public void run() {
                    reconnectScheduled.set(false);
                    connect();
                }
            }.runTaskLaterAsynchronously(NeptuneChat.this, delay);
        }

        public void send(Map<String, Object> data) {
            if (ws != null && connected.get()) {
                long now = System.currentTimeMillis() / 1000;
                if (now != lastSecond) {
                    lastSecond = now;
                    sendsThisSecond.set(0);
                }
                int limit = config.getInt("backend.send-rate-limit", 50);
                if (sendsThisSecond.incrementAndGet() > limit) {
                    getLogger().warning("WebSocket rate limit hit — dropping event");
                    return;
                }
                try {
                    ws.sendText(JsonUtil.toJson(data), true);
                } catch (Exception e) {
                    getLogger().warning("Failed to send: " + e.getMessage());
                    enqueue(data);
                }
            } else {
                enqueue(data);
            }
        }

        private void enqueue(Map<String, Object> data) {
            int max = config.getInt("backend.max-queue-size", 10000);
            if (eventQueue.size() >= max) {
                eventQueue.poll();
            }
            eventQueue.add(data);
        }

        public void sendPing() {
            Map<String, Object> ping = new HashMap<>();
            ping.put("type", "PING");
            ping.put("timestamp", System.currentTimeMillis());
            send(ping);
        }

        public void close() {
            if (ws != null) {
                try {
                    ws.sendClose(WebSocket.NORMAL_CLOSURE, "Plugin shutting down");
                } catch (Exception e) { }
            }
        }
    }

    private String resolveToken() {
        String envKey = config.getString("backend.token-env", "NEPTUNE_TOKEN");
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        return config.getString("backend.token");
    }

    private void requestSync() {
        if (wsClient != null && connected.get()) {
            Map<String, Object> sync = new HashMap<>();
            sync.put("type", "SYNC_REQUEST");
            wsClient.send(sync);
            getLogger().info("Requested sync from backend.");
        }
    }

    private void startHeartbeat() {
        if (heartbeatTask != null) heartbeatTask.cancel();
        heartbeatTask = new BukkitRunnable() {
            @Override
            public void run() {
                if (wsClient != null && connected.get()) {
                    wsClient.sendPing();
                }
            }
        }.runTaskTimerAsynchronously(this, 20L * 30, 20L * 30);
    }

    // ============================================================
    //                    BACKEND MESSAGES
    // ============================================================

    /* ============================================================
     *  Bezpieczne rozwiązywanie gracza z payloadu backendu.
     *  - Preferuje UUID (odporne na zmianę nicku).
     *  - Fallback na player_name z walidacją null/blank.
     *  - Obsługuje payload zagnieżdżony pod kluczem "payload".
     *  - Nigdy nie woła Bukkit.getPlayer(null) ani getPlayer("").
     * ============================================================ */
    private Player resolvePlayer(Map<String, Object> data) {
        if (data == null) return null;

        Player p = tryResolvePlayer(data);
        if (p != null) return p;

        Object nested = data.get("payload");
        if (nested instanceof Map<?, ?> raw) {
            Map<String, Object> payload = new HashMap<>();
            for (Map.Entry<?, ?> e : raw.entrySet()) {
                payload.put(String.valueOf(e.getKey()), e.getValue());
            }
            return tryResolvePlayer(payload);
        }
        return null;
    }

    private Player tryResolvePlayer(Map<String, Object> data) {
        Object uuidObj = data.get("player_uuid");
        if (uuidObj != null) {
            try {
                UUID uuid = UUID.fromString(uuidObj.toString().trim());
                Player p = Bukkit.getPlayer(uuid);
                if (p != null) return p;
            } catch (IllegalArgumentException ignored) { }
        }

        Object nameObj = data.get("player_name");
        if (nameObj == null) return null;
        String name = nameObj.toString().trim();
        if (name.isEmpty()) return null;

        return Bukkit.getPlayerExact(name);
    }

    /** Bezpieczne pobranie wartości z top-level LUB z payloadu. */
    private Object getPayloadValue(Map<String, Object> data, String key) {
        if (data == null) return null;
        Object v = data.get(key);
        if (v != null) return v;
        Object nested = data.get("payload");
        if (nested instanceof Map<?, ?> raw) {
            return raw.get(key);
        }
        return null;
    }

    private String getPayloadString(Map<String, Object> data, String key) {
        Object v = getPayloadValue(data, key);
        return v != null ? v.toString() : null;
    }

    private long getPayloadLong(Map<String, Object> data, String key) {
        return toLong(getPayloadValue(data, key));
    }

    /** Zwraca UUID gracza — z payloadu, offline cache lub nicku. */
    private UUID resolveUuid(Map<String, Object> data) {
        Player online = resolvePlayer(data);
        if (online != null) return online.getUniqueId();

        String uuidStr = getPayloadString(data, "player_uuid");
        if (uuidStr != null && !uuidStr.isEmpty()) {
            try { return UUID.fromString(uuidStr.trim()); }
            catch (IllegalArgumentException ignored) { }
        }
        String name = getPayloadString(data, "player_name");
        if (name != null && !name.isEmpty()) {
            OfflinePlayer off = Bukkit.getOfflinePlayer(name);
            if (off != null && off.getUniqueId() != null) return off.getUniqueId();
        }
        return null;
    }

    private void handleBackendMessage(String msg) {
        try {
            Map<String, Object> data = JsonUtil.fromJson(msg);
            String type = (String) data.get("type");
            if (type == null) return;

            switch (type) {
                case "PONG" -> { }

                case "CHAT_MESSAGE" -> {
                    String message = getPayloadString(data, "message");
                    if (message == null) message = (String) data.get("message");
                    if (message != null && !message.isEmpty()) {
                        broadcastFormatted("&7[&cAdmin&7] &f" + message);
                    }
                }

                case "MUTE_REQUEST" -> {
                    long duration = getPayloadLong(data, "duration");
                    String reason = getPayloadString(data, "reason");
                    if (reason == null) reason = "Brak powodu";
                    UUID uuid = resolveUuid(data);
                    if (uuid == null) {
                        getLogger().warning("[BACKEND] MUTE_REQUEST: nie znaleziono gracza");
                        return;
                    }
                    final long d = duration;
                    final String r = reason;
                    final UUID u = uuid;
                    Bukkit.getScheduler().runTask(this, () -> {
                        mutePlayer(u, d, r, "WEB");
                        Player target = Bukkit.getPlayer(u);
                        if (target != null) target.sendMessage(ChatColor.RED + "Zostałeś wyciszony! Powód: " + r);
                    });
                }

                case "UNMUTE_REQUEST" -> {
                    UUID uuid = resolveUuid(data);
                    if (uuid == null) { getLogger().warning("[BACKEND] UNMUTE_REQUEST: brak gracza"); return; }
                    final UUID u = uuid;
                    Bukkit.getScheduler().runTask(this, () -> unmutePlayer(u, "WEB"));
                }

                case "WARN_REQUEST" -> {
                    String reason = getPayloadString(data, "reason");
                    if (reason == null) reason = "Brak powodu";
                    UUID uuid = resolveUuid(data);
                    if (uuid == null) { getLogger().warning("[BACKEND] WARN_REQUEST: brak gracza"); return; }
                    final UUID u = uuid;
                    final String r = reason;
                    Bukkit.getScheduler().runTask(this, () -> {
                        warnPlayer(u, r, "WEB");
                        Player target = Bukkit.getPlayer(u);
                        if (target != null) target.sendMessage(ChatColor.RED + "Ostrzeżenie: " + r);
                    });
                }

                case "KICK_REQUEST" -> {
                    String reason = getPayloadString(data, "reason");
                    if (reason == null) reason = "Brak powodu";
                    Player target = resolvePlayer(data);
                    if (target == null) { getLogger().warning("[BACKEND] KICK_REQUEST: brak gracza online"); return; }
                    final Player t = target;
                    final String r = reason;
                    Bukkit.getScheduler().runTask(this, () -> {
                        t.kickPlayer(colorize(getMessage("kicked",
                                "&cZostałeś wyrzucony!\n&ePowód: &f{reason}").replace("{reason}", r)));
                        sendActionEvent("KICK", t.getUniqueId(), t.getName(), r, "WEB");
                    });
                }

                case "BAN_REQUEST" -> {
                    long duration = getPayloadLong(data, "duration");
                    String reason = getPayloadString(data, "reason");
                    if (reason == null) reason = "Brak powodu";
                    UUID uuid = resolveUuid(data);
                    String name = getPayloadString(data, "player_name");
                    if (uuid == null) {
                        getLogger().warning("[BACKEND] BAN_REQUEST: nie znaleziono gracza");
                        return;
                    }
                    final long d = duration;
                    final String r = reason;
                    final UUID u = uuid;
                    final String n = name;
                    Bukkit.getScheduler().runTask(this, () -> {
                        Player target = Bukkit.getPlayer(u);
                        banPlayer(u, n, d, r, "WEB");
                        if (target != null) {
                            target.kickPlayer(colorize(getMessage("banned",
                                    "&cZostałeś zbanowany!\n&ePowód: &f{reason}").replace("{reason}", r)));
                        }
                    });
                }

                case "UNBAN_REQUEST" -> {
                    UUID uuid = resolveUuid(data);
                    if (uuid == null) { getLogger().warning("[BACKEND] UNBAN_REQUEST: brak gracza"); return; }
                    final UUID u = uuid;
                    final String name = getPayloadString(data, "player_name");
                    Bukkit.getScheduler().runTask(this, () -> unbanPlayer(u, name, "WEB"));
                }

                case "CLEAR_CHAT_REQUEST" -> Bukkit.getScheduler().runTask(this, this::clearChat);

                case "CHAT_LOCK_REQUEST" -> {
                    long duration = getPayloadLong(data, "duration");
                    String reason = getPayloadString(data, "reason");
                    if (reason == null) reason = "";
                    final long d = duration;
                    final String r = reason;
                    Bukkit.getScheduler().runTask(this, () -> lockChat(d, r, "WEB"));
                }

                case "CHAT_UNLOCK_REQUEST" ->
                        Bukkit.getScheduler().runTask(this, () -> unlockChat("WEB"));

                case "SLOWMODE_REQUEST" -> {
                    int seconds = (int) getPayloadLong(data, "seconds");
                    Bukkit.getScheduler().runTask(this, () -> {
                        slowmodeSeconds = Math.max(0, seconds);
                        config.set("chat.slowmode-seconds", slowmodeSeconds);
                        saveConfig();
                        broadcastFormatted("&eSlowmode ustawiony na &f" + slowmodeSeconds + "s &eprzez panel.");
                    });
                }

                case "BROADCAST_REQUEST" -> {
                    String message = getPayloadString(data, "message");
                    if (message == null) message = (String) data.get("message");
                    if (message == null || message.isEmpty()) return;
                    final String m = message;
                    Bukkit.getScheduler().runTask(this, () -> {
                        String formatted = colorize("&8[&c&lOGŁOSZENIE&8] &f" + m);
                        for (Player p : Bukkit.getOnlinePlayers()) {
                            p.sendMessage(formatted);
                            p.playSound(p.getLocation(), Sound.BLOCK_NOTE_BLOCK_PLING, 1f, 1.5f);
                        }
                        Bukkit.getConsoleSender().sendMessage(formatted);
                    });
                }

                case "SYNC_RESPONSE" -> handleSyncResponse(data);
                case "FILTER_UPDATE" -> handleFilterUpdate(data);

                default -> getLogger().warning("Unknown message type: " + type);
            }
        } catch (Exception e) {
            getLogger().warning("Error handling backend message: " + e.getMessage());
        }
    }

    private void handleSyncResponse(Map<String, Object> data) {
        Object filterConfig = data.get("filter_config");
        if (filterConfig instanceof List<?> configList && !configList.isEmpty()) {
            filterConfigs.clear();
            filterWords.clear();
            int count = 0;
            for (Object obj : configList) {
                if (obj instanceof Map<?, ?> m) {
                    String word = (String) m.get("word");
                    String action = (String) m.get("action");
                    long duration = toLong(m.get("duration_ms"));
                    if (word != null && !word.isEmpty()) {
                        String lower = word.toLowerCase();
                        filterConfigs.put(lower, new FilterConfig(word, action != null ? action : "REPLACE", duration));
                        filterWords.add(lower);
                        count++;
                    }
                }
            }
            getLogger().info("Filters synced from backend: " + count);
        } else {
            getLogger().info("Filters synced from backend: 0 (pusta lista — zachowuję lokalne)");
        }

        Object mutes = data.get("mutes");
        if (mutes instanceof List<?> muteList) {
            activeMutes.clear();
            mutedPlayers.clear();
            for (Object obj : muteList) {
                if (obj instanceof Map<?, ?> m) {
                    String uuidStr = (String) m.get("player_uuid");
                    long until = toLong(m.get("until"));
                    String reason = (String) m.get("reason");
                    String moderator = (String) m.get("moderator");
                    if (uuidStr != null) {
                        try {
                            UUID uuid = UUID.fromString(uuidStr);
                            long untilMs = until > 0 ? until : Long.MAX_VALUE;
                            if (untilMs == Long.MAX_VALUE || untilMs > System.currentTimeMillis()) {
                                activeMutes.put(uuid, new MuteInfo(untilMs, reason, moderator));
                                mutedPlayers.add(uuid);
                            }
                        } catch (Exception e) {
                            getLogger().warning("Bad mute uuid: " + uuidStr);
                        }
                    }
                }
            }
        }

        Object bans = data.get("bans");
        if (bans instanceof List<?> banList) {
            activeBans.clear();
            bannedPlayers.clear();
            long now = System.currentTimeMillis();
            for (Object obj : banList) {
                if (obj instanceof Map<?, ?> m) {
                    String uuidStr = (String) m.get("player_uuid");
                    long until = toLong(m.get("until"));
                    String reason = (String) m.get("reason");
                    String moderator = (String) m.get("moderator");
                    if (uuidStr != null) {
                        try {
                            UUID uuid = UUID.fromString(uuidStr);
                            long untilMs = until > 0 ? until : Long.MAX_VALUE;
                            if (untilMs == Long.MAX_VALUE || untilMs > now) {
                                activeBans.put(uuid, new BanInfo(untilMs, reason, moderator));
                                bannedPlayers.add(uuid);
                            }
                        } catch (Exception e) {
                            getLogger().warning("Bad ban uuid: " + uuidStr);
                        }
                    }
                }
            }
        }

        Object automessages = data.get("automessages");
        if (automessages instanceof List<?> amList) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (Object obj : amList) {
                if (obj instanceof Map<?, ?> m) {
                    Map<String, Object> map = new HashMap<>();
                    for (Map.Entry<?, ?> e : m.entrySet()) {
                        map.put(String.valueOf(e.getKey()), e.getValue());
                    }
                    list.add(map);
                }
            }
            reloadAutoMessages(list);
        }

        getLogger().info("Sync complete. Filters: " + filterWords.size() +
                ", Mutes: " + activeMutes.size() +
                ", Bans: " + activeBans.size() +
                ", AutoMessages: " + autoMessages.size());
    }

    private void handleFilterUpdate(Map<String, Object> data) {
        Object configObj = data.get("filter_config");
        if (configObj instanceof List<?> configList) {
            filterConfigs.clear();
            filterWords.clear();
            for (Object obj : configList) {
                if (obj instanceof Map<?, ?> m) {
                    String word = (String) m.get("word");
                    String action = (String) m.get("action");
                    long duration = toLong(m.get("duration_ms"));
                    if (word != null && !word.isEmpty()) {
                        String lower = word.toLowerCase();
                        filterConfigs.put(lower, new FilterConfig(word, action != null ? action : "REPLACE", duration));
                        filterWords.add(lower);
                    }
                }
            }
            getLogger().info("Filter config updated live: " + filterConfigs.size());
        }

        Object automessages = data.get("automessages");
        if (automessages instanceof List<?> amList) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (Object obj : amList) {
                if (obj instanceof Map<?, ?> m) {
                    Map<String, Object> map = new HashMap<>();
                    for (Map.Entry<?, ?> e : m.entrySet()) {
                        map.put(String.valueOf(e.getKey()), e.getValue());
                    }
                    list.add(map);
                }
            }
            reloadAutoMessages(list);
        }
    }

    private long toLong(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number) return ((Number) obj).longValue();
        try { return Long.parseLong(obj.toString()); } catch (Exception e) { return 0; }
    }

    // ============================================================
    //                        ACTIONS
    // ============================================================
    private void sendActionEvent(String action, UUID playerUuid, String playerName, String reason, String source) {
        Map<String, Object> event = new HashMap<>();
        event.put("type", action);
        event.put("event_id", UUID.randomUUID().toString());
        event.put("server_id", config.getString("backend.server-id"));
        event.put("timestamp", System.currentTimeMillis());
        Map<String, Object> payload = new HashMap<>();
        if (playerUuid != null) payload.put("player_uuid", playerUuid.toString());
        if (playerName != null) payload.put("player_name", playerName);
        if (reason != null) payload.put("reason", reason);
        payload.put("source", source);
        event.put("payload", payload);
        if (wsClient != null) wsClient.send(event);
    }

    private void mutePlayer(UUID uuid, long durationMillis, String reason, String source) {
        long until = durationMillis > 0 ? System.currentTimeMillis() + durationMillis : Long.MAX_VALUE;
        activeMutes.put(uuid, new MuteInfo(until, reason, source));
        mutedPlayers.add(uuid);
        String name = playerNames.getOrDefault(uuid, null);
        if (name == null) {
            OfflinePlayer off = Bukkit.getOfflinePlayer(uuid);
            name = off != null ? off.getName() : null;
        }
        if (name == null) name = "Unknown";

        saveHistory(uuid, name, "MUTE", reason, source, source);

        Map<String, Object> event = new HashMap<>();
        event.put("type", "MUTE");
        event.put("event_id", UUID.randomUUID().toString());
        event.put("server_id", config.getString("backend.server-id"));
        event.put("timestamp", System.currentTimeMillis());
        Map<String, Object> payload = new HashMap<>();
        payload.put("player_uuid", uuid.toString());
        payload.put("player_name", name);
        payload.put("reason", reason);
        payload.put("source", source);
        payload.put("duration_ms", durationMillis);
        event.put("payload", payload);
        if (wsClient != null) wsClient.send(event);
    }

    private void unmutePlayer(UUID uuid, String source) {
        if (activeMutes.remove(uuid) != null) {
            mutedPlayers.remove(uuid);
            String name = playerNames.getOrDefault(uuid, null);
            if (name == null) {
                OfflinePlayer off = Bukkit.getOfflinePlayer(uuid);
                name = off != null ? off.getName() : null;
            }
            if (name == null) name = "Unknown";
            saveHistory(uuid, name, "UNMUTE", null, source, source);
            sendActionEvent("UNMUTE", uuid, name, null, source);
        }
    }

    private void banPlayer(UUID uuid, String playerName, long durationMs, String reason, String source) {
        long until = durationMs > 0 ? System.currentTimeMillis() + durationMs : Long.MAX_VALUE;
        String name = playerName;
        if (name == null && uuid != null) {
            OfflinePlayer off = Bukkit.getOfflinePlayer(uuid);
            name = off != null ? off.getName() : null;
        }
        if (name == null) name = "Unknown";

        activeBans.put(uuid, new BanInfo(until, reason, source));
        bannedPlayers.add(uuid);
        saveBanToDb(uuid, name, until, reason, source);
        saveHistory(uuid, name, "BAN", reason, source, source);
        sendActionEvent("BAN", uuid, name, reason, source);
    }

    private void unbanPlayer(UUID uuid, String playerName, String source) {
        activeBans.remove(uuid);
        bannedPlayers.remove(uuid);
        deleteBanFromDb(uuid);
        String name = playerName;
        if (name == null) {
            OfflinePlayer off = Bukkit.getOfflinePlayer(uuid);
            name = off != null ? off.getName() : null;
        }
        if (name == null) name = "Unknown";
        saveHistory(uuid, name, "UNBAN", null, source, source);
        sendActionEvent("UNBAN", uuid, name, null, source);
    }

    private void warnPlayer(UUID uuid, String reason, String source) {
        String id = UUID.randomUUID().toString();
        WarnInfo warn = new WarnInfo(id, reason, source, System.currentTimeMillis());
        warnings.computeIfAbsent(uuid, k -> new ArrayList<>()).add(warn);

        String name = playerNames.getOrDefault(uuid, null);
        if (name == null) {
            OfflinePlayer off = Bukkit.getOfflinePlayer(uuid);
            name = off != null ? off.getName() : null;
        }
        if (name == null) name = "Unknown";

        saveWarningToDb(warn, uuid, name);
        saveHistory(uuid, name, "WARN", reason, source, source);
        sendActionEvent("WARN", uuid, name, reason, source);

        int autoMuteAfter = config.getInt("chat.auto-mute-after-warns", 0);
        if (autoMuteAfter > 0 && warnings.get(uuid).size() >= autoMuteAfter) {
            long duration = parseDuration(config.getString("chat.auto-mute-duration", "1h"));
            mutePlayer(uuid, duration, "Auto-mute: " + autoMuteAfter + " ostrzeżeń", "SYSTEM");
            Player target = Bukkit.getPlayer(uuid);
            if (target != null) {
                target.sendMessage(colorize(getMessage("auto-mute-warn", "&cZostałeś automatycznie wyciszony za {count} ostrzeżeń.")
                        .replace("{count}", String.valueOf(autoMuteAfter))));
            }
        }
    }

    private void clearChat() {
        int lines = config.getInt("chat.clear-lines", 100);
        for (int i = 0; i < lines; i++) {
            Bukkit.broadcastMessage("");
        }
        sendActionEvent("CLEAR_CHAT", null, null, null, "MINECRAFT");
    }

    private void lockChat(long durationMillis, String reason, String source) {
        chatLocked = true;
        chatLockReason = reason != null ? reason : "";
        chatLockUntil = durationMillis > 0 ? System.currentTimeMillis() + durationMillis : 0;

        if (chatLockTask != null) chatLockTask.cancel();

        chatLockTask = new BukkitRunnable() {
            @Override
            public void run() {
                if (!chatLocked) {
                    cancel();
                    return;
                }
                if (chatLockUntil > 0 && System.currentTimeMillis() >= chatLockUntil) {
                    chatLocked = false;
                    saveChatLockState();
                    broadcastFormatted(getMessage("chat-unlocked", "&aCzat został odblokowany."));
                    sendActionEvent("CHAT_UNLOCK", null, null, null, "SYSTEM");
                    cancel();
                    return;
                }
                String timeStr = chatLockUntil == 0 ? "PERMANENTNIE" : formatDuration(chatLockUntil - System.currentTimeMillis());
                broadcastFormatted("&c&lCzat jest zablokowany!&r &7Pozostały czas: &e" + timeStr +
                        (chatLockReason.isEmpty() ? "" : " &7Powód: &e" + chatLockReason));
            }
        }.runTaskTimer(this, 0, 20L * 5);

        saveChatLockState();
        sendActionEvent("CHAT_LOCK", null, null, reason, source);
        broadcastFormatted("&c&lCzat został zablokowany!" +
                (reason != null && !reason.isEmpty() ? " &7Powód: &e" + reason : ""));
    }

    private void unlockChat(String source) {
        chatLocked = false;
        chatLockUntil = 0;
        chatLockReason = "";
        if (chatLockTask != null) chatLockTask.cancel();
        saveChatLockState();
        sendActionEvent("CHAT_UNLOCK", null, null, null, source);
        broadcastFormatted(getMessage("chat-unlocked", "&aCzat został odblokowany."));
    }

    // ============================================================
    //                       CHAT EVENT
    // ============================================================
    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true)
    public void onChat(AsyncPlayerChatEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();
        String message = event.getMessage();

        if (mutedPlayers.contains(uuid)) {
            MuteInfo info = activeMutes.get(uuid);
            if (info != null && info.until() > System.currentTimeMillis()) {
                long remaining = info.until() - System.currentTimeMillis();
                String msg = getMessage("muted", "&cZostałeś wyciszony!\n&ePowód: &f{reason}\n&ePozostały czas: &f{time}")
                        .replace("{reason}", info.reason() != null ? info.reason() : "Brak")
                        .replace("{time}", formatDuration(remaining));
                player.sendMessage(colorize(msg));
                event.setCancelled(true);
                return;
            } else {
                mutedPlayers.remove(uuid);
                activeMutes.remove(uuid);
            }
        }

        if (chatLocked && !hasPermission(player, "neptune.chat.bypass.lock")) {
            String timeStr = chatLockUntil == 0 ? "PERMANENTNIE" : formatDuration(chatLockUntil - System.currentTimeMillis());
            String msg = getMessage("chat-locked", "&cCzat jest zablokowany! &7Pozostały czas: &e{time}")
                    .replace("{time}", timeStr);
            player.sendMessage(colorize(msg));
            event.setCancelled(true);
            return;
        }

        if (config.getBoolean("anticaps.strip-invisible", true)) {
            message = INVISIBLE_PATTERN.matcher(message).replaceAll("");
        }
        if (config.getBoolean("anticaps.strip-spoofing", true)) {
            message = SPOOFING_PATTERN.matcher(message).replaceAll("");
        }

        if (config.getBoolean("antilink.enabled", true) && !hasPermission(player, "neptune.chat.bypass.antilink")) {
            if (config.getBoolean("antilink.block-urls", true) && URL_PATTERN.matcher(message).find()) {
                if (!isWhitelistedUrl(message)) {
                    player.sendMessage(colorize(getMessage("antilink-blocked", "&cNie wysyłaj linków!")));
                    event.setCancelled(true);
                    return;
                }
            }
            if (config.getBoolean("antilink.block-ips", true) && IP_PATTERN.matcher(message).find()) {
                player.sendMessage(colorize(getMessage("antilink-blocked", "&cNie wysyłaj adresów IP!")));
                event.setCancelled(true);
                return;
            }
        }

        String autoResp = checkAutoResponder(message, player);
        if (autoResp != null) {
            Bukkit.getScheduler().runTask(this, () -> player.sendMessage(colorize(autoResp)));
        }

        if (config.getBoolean("caps.enabled", true) && !hasPermission(player, "neptune.chat.bypass.caps")) {
            String processed = applyAntiCaps(player, message);
            if (processed == null) {
                event.setCancelled(true);
                return;
            }
            if (!processed.equals(message)) {
                event.setMessage(processed);
                message = processed;
            }
        }

        if (config.getBoolean("filter.enabled", true) && !hasPermission(player, "neptune.chat.bypass.filter")) {
            String filtered = applyFilter(message, player);
            if (filtered == null) {
                event.setCancelled(true);
                return;
            }
            if (!filtered.equals(message)) {
                event.setMessage(filtered);
                message = filtered;
            }
        }

        if (config.getBoolean("antispam.enabled", true) && !hasPermission(player, "neptune.chat.bypass.antispam")) {
            if (checkSpam(player, message)) {
                event.setCancelled(true);
                return;
            }
        }

        if (slowmodeSeconds > 0 && !hasPermission(player, "neptune.chat.bypass.slowmode")) {
            long last = lastMessageTime.getOrDefault(uuid, 0L);
            long diff = (System.currentTimeMillis() - last) / 1000;
            if (diff < slowmodeSeconds) {
                long remaining = slowmodeSeconds - diff;
                player.sendMessage(colorize(getMessage("slowmode", "&cZwolnij! Poczekaj jeszcze {time}.")
                        .replace("{time}", remaining + "s")));
                event.setCancelled(true);
                return;
            }
            lastMessageTime.put(uuid, System.currentTimeMillis());
        }

        Set<UUID> mentioned = new HashSet<>();
        if (config.getBoolean("chat.mention-sound", true)) {
            Matcher mMatch = MENTION_PATTERN.matcher(message);
            while (mMatch.find()) {
                Player online = Bukkit.getPlayerExact(mMatch.group(1));
                if (online != null && !online.getUniqueId().equals(uuid)) {
                    mentioned.add(online.getUniqueId());
                }
            }
        }

        final String finalMessage = message;
        final Set<UUID> finalMentioned = mentioned;
        String prefix = getLuckPermsPrefix(player);
        String color = getPlayerColor(player);
        String coloredName = colorize(color) + player.getName() + ChatColor.RESET;
        String formatted = prefix + coloredName + ChatColor.RESET + ": " + colorize(finalMessage);

        event.setCancelled(true);
        Bukkit.getScheduler().runTask(this, () -> {
            for (Player online : Bukkit.getOnlinePlayers()) {
                if (finalMentioned.contains(online.getUniqueId())) {
                    online.sendMessage(colorize("&e&l» &r") + formatted);
                    online.playSound(online.getLocation(), Sound.BLOCK_NOTE_BLOCK_PLING, 1f, 2f);
                } else {
                    online.sendMessage(formatted);
                }
            }
            Bukkit.getConsoleSender().sendMessage(formatted);
        });

        Map<String, Object> chatEvent = new HashMap<>();
        chatEvent.put("type", "CHAT_MESSAGE");
        chatEvent.put("event_id", UUID.randomUUID().toString());
        chatEvent.put("server_id", config.getString("backend.server-id"));
        chatEvent.put("timestamp", System.currentTimeMillis());
        Map<String, Object> payload = new HashMap<>();
        payload.put("player_uuid", uuid.toString());
        payload.put("player_name", player.getName());
        payload.put("message", finalMessage);
        chatEvent.put("payload", payload);
        if (wsClient != null) wsClient.send(chatEvent);
    }

    private boolean isWhitelistedUrl(String message) {
        List<String> whitelist = config.getStringList("antilink.whitelist");
        for (String domain : whitelist) {
            if (message.toLowerCase().contains(domain.toLowerCase())) return true;
        }
        return false;
    }

    private String applyAntiCaps(Player player, String msg) {
        if (msg.length() < config.getInt("caps.min-message-length", 5)) return msg;
        int uppercase = 0, letters = 0;
        for (char c : msg.toCharArray()) {
            if (Character.isLetter(c)) {
                letters++;
                if (Character.isUpperCase(c)) uppercase++;
            }
        }
        if (letters == 0) return msg;
        double percentage = (double) uppercase / letters * 100;
        int threshold = config.getInt("caps.minimum-uppercase-percentage", 60);
        if (percentage >= threshold) {
            int current = capsWarnings.getOrDefault(player.getUniqueId(), 0) + 1;
            capsWarnings.put(player.getUniqueId(), current);
            int limit = config.getInt("caps.warning-limit", 2);
            if (current >= limit) {
                long muteMillis = parseDuration(config.getString("caps.mute-duration", "20m"));
                mutePlayer(player.getUniqueId(), muteMillis, "Nadmierne używanie wielkich liter", "ANTICAPS");
                player.sendMessage(ChatColor.RED + "Zostałeś automatycznie wyciszony za nadmierne używanie wielkich liter.");
                return null;
            } else {
                player.sendMessage(ChatColor.YELLOW + "⚠ Nie używaj nadmiernie wielkich liter!\n" +
                        ChatColor.GRAY + "Ostrzeżenie: " + current + "/" + limit);
            }
            if (config.getBoolean("caps.convert-to-lowercase", true)) {
                return msg.toLowerCase();
            }
        } else {
            capsWarnings.remove(player.getUniqueId());
        }
        return msg;
    }

    private String applyFilter(String msg, Player player) {
        if (filterWords.isEmpty()) return msg;

        String normalized = msg.toLowerCase()
                .replaceAll("[\\s.,!?;:]+", "")
                .replaceAll("(.)\\1+", "$1");

        for (String word : filterWords) {
            String wordNorm = word.toLowerCase()
                    .replaceAll("[\\s.,!?;:]+", "")
                    .replaceAll("(.)\\1+", "$1");

            if (wordNorm.isEmpty()) continue;

            if (normalized.contains(wordNorm)) {
                FilterConfig fc = filterConfigs.getOrDefault(word, new FilterConfig(word, "REPLACE", 0));
                getLogger().info("Filter match: '" + word + "' (" + fc.action() + ") in: " + msg);

                switch (fc.action()) {
                    case "BLOCK" -> {
                        player.sendMessage(ChatColor.RED + "Twoja wiadomość zawiera zakazane słowo.");
                        return null;
                    }
                    case "WARN" -> {
                        warnPlayer(player.getUniqueId(), "Użycie zakazanego słowa: " + word, "FILTER");
                        player.sendMessage(ChatColor.YELLOW + "⚠ Twoja wiadomość zawiera zakazane słowo.");
                        return msg;
                    }
                    case "MUTE" -> {
                        long duration = fc.durationMs() > 0 ? fc.durationMs() : 300000;
                        mutePlayer(player.getUniqueId(), duration, "Użycie zakazanego słowa: " + word, "FILTER");
                        player.sendMessage(ChatColor.RED + "Zostałeś wyciszony na " + formatDuration(duration) + " za użycie zakazanego słowa.");
                        return null;
                    }
                    case "BAN" -> {
                        long duration = fc.durationMs() > 0 ? fc.durationMs() : 86400000;
                        banPlayer(player.getUniqueId(), player.getName(), duration, "Użycie zakazanego słowa: " + word, "FILTER");
                        player.kickPlayer(colorize(getMessage("banned", "&cZostałeś zbanowany!\n&ePowód: &f{reason}")
                                .replace("{reason}", "Zakazane słowo: " + word)));
                        return null;
                    }
                    case "KICK" -> {
                        player.kickPlayer(colorize(getMessage("kicked", "&cZostałeś wyrzucony!\n&ePowód: &f{reason}")
                                .replace("{reason}", "Zakazane słowo: " + word)));
                        sendActionEvent("KICK", player.getUniqueId(), player.getName(), "Zakazane słowo: " + word, "FILTER");
                        return null;
                    }
                    default -> {
                        String stars = "*".repeat(Math.max(3, word.length()));
                        return msg.replaceAll("(?i)" + Pattern.quote(word), stars);
                    }
                }
            }
        }
        return msg;
    }

    private boolean checkSpam(Player player, String message) {
        UUID uuid = player.getUniqueId();
        Deque<Long> timestamps = messageTimestamps.computeIfAbsent(uuid, k -> new LinkedList<>());
        long now = System.currentTimeMillis();
        int maxMessages = config.getInt("antispam.max-messages", 5);
        int windowSeconds = config.getInt("antispam.time-window-seconds", 5);
        long windowMillis = windowSeconds * 1000L;

        timestamps.add(now);
        while (!timestamps.isEmpty() && now - timestamps.peekFirst() > windowMillis) {
            timestamps.pollFirst();
        }
        if (timestamps.size() > maxMessages) {
            player.sendMessage(ChatColor.RED + "Zbyt szybkie pisanie! Zwolnij.");
            return true;
        }

        if (config.getBoolean("antispam.duplicate-detection", true)) {
            String last = lastMessages.get(uuid);
            if (last != null && last.equalsIgnoreCase(message)) {
                player.sendMessage(ChatColor.RED + "Nie powtarzaj tej samej wiadomości!");
                return true;
            }
        }
        lastMessages.put(uuid, message);
        return false;
    }

    // ============================================================
    //                       JOIN/QUIT
    // ============================================================
    @EventHandler
    public void onJoin(PlayerJoinEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();

        // --- BAN CHECK ---
        BanInfo ban = activeBans.get(uuid);
        if (ban != null) {
            if (ban.until() == Long.MAX_VALUE || ban.until() > System.currentTimeMillis()) {
                player.kickPlayer(colorize(getMessage("banned",
                        "&cZostałeś zbanowany!\n&ePowód: &f{reason}")
                        .replace("{reason}", ban.reason() != null ? ban.reason() : "Brak")));
                return;
            } else {
                activeBans.remove(uuid);
                bannedPlayers.remove(uuid);
                deleteBanFromDb(uuid);
            }
        }

        long now = System.currentTimeMillis();

        playerNames.put(uuid, player.getName());
        savePlayerNameToDb(uuid, player.getName());

        playerFirstSeen.putIfAbsent(uuid, now);
        playerLastSeen.put(uuid, now);
        playerSessionStart.put(uuid, now);
        playerPlaytime.putIfAbsent(uuid, 0L);

        if (config.getBoolean("antivpn.enabled", false)
                && config.getBoolean("antivpn.check-on-join", true)
                && !hasPermission(player, "neptune.chat.bypass.vpn")) {
            String ip = player.getAddress() != null && player.getAddress().getAddress() != null
                    ? player.getAddress().getAddress().getHostAddress() : null;
            if (ip != null) {
                Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
                    if (isVpn(ip)) {
                        Bukkit.getScheduler().runTask(this, () -> {
                            if (player.isOnline()) {
                                player.kickPlayer(colorize(config.getString("antivpn.kick-message",
                                        getMessage("vpn-kick", "&cVPN/Proxy wykryty!"))));
                            }
                        });
                    }
                });
            }
        }

        if (config.getBoolean("automessages.enabled", true)) {
            String group = getPrimaryGroup(player);
            String joinMsg = config.getString("automessages.join-messages." + group,
                    config.getString("automessages.join-messages.default", "&7[&a+&7] &f{player}"));
            if (joinMsg != null && !joinMsg.isEmpty()) {
                broadcastFormatted(joinMsg.replace("{player}", player.getName()));
            }
        }

        Map<String, Object> joinEvent = new HashMap<>();
        joinEvent.put("type", "PLAYER_JOIN");
        joinEvent.put("event_id", UUID.randomUUID().toString());
        joinEvent.put("server_id", config.getString("backend.server-id"));
        joinEvent.put("timestamp", System.currentTimeMillis());
        joinEvent.put("player_count", Bukkit.getOnlinePlayers().size());
        Map<String, Object> payload = new HashMap<>();
        payload.put("player_uuid", uuid.toString());
        payload.put("player_name", player.getName());
        joinEvent.put("payload", payload);
        if (wsClient != null) wsClient.send(joinEvent);
    }

    @EventHandler
    public void onQuit(PlayerQuitEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();
        long now = System.currentTimeMillis();

        playerLastSeen.put(uuid, now);

        Long start = playerSessionStart.remove(uuid);
        if (start != null) {
            playerPlaytime.merge(uuid, now - start, Long::sum);
        }

        if (config.getBoolean("automessages.enabled", true)) {
            String group = getPrimaryGroup(player);
            String quitMsg = config.getString("automessages.quit-messages." + group,
                    config.getString("automessages.quit-messages.default", "&7[&c-&7] &f{player}"));
            if (quitMsg != null && !quitMsg.isEmpty()) {
                broadcastFormatted(quitMsg.replace("{player}", player.getName()));
            }
        }

        Map<String, Object> quitEvent = new HashMap<>();
        quitEvent.put("type", "PLAYER_QUIT");
        quitEvent.put("event_id", UUID.randomUUID().toString());
        quitEvent.put("server_id", config.getString("backend.server-id"));
        quitEvent.put("timestamp", System.currentTimeMillis());
        quitEvent.put("player_count", Math.max(0, Bukkit.getOnlinePlayers().size() - 1));
        Map<String, Object> payload = new HashMap<>();
        payload.put("player_uuid", uuid.toString());
        payload.put("player_name", player.getName());
        quitEvent.put("payload", payload);
        if (wsClient != null) wsClient.send(quitEvent);
    }

    // ============================================================
    //                        UTILS
    // ============================================================
    public static long parseDuration(String input) {
        if (input == null || input.isEmpty()) return 0;
        String s = input.trim().toLowerCase();
        if (s.equals("perm") || s.equals("permanent") || s.equals("p") || s.equals("0")) {
            return Long.MAX_VALUE;
        }
        char unit = s.charAt(s.length() - 1);
        long value;
        try {
            value = Long.parseLong(s.substring(0, s.length() - 1));
        } catch (NumberFormatException e) {
            return 0;
        }
        if (value <= 0) return 0;
        return switch (unit) {
            case 's' -> value * 1000L;
            case 'm' -> value * 60 * 1000L;
            case 'h' -> value * 3600 * 1000L;
            case 'd' -> value * 86400 * 1000L;
            default -> 0;
        };
    }

    public static String formatDuration(long millis) {
        if (millis == Long.MAX_VALUE) return "permanentny";
        long totalSeconds = millis / 1000;
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        StringBuilder sb = new StringBuilder();
        if (days > 0) sb.append(days).append("d ");
        if (hours > 0) sb.append(hours).append("h ");
        if (minutes > 0) sb.append(minutes).append("m ");
        if (seconds > 0) sb.append(seconds).append("s");
        return sb.toString().trim();
    }

    // ============================================================
    //                       COMMANDS
    // ============================================================
    private class MuteCommand implements CommandExecutor {
        @Override
        public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
            if (!hasPermission(sender, "neptune.chat.mute")) { sendMsg(sender, getMessage("no-permission", "&cNie masz uprawnień.")); return true; }
            if (args.length < 2) { sendMsg(sender, "&cUżycie: /mute <gracz> <czas> [powód]"); return true; }
            Player target = Bukkit.getPlayer(args[0]);
            if (target == null) { sendMsg(sender, getMessage("player-not-found", "&cGracz nie jest online.")); return true; }
            long duration = parseDuration(args[1]);
            if (duration == 0) { sendMsg(sender, getMessage("invalid-time", "&cNieprawidłowy czas.")); return true; }
            String reason = args.length > 2 ? String.join(" ", Arrays.copyOfRange(args, 2, args.length)) : "Brak powodu";
            mutePlayer(target.getUniqueId(), duration, reason, "MINECRAFT");
            sendMsg(sender, "&aWyciszono " + target.getName() + " na " + (duration == Long.MAX_VALUE ? "perm" : formatDuration(duration)) + ".");
            return true;
        }
    }

    private class UnmuteCommand implements CommandExecutor {
        @Override
        public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
            if (!hasPermission(sender, "neptune.chat.unmute")) { sendMsg(sender, getMessage("no-permission", "&cNie masz uprawnień.")); return true; }
            if (args.length < 1) { sendMsg(sender, "&cUżycie: /unmute <gracz>"); return true; }
            Player target = Bukkit.getPlayer(args[0]);
            if (target == null) { sendMsg(sender, getMessage("player-not-found", "&cGracz nie jest online.")); return true; }
            if (mutedPlayers.contains(target.getUniqueId())) {
                unmutePlayer(target.getUniqueId(), "MINECRAFT");
                sendMsg(sender, "&aOdciszono " + target.getName() + ".");
            } else {
                sendMsg(sender, "&cGracz nie jest wyciszony.");
            }
            return true;
        }
    }

    private class WarnCommand implements CommandExecutor {
        @Override
        public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
            if (!hasPermission(sender, "neptune.chat.warn")) { sendMsg(sender, getMessage("no-permission", "&cNie masz uprawnień.")); return true; }
            if (args.length < 1) { sendMsg(sender, "&cUżycie: /warn <gracz> [powód]"); return true; }
            Player target = Bukkit.getPlayer(args[0]);
            if (target == null) { sendMsg(sender, getMessage("player-not-found", "&cGracz nie jest online.")); return true; }
            String reason = args.length > 1 ? String.join(" ", Arrays.copyOfRange(args, 1, args.length)) : "Brak powodu";
            warnPlayer(target.getUniqueId(), reason, "MINECRAFT");
            sendMsg(sender, "&aOstrzeżono " + target.getName() + ".");
            target.sendMessage(ChatColor.RED + "Ostrzeżenie: " + reason);
            return true;
        }
    }

    private class WarningsCommand implements CommandExecutor {
        private static final int PER_PAGE = 10;

        @Override
        public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
            if (!hasPermission(sender, "neptune.chat.warnings")) { sendMsg(sender, getMessage("no-permission", "&cNie masz uprawnień.")); return true; }
            if (args.length < 1) { sendMsg(sender, "&cUżycie: /warnings <gracz> [strona|clear]"); return true; }

            if (args[0].equalsIgnoreCase("clear")) {
                if (args.length < 2) { sendMsg(sender, "&cUżycie: /warnings clear <gracz>"); return true; }
                Player t = Bukkit.getPlayer(args[1]);
                if (t == null) { sendMsg(sender, getMessage("player-not-found", "&cGracz nie jest online.")); return true; }
                warnings.remove(t.getUniqueId());
                synchronized (dbLock) {
                    try (PreparedStatement ps = localDb.prepareStatement("DELETE FROM warnings WHERE player_uuid = ?")) {
                        ps.setString(1, t.getUniqueId().toString());
                        ps.executeUpdate();
                    } catch (SQLException e) {
                        getLogger().warning("Failed to clear warnings: " + e.getMessage());
                    }
                }
                sendMsg(sender, "&aWyczyściono ostrzeżenia gracza " + t.getName() + ".");
                return true;
            }

            Player target = Bukkit.getPlayer(args[0]);
            if (target == null) { sendMsg(sender, getMessage("player-not-found", "&cGracz nie jest online.")); return true; }

            List<WarnInfo> warns = warnings.getOrDefault(target.getUniqueId(), new ArrayList<>());
            if (warns.isEmpty()) {
                sendMsg(sender, "&aBrak ostrzeżeń.");
                return true;
            }

            int page = 1;
            if (args.length > 1) {
                try { page = Math.max(1, Integer.parseInt(args[1])); } catch (NumberFormatException ignored) { }
            }
            int totalPages = (int) Math.ceil((double) warns.size() / PER_PAGE);
            if (page > totalPages) page = totalPages;
            int start = (page - 1) * PER_PAGE;
            int end = Math.min(start + PER_PAGE, warns.size());

            sendMsg(sender, "&eOstrzeżenia " + target.getName() + " &7(strona " + page + "/" + totalPages + "):");
            for (int i = start; i < end; i++) {
                WarnInfo w = warns.get(i);
                long ago = System.currentTimeMillis() - w.createdAt();
                final int idx = i + 1;
                final String cmdClear = "/warnings clear " + target.getName();
                final String line = colorize("&7" + idx + ". &f" + w.reason() +
                        " &7- " + w.moderator() + " &8(" + formatDuration(ago) + " temu)");

                if (sender instanceof Player p) {
                    try {
                        net.md_5.bungee.api.chat.TextComponent comp = new net.md_5.bungee.api.chat.TextComponent(line);
                        comp.setHoverEvent(new net.md_5.bungee.api.chat.HoverEvent(
                                net.md_5.bungee.api.chat.HoverEvent.Action.SHOW_TEXT,
                                new net.md_5.bungee.api.chat.ComponentBuilder("&cKliknij, aby wyczyścić wszystkie ostrzeżenia").create()));
                        comp.setClickEvent(new net.md_5.bungee.api.chat.ClickEvent(
                                net.md_5.bungee.api.chat.ClickEvent.Action.RUN_COMMAND, cmdClear));
                        p.spigot().sendMessage(comp);
                    } catch (Throwable t) {
                        sendMsg(sender, "&7" + idx + ". &f" + w.reason() +
                                " &7- " + w.moderator() + " &8(" + formatDuration(ago) + " temu)");
                    }
                } else {
                    sendMsg(sender, "&7" + idx + ". &f" + w.reason() +
                            " &7- " + w.moderator() + " &8(" + formatDuration(ago) + " temu)");
                }
            }
            if (page < totalPages) {
                sendMsg(sender, "&7Następna strona: &e/warnings " + target.getName() + " " + (page + 1));
            }
            return true;
        }
    }

    private class HistoryCommand implements CommandExecutor {
        @Override
        public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
            if (!hasPermission(sender, "neptune.chat.history")) { sendMsg(sender, getMessage("no-permission", "&cNie masz uprawnień.")); return true; }
            if (args.length < 1) { sendMsg(sender, "&cUżycie: /history <gracz> [mute|warn|all]"); return true; }
            Player target = Bukkit.getPlayer(args[0]);
            UUID uuid;
            String name;
            if (target != null) {
                uuid = target.getUniqueId();
                name = target.getName();
            } else {
                var offline = Bukkit.getOfflinePlayer(args[0]);
                if (offline == null || offline.getUniqueId() == null) {
                    sendMsg(sender, getMessage("player-not-found", "&cGracz nie znaleziony."));
                    return true;
                }
                uuid = offline.getUniqueId();
                name = offline.getName() != null ? offline.getName() : args[0];
            }
            String filter = args.length > 1 ? args[1].toUpperCase() : "ALL";
            List<String[]> history = loadHistory(uuid, filter);
            if (history.isEmpty()) {
                sendMsg(sender, "&aBrak historii dla " + name + ".");
                return true;
            }
            sendMsg(sender, "&eHistoria " + name + " (" + history.size() + "):");
            for (int i = 0; i < Math.min(history.size(), 20); i++) {
                String[] h = history.get(i);
                long ago = System.currentTimeMillis() - Long.parseLong(h[4]);
                sendMsg(sender, "&7- &e" + h[0] + " &7| &f" + (h[1] != null ? h[1] : "brak powodu") +
                        " &7| " + h[2] + " &8(" + formatDuration(ago) + " temu)");
            }
            return true;
        }
    }

    private class ChatCommand implements CommandExecutor {
        @Override
        public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
            if (!hasPermission(sender, "neptune.chat.admin")) { sendMsg(sender, getMessage("no-permission", "&cNie masz uprawnień.")); return true; }
            if (args.length < 1) {
                sendMsg(sender, "&eUżycie: /chat <clear|lock|unlock|status|color|slowmode|broadcast|info|reload|help|ban|unban|kick|stats|note|tag|seen|playtime>");
                return true;
            }
            switch (args[0].toLowerCase()) {
                case "clear" -> { clearChat(); sendMsg(sender, getMessage("chat-cleared", "&aCzat wyczyszczony.")); }
                case "lock" -> {
                    long duration = 0;
                    String reason = "";
                    if (args.length > 1) {
                        duration = parseDuration(args[1]);
                        if (args.length > 2) reason = String.join(" ", Arrays.copyOfRange(args, 2, args.length));
                    }
                    lockChat(duration, reason, "MINECRAFT");
                    String timeStr = duration == 0 ? "PERMANENTNIE" : formatDuration(duration);
                    sendMsg(sender, "&aZablokowano czat na: " + timeStr);
                }
                case "unlock" -> { unlockChat("MINECRAFT"); sendMsg(sender, "&aOdblokowano czat."); }
                case "status" -> {
                    String status = chatLocked ? "&cZablokowany" : "&aOtwarty";
                    if (chatLocked && chatLockUntil > 0) {
                        status += " &7(pozostało: &e" + formatDuration(chatLockUntil - System.currentTimeMillis()) + "&7)";
                    }
                    sendMsg(sender, "&eStatus czatu: " + status);
                    sendMsg(sender, "&eSlowmode: &f" + slowmodeSeconds + "s");
                }
                case "slowmode" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat slowmode <sekundy>"); return true; }
                    try {
                        slowmodeSeconds = Math.max(0, Integer.parseInt(args[1]));
                        config.set("chat.slowmode-seconds", slowmodeSeconds);
                        saveConfig();
                        sendMsg(sender, "&aUstawiono slowmode na " + slowmodeSeconds + "s.");

                        Map<String, Object> event = new HashMap<>();
                        event.put("type", "SLOWMODE_CHANGED");
                        event.put("event_id", UUID.randomUUID().toString());
                        event.put("server_id", config.getString("backend.server-id"));
                        event.put("timestamp", System.currentTimeMillis());
                        Map<String, Object> payload = new HashMap<>();
                        payload.put("seconds", slowmodeSeconds);
                        event.put("payload", payload);
                        if (wsClient != null) wsClient.send(event);
                    } catch (NumberFormatException e) {
                        sendMsg(sender, "&cNieprawidłowa liczba!");
                    }
                }
                case "broadcast" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat broadcast <wiadomość>"); return true; }
                    String msg = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
                    String formatted = colorize("&8[&c&lOGŁOSZENIE&8] &f" + msg);
                    for (Player p : Bukkit.getOnlinePlayers()) {
                        p.sendMessage(formatted);
                        p.playSound(p.getLocation(), Sound.BLOCK_NOTE_BLOCK_PLING, 1f, 1.5f);
                    }
                    Bukkit.getConsoleSender().sendMessage(formatted);

                    Map<String, Object> event = new HashMap<>();
                    event.put("type", "BROADCAST");
                    event.put("event_id", UUID.randomUUID().toString());
                    event.put("server_id", config.getString("backend.server-id"));
                    event.put("timestamp", System.currentTimeMillis());
                    Map<String, Object> payload = new HashMap<>();
                    payload.put("message", msg);
                    payload.put("sender", sender.getName());
                    event.put("payload", payload);
                    if (wsClient != null) wsClient.send(event);
                }
                case "ban" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat ban <gracz> [czas] [powód]"); return true; }
                    Player t = Bukkit.getPlayer(args[0]);
                    if (t == null) { sendMsg(sender, "&cGracz nie jest online."); return true; }
                    long duration = args.length > 1 ? parseDuration(args[1]) : 0;
                    String reason = args.length > 2 ? String.join(" ", Arrays.copyOfRange(args, 2, args.length)) : "Brak powodu";
                    banPlayer(t.getUniqueId(), t.getName(), duration, reason, "MINECRAFT");
                    t.kickPlayer(colorize(getMessage("banned", "&cZostałeś zbanowany!\n&ePowód: &f{reason}")
                            .replace("{reason}", reason)));
                    sendMsg(sender, "&aZbanowano " + t.getName() + ".");
                }
                case "unban" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat unban <gracz>"); return true; }
                    OfflinePlayer off = Bukkit.getOfflinePlayer(args[1]);
                    if (off == null || off.getUniqueId() == null) {
                        sendMsg(sender, "&cGracz nie znaleziony.");
                        return true;
                    }
                    unbanPlayer(off.getUniqueId(), off.getName() != null ? off.getName() : args[1], "MINECRAFT");
                    sendMsg(sender, "&aOdbanowano " + args[1] + ".");
                }
                case "kick" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat kick <gracz> [powód]"); return true; }
                    Player t = Bukkit.getPlayer(args[0]);
                    if (t == null) { sendMsg(sender, "&cGracz nie jest online."); return true; }
                    String reason = args.length > 2 ? String.join(" ", Arrays.copyOfRange(args, 2, args.length)) : "Brak powodu";
                    t.kickPlayer(colorize(getMessage("kicked", "&cZostałeś wyrzucony!\n&ePowód: &f{reason}")
                            .replace("{reason}", reason)));
                    sendActionEvent("KICK", t.getUniqueId(), t.getName(), reason, "MINECRAFT");
                    sendMsg(sender, "&aWyrzucono " + t.getName() + ".");
                }
                case "stats" -> {
                    sendMsg(sender, "&e--- Statystyki serwera ---");
                    sendMsg(sender, "&7Online: &f" + Bukkit.getOnlinePlayers().size());
                    sendMsg(sender, "&7Wyciszonych: &f" + mutedPlayers.size());
                    sendMsg(sender, "&7Zbanowanych: &f" + bannedPlayers.size());
                    sendMsg(sender, "&7Wszystkich ostrzeżeń: &f" + warnings.values().stream().mapToInt(List::size).sum());
                    sendMsg(sender, "&7Filtrów: &f" + filterWords.size());
                    sendMsg(sender, "&7AutoMessages: &f" + autoMessages.size());
                    sendMsg(sender, "&7Slowmode: &f" + slowmodeSeconds + "s");
                    sendMsg(sender, "&7Czat: " + (chatLocked ? "&cZablokowany" : "&aOtwarty"));
                }
                case "note" -> {
                    if (args.length < 3) { sendMsg(sender, "&cUżycie: /chat note <gracz> <notatka>"); return true; }
                    Player t = Bukkit.getPlayer(args[0]);
                    UUID targetUuid;
                    String targetName;
                    if (t != null) { targetUuid = t.getUniqueId(); targetName = t.getName(); }
                    else {
                        var off = Bukkit.getOfflinePlayer(args[0]);
                        if (off == null || off.getUniqueId() == null) { sendMsg(sender, "&cGracz nie znaleziony."); return true; }
                        targetUuid = off.getUniqueId();
                        targetName = off.getName() != null ? off.getName() : args[0];
                    }
                    String text = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
                    NoteInfo note = new NoteInfo(UUID.randomUUID().toString(), text, sender.getName(), System.currentTimeMillis());
                    notes.computeIfAbsent(targetUuid, k -> new ArrayList<>()).add(note);
                    saveNoteToDb(targetUuid, note);
                    sendMsg(sender, "&aDodano notatkę do " + targetName + ".");
                }
                case "tag" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat tag <gracz> [tag|clear]"); return true; }
                    Player t = Bukkit.getPlayer(args[0]);
                    UUID targetUuid;
                    String targetName;
                    if (t != null) { targetUuid = t.getUniqueId(); targetName = t.getName(); }
                    else {
                        var off = Bukkit.getOfflinePlayer(args[0]);
                        if (off == null || off.getUniqueId() == null) { sendMsg(sender, "&cGracz nie znaleziony."); return true; }
                        targetUuid = off.getUniqueId();
                        targetName = off.getName() != null ? off.getName() : args[0];
                    }
                    if (args.length < 3) {
                        Set<String> tgs = tags.getOrDefault(targetUuid, Set.of());
                        sendMsg(sender, "&eTagi " + targetName + ": &f" + (tgs.isEmpty() ? "brak" : String.join(", ", tgs)));
                        return true;
                    }
                    if (args[1].equalsIgnoreCase("clear")) {
                        tags.remove(targetUuid);
                        synchronized (dbLock) {
                            try (PreparedStatement ps = localDb.prepareStatement("DELETE FROM tags WHERE player_uuid = ?")) {
                                ps.setString(1, targetUuid.toString());
                                ps.executeUpdate();
                            } catch (SQLException e) {
                                getLogger().warning("Failed to clear tags: " + e.getMessage());
                            }
                        }
                        sendMsg(sender, "&aWyczyściono tagi " + targetName + ".");
                    } else {
                        String tag = args[1];
                        tags.computeIfAbsent(targetUuid, k -> ConcurrentHashMap.newKeySet()).add(tag);
                        saveTagToDb(targetUuid, tag, sender.getName());
                        sendMsg(sender, "&aDodano tag &f" + tag + " &adla " + targetName + ".");
                    }
                }
                case "seen" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat seen <gracz>"); return true; }
                    OfflinePlayer off = Bukkit.getOfflinePlayer(args[0]);
                    if (off == null || off.getUniqueId() == null) { sendMsg(sender, "&cGracz nie znaleziony."); return true; }
                    Long last = playerLastSeen.get(off.getUniqueId());
                    if (last == null) { sendMsg(sender, "&eBrak danych o " + args[0] + "."); return true; }
                    sendMsg(sender, "&eOstatnio widziany: &f" + args[0] + " &7- &f" + formatDuration(System.currentTimeMillis() - last) + " temu");
                }
                case "playtime" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat playtime <gracz>"); return true; }
                    OfflinePlayer off = Bukkit.getOfflinePlayer(args[0]);
                    if (off == null || off.getUniqueId() == null) { sendMsg(sender, "&cGracz nie znaleziony."); return true; }
                    Long pt = playerPlaytime.get(off.getUniqueId());
                    if (pt == null) { sendMsg(sender, "&eBrak danych o " + args[0] + "."); return true; }
                    sendMsg(sender, "&eCzas gry: &f" + args[0] + " &7- &f" + formatDuration(pt));
                }
                case "info" -> {
                    if (args.length < 2) { sendMsg(sender, "&cUżycie: /chat info <gracz>"); return true; }
                    Player t = Bukkit.getPlayer(args[1]);
                    if (t == null) { sendMsg(sender, "&cGracz nie jest online."); return true; }
                    UUID tu = t.getUniqueId();
                    sendMsg(sender, "&eInfo o " + t.getName() + ":");
                    sendMsg(sender, "&7Kolor: &f" + playerColors.getOrDefault(tu, "brak"));
                    sendMsg(sender, "&7Wyciszony: &f" + (mutedPlayers.contains(tu) ? "tak" : "nie"));
                    sendMsg(sender, "&7Zbanowany: &f" + (bannedPlayers.contains(tu) ? "tak" : "nie"));
                    sendMsg(sender, "&7Ostrzeżenia: &f" + warnings.getOrDefault(tu, new ArrayList<>()).size());
                    sendMsg(sender, "&7Notatki: &f" + notes.getOrDefault(tu, new ArrayList<>()).size());
                    sendMsg(sender, "&7Tagi: &f" + String.join(", ", tags.getOrDefault(tu, Set.of())));
                    sendMsg(sender, "&7Czas gry: &f" + formatDuration(playerPlaytime.getOrDefault(tu, 0L)));
                    sendMsg(sender, "&7UUID: &f" + tu);
                }
                case "reload" -> {
                    reloadConfig();
                    config = getConfig();
                    filterWords.clear();
                    filterConfigs.clear();
                    loadFiltersFromConfig();
                    slowmodeSeconds = config.getInt("chat.slowmode-seconds", 0);
                    requestSync();
                    sendMsg(sender, "&aPrzeładowano konfigurację.");
                }
                case "color" -> {
                    if (args.length < 3) {
                        sendMsg(sender, "&cUżycie: /chat color <gracz> <nazwa_koloru|#RRGGBB|reset>");
                        return true;
                    }
                    Player target = Bukkit.getPlayer(args[1]);
                    if (target == null) { sendMsg(sender, "&cGracz nie jest online."); return true; }

                    if (args[2].equalsIgnoreCase("reset")) {
                        playerColors.remove(target.getUniqueId());
                        deletePlayerColorFromDb(target.getUniqueId());
                        sendMsg(sender, "&aZresetowano kolor dla " + target.getName() + ".");
                        return true;
                    }

                    boolean hexOnly = config.getBoolean("colors.hex-only", false);
                    String value = args[2];
                    String finalColor;

                    if (hexOnly) {
                        if (!isValidHex(value)) { sendMsg(sender, "&cNieprawidłowy HEX! Użyj #RRGGBB."); return true; }
                        finalColor = normalizeHex(value);
                    } else {
                        if (isValidHex(value)) {
                            finalColor = normalizeHex(value);
                        } else {
                            String code = colorNameToCode(value);
                            if (code == null) { sendMsg(sender, "&cNieprawidłowa nazwa koloru ani HEX!"); return true; }
                            finalColor = code;
                        }
                    }

                    playerColors.put(target.getUniqueId(), finalColor);
                    savePlayerColorToDb(target.getUniqueId(), finalColor);
                    sendMsg(sender, "&aUstawiono kolor dla " + target.getName() + ": " + finalColor);

                    if (config.getBoolean("colors.show-preview", true)) {
                        String preview = colorize(finalColor) + target.getName() + ChatColor.RESET + ": Przykładowa wiadomość";
                        sendMsg(sender, "&ePodgląd: " + preview);
                    }
                }
                case "help" -> {
                    sendMsg(sender, "&e--- NeptuneChat Help ---");
                    sendMsg(sender, "&e/chat clear &7- czyści czat");
                    sendMsg(sender, "&e/chat lock [czas] [powód] &7- blokuje czat");
                    sendMsg(sender, "&e/chat unlock &7- odblokowuje czat");
                    sendMsg(sender, "&e/chat slowmode <sek> &7- ustawia slowmode");
                    sendMsg(sender, "&e/chat broadcast <msg> &7- ogłoszenie");
                    sendMsg(sender, "&e/chat ban <gracz> [czas] [powód] &7- banuje");
                    sendMsg(sender, "&e/chat unban <gracz> &7- odbanowuje");
                    sendMsg(sender, "&e/chat kick <gracz> [powód] &7- wyrzuca");
                    sendMsg(sender, "&e/chat info <gracz> &7- info o graczu");
                    sendMsg(sender, "&e/chat note <gracz> <notatka> &7- dodaje notatkę");
                    sendMsg(sender, "&e/chat tag <gracz> <tag|clear> &7- tagi");
                    sendMsg(sender, "&e/chat seen <gracz> &7- ostatnio online");
                    sendMsg(sender, "&e/chat playtime <gracz> &7- czas gry");
                    sendMsg(sender, "&e/chat stats &7- statystyki");
                    sendMsg(sender, "&e/chat color <gracz> <kolor|HEX|reset>");
                    sendMsg(sender, "&e/chat reload &7- przeładowuje config");
                }
                default -> sendMsg(sender, "&cNieznana akcja.");
            }
            return true;
        }
    }

    private class NeptuneCommand implements CommandExecutor {
        @Override
        public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
            if (!hasPermission(sender, "neptune.chat.admin")) { sendMsg(sender, getMessage("no-permission", "&cNie masz uprawnień.")); return true; }
            if (args.length < 1) {
                sendMsg(sender, "&eNeptuneChat v" + getDescription().getVersion());
                sendMsg(sender, "&7Użycie: /nc <reload|status|sync|db>");
                return true;
            }
            switch (args[0].toLowerCase()) {
                case "reload" -> {
                    reloadConfig();
                    config = getConfig();
                    filterWords.clear();
                    filterConfigs.clear();
                    loadFiltersFromConfig();
                    loadAutoMessagesFromConfig();
                    slowmodeSeconds = config.getInt("chat.slowmode-seconds", 0);
                    chatLocked = config.getBoolean("chat.lock-default", false);
                    requestSync();
                    sendMsg(sender, "&aPrzeładowano konfigurację.");
                    sendMsg(sender, "&eFiltry: &f" + filterWords.size());
                }
                case "status" -> {
                    sendMsg(sender, "&eBackend: " + (connected.get() ? "&aPołączony" : "&cRozłączony"));
                    sendMsg(sender, "&eFiltry: &f" + filterWords.size());
                    sendMsg(sender, "&eWyciszenia: &f" + activeMutes.size());
                    sendMsg(sender, "&eBany: &f" + activeBans.size());
                    sendMsg(sender, "&eCzat: " + (chatLocked ? "&cZablokowany" : "&aOtwarty"));
                    sendMsg(sender, "&eSlowmode: &f" + slowmodeSeconds + "s");
                    sendMsg(sender, "&eAutoMessages: &f" + autoMessages.size());
                    sendMsg(sender, "&eKolorów graczy: &f" + playerColors.size());
                    sendMsg(sender, "&eZapisanych nicków: &f" + playerNames.size());
                    sendMsg(sender, "&eKolejka eventów: &f" + eventQueue.size());
                    sendMsg(sender, "&eReconnect attempts: &f" + reconnectAttempts);
                    sendMsg(sender, "&eVPN cache: &f" + vpnCache.size());
                    sendMsg(sender, "&eNotatek: &f" + notes.values().stream().mapToInt(List::size).sum());
                    sendMsg(sender, "&eTagów: &f" + tags.values().stream().mapToInt(Set::size).sum());
                }
                case "sync" -> {
                    if (wsClient != null && connected.get()) {
                        requestSync();
                        sendMsg(sender, "&aZażądano synchronizacji z backendem.");
                    } else {
                        sendMsg(sender, "&cBrak połączenia z backendem.");
                    }
                }
                case "db" -> {
                    Map<String, Integer> stats = getDbStats();
                    sendMsg(sender, "&eStatystyki lokalnej bazy danych:");
                    sendMsg(sender, "&7- Kolory graczy: &f" + stats.getOrDefault("player_colors", 0));
                    sendMsg(sender, "&7- Nicki graczy: &f" + stats.getOrDefault("player_names", 0));
                    sendMsg(sender, "&7- Ostrzeżenia: &f" + stats.getOrDefault("warnings", 0));
                    sendMsg(sender, "&7- Historia: &f" + stats.getOrDefault("history", 0));
                    sendMsg(sender, "&7- Notatki: &f" + stats.getOrDefault("notes", 0));
                    sendMsg(sender, "&7- Tagi: &f" + stats.getOrDefault("tags", 0));
                    sendMsg(sender, "&7- Bany: &f" + stats.getOrDefault("bans", 0));
                    sendMsg(sender, "&7- Aktywne kolory w pamięci: &f" + playerColors.size());
                    sendMsg(sender, "&ePlik: &f" + new File(getDataFolder(), "localdata.db").getAbsolutePath());
                }
                default -> sendMsg(sender, "&cNieznana akcja. Użyj: reload, status, sync, db");
            }
            return true;
        }
    }

    public static NeptuneChat getInstance() {
        return instance;
    }

    // ============================================================
    //                       JSON UTIL
    // ============================================================
    private static class JsonUtil {
        public static String toJson(Map<String, Object> map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (!first) sb.append(",");
                first = false;
                sb.append("\"").append(escape(entry.getKey())).append("\":");
                Object value = entry.getValue();
                if (value instanceof String) {
                    sb.append("\"").append(escape((String) value)).append("\"");
                } else if (value instanceof Number || value instanceof Boolean) {
                    sb.append(value);
                } else if (value instanceof Map) {
                    sb.append(toJson((Map<String, Object>) value));
                } else if (value instanceof List) {
                    sb.append("[");
                    boolean firstItem = true;
                    for (Object item : (List<?>) value) {
                        if (!firstItem) sb.append(",");
                        firstItem = false;
                        if (item instanceof String) sb.append("\"").append(escape((String) item)).append("\"");
                        else if (item instanceof Map) sb.append(toJson((Map<String, Object>) item));
                        else if (item instanceof Number || item instanceof Boolean) sb.append(item);
                        else if (item == null) sb.append("null");
                        else sb.append("\"").append(escape(item.toString())).append("\"");
                    }
                    sb.append("]");
                } else if (value == null) {
                    sb.append("null");
                } else {
                    sb.append("\"").append(escape(value.toString())).append("\"");
                }
            }
            sb.append("}");
            return sb.toString();
        }

        public static Map<String, Object> fromJson(String json) {
            Map<String, Object> map = new HashMap<>();
            if (json == null) return map;
            json = json.trim();
            if (!json.startsWith("{") || !json.endsWith("}")) return map;
            json = json.substring(1, json.length() - 1);
            List<String> pairs = splitJson(json);
            for (String pair : pairs) {
                int colon = findColon(pair);
                if (colon == -1) continue;
                String key = pair.substring(0, colon).trim();
                String value = pair.substring(colon + 1).trim();
                if (key.startsWith("\"") && key.endsWith("\"")) key = key.substring(1, key.length() - 1);
                map.put(key, parseValue(value));
            }
            return map;
        }

        private static int findColon(String s) {
            boolean inString = false;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '"' && (i == 0 || s.charAt(i-1) != '\\')) inString = !inString;
                if (c == ':' && !inString) return i;
            }
            return -1;
        }

        private static Object parseValue(String value) {
            if (value == null || value.isEmpty()) return null;
            if (value.equals("null")) return null;
            if (value.equals("true")) return Boolean.TRUE;
            if (value.equals("false")) return Boolean.FALSE;
            if (value.startsWith("\"") && value.endsWith("\"")) return unescape(value.substring(1, value.length() - 1));
            if (value.startsWith("{")) return fromJson(value);
            if (value.startsWith("[")) return parseList(value);
            try {
                if (value.contains(".")) return Double.parseDouble(value);
                return Long.parseLong(value);
            } catch (NumberFormatException e) { }
            return value;
        }

        private static List<Object> parseList(String value) {
            List<Object> list = new ArrayList<>();
            if (value.length() < 2) return list;
            String inner = value.substring(1, value.length() - 1);
            List<String> items = splitJson(inner);
            for (String item : items) list.add(parseValue(item.trim()));
            return list;
        }

        private static List<String> splitJson(String s) {
            List<String> result = new ArrayList<>();
            int depth = 0;
            boolean inString = false;
            StringBuilder current = new StringBuilder();
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '"' && (i == 0 || s.charAt(i-1) != '\\')) inString = !inString;
                if (!inString) {
                    if (c == '{' || c == '[') depth++;
                    if (c == '}' || c == ']') depth--;
                    if (c == ',' && depth == 0) {
                        result.add(current.toString());
                        current.setLength(0);
                        continue;
                    }
                }
                current.append(c);
            }
            if (current.length() > 0) result.add(current.toString());
            return result;
        }

        private static String escape(String s) {
            if (s == null) return "";
            return s.replace("\\", "\\\\").replace("\"", "\\\"")
                    .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
        }

        private static String unescape(String s) {
            return s.replace("\\\"", "\"").replace("\\\\", "\\")
                    .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        }
    }
}