package com.neptune.chat;

import me.clip.placeholderapi.expansion.PlaceholderExpansion;
import org.bukkit.OfflinePlayer;

/**
 * PlaceholderAPI expansion for NeptuneChat.
 *
 * Dostepne placeholdery (%neptune_<key>%):
 *   muted            -> true/false
 *   mute_time        -> pozostaly czas mute (np. "1h 5m")
 *   banned           -> true/false
 *   ban_time         -> pozostaly czas bana (np. "3d 2h")
 *   warnings         -> liczba ostrzezen
 *   color            -> surowy kolor gracza (np. "&a" lub "#RRGGBB")
 *   chat_locked      -> true/false
 *   slowmode         -> liczba sekund slowmode
 *   notes            -> liczba notatek
 *   tags             -> liczba tagow
 *   playtime         -> sformatowany czas gry
 *   last_seen        -> "5m temu" / "nigdy" / "teraz"
 */
public class NeptuneExpansion extends PlaceholderExpansion {

    private final NeptuneChat plugin;

    public NeptuneExpansion(NeptuneChat plugin) {
        this.plugin = plugin;
    }

    @Override
    public String getIdentifier() {
        return "neptune";
    }

    @Override
    public String getAuthor() {
        return "Neptune";
    }

    @Override
    public String getVersion() {
        // getDescription() jest deprecated na Paper 1.20.6+, ale wciaz dziala.
        // Uzywamy go dla kompatybilnosci wstecznej (plugin.yml / pom.xml).
        return plugin.getDescription().getVersion();
    }

    @Override
    public boolean persist() {
        // Dzieki temu expansion nie jest usuwany przy /papi reload
        return true;
    }

    @Override
    public boolean canRegister() {
        return true;
    }

    @Override
    public String onRequest(OfflinePlayer player, String params) {
        if (params == null) return "";

        // Placeholdery globalne (nie wymagaja gracza)
        switch (params.toLowerCase()) {
            case "chat_locked":
                return String.valueOf(plugin.isChatLocked());
            case "slowmode":
                return String.valueOf(plugin.getSlowmodeSeconds());
            case "server_version":
                return plugin.getDescription().getVersion();
        }

        // Placeholdery per-gracz
        if (player == null || player.getUniqueId() == null) return "";

        try {
            return switch (params.toLowerCase()) {
                // --- MUTE ---
                case "muted"          -> String.valueOf(plugin.isMuted(player.getUniqueId()));
                case "mute_time"      -> plugin.getMuteTimeFormatted(player.getUniqueId());

                // --- BAN (NOWE) ---
                case "banned"         -> String.valueOf(plugin.isBanned(player.getUniqueId()));
                case "ban_time"       -> plugin.getBanTimeFormatted(player.getUniqueId());

                // --- WARNINGS ---
                case "warnings"       -> String.valueOf(plugin.getWarningsCount(player.getUniqueId()));

                // --- KOLOR ---
                case "color"          -> plugin.getRawColor(player.getUniqueId());

                // --- NOTATKI / TAGI ---
                case "notes"          -> String.valueOf(plugin.getNotesCount(player.getUniqueId()));
                case "tags"           -> String.valueOf(plugin.getTagsCount(player.getUniqueId()));

                // --- CZAS GRY / OSTATNIO WIDZIANY ---
                case "playtime"       -> plugin.getPlaytimeFormatted(player.getUniqueId());
                case "last_seen"      -> plugin.getLastSeenFormatted(player.getUniqueId());

                default -> null;
            };
        } catch (Exception e) {
            // Nie logujemy spamu - PAPI czesto odpytuje placeholdery dla offline graczy
            return "";
        }
    }
}
