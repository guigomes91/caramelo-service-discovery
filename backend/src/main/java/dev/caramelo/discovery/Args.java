package dev.caramelo.discovery;

import java.util.HashMap;
import java.util.Map;

/** Lê "--chave valor", com a variável de ambiente CARAMELO_CHAVE como fallback. */
public final class Args {

    private final Map<String, String> values = new HashMap<>();

    public Args(String[] args, int from) {
        for (int i = from; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("argumento inesperado: " + args[i]);
            }
            String key = args[i].substring(2);
            String value = i + 1 < args.length && !args[i + 1].startsWith("--") ? args[++i] : "true";
            values.put(key, value);
        }
    }

    public String get(String key, String fallback) {
        String value = values.get(key);
        if (value != null) {
            return value;
        }
        String env = System.getenv("CARAMELO_" + key.toUpperCase().replace('-', '_'));
        return env != null ? env : fallback;
    }

    public int getInt(String key, int fallback) {
        return Integer.parseInt(get(key, String.valueOf(fallback)));
    }
}
