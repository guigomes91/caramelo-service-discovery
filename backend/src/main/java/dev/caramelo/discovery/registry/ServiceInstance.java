package dev.caramelo.discovery.registry;

import java.time.Duration;
import java.time.Instant;

/**
 * @param source IP:porta de onde o datagrama saiu, que pode ser diferente do host anunciado
 */
public record ServiceInstance(String service, String instanceId, String host, int port, Duration ttl,
                              Instant firstSeen, Instant lastSeen, long seq, String source) {

    public Instant expiresAt() {
        return lastSeen.plus(ttl);
    }

    public boolean isExpired(Instant now) {
        return now.isAfter(expiresAt());
    }

    public String address() {
        return host + ":" + port;
    }
}
