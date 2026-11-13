package dev.caramelo.discovery.protocol;

/**
 * Mensagens do protocolo, equivalentes ao NOTIFY alive/byebye e ao M-SEARCH do SSDP.
 */
public sealed interface DiscoveryMessage
        permits DiscoveryMessage.Alive, DiscoveryMessage.Bye, DiscoveryMessage.Search {

    String ALL_SERVICES = "*";

    String service();

    record Alive(String service, String instanceId, String host, int port, int ttlSeconds, long seq)
            implements DiscoveryMessage {
        public Alive {
            requireText(service, "service");
            requireText(instanceId, "instance");
            requireText(host, "host");
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("porta fora do intervalo: " + port);
            }
            if (ttlSeconds < 1) {
                throw new IllegalArgumentException("ttl precisa ser positivo: " + ttlSeconds);
            }
        }
    }

    record Bye(String service, String instanceId) implements DiscoveryMessage {
        public Bye {
            requireText(service, "service");
            requireText(instanceId, "instance");
        }
    }

    record Search(String service, String searcherId) implements DiscoveryMessage {
        public Search {
            requireText(service, "service");
            requireText(searcherId, "searcher");
        }

        public boolean matches(String candidate) {
            return ALL_SERVICES.equals(service) || service.equals(candidate);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("campo obrigatório ausente: " + field);
        }
    }
}
