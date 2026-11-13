package dev.caramelo.discovery.protocol;

import dev.caramelo.discovery.protocol.DiscoveryMessage.Alive;
import dev.caramelo.discovery.protocol.DiscoveryMessage.Bye;
import dev.caramelo.discovery.protocol.DiscoveryMessage.Search;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Codifica as mensagens em texto, com cabeçalhos "chave: valor", como o SSDP faz:
 *
 * <pre>
 * CARAMELO/1 ALIVE
 * service: orders-service
 * instance: orders-service-3f2a9c1d
 * host: 192.168.0.10
 * port: 8080
 * ttl: 15
 * seq: 42
 * </pre>
 */
public final class MessageCodec {

    public static final String PROTOCOL = "CARAMELO/1";

    /** Abaixo do MTU de 1500 menos os cabeçalhos IP/UDP, para o datagrama não ser fragmentado. */
    public static final int MAX_DATAGRAM_SIZE = 1_400;

    private MessageCodec() {
    }

    public static ByteBuffer encode(DiscoveryMessage message) {
        StringBuilder text = new StringBuilder(PROTOCOL).append(' ');
        switch (message) {
            case Alive alive -> {
                text.append("ALIVE\n");
                header(text, "service", alive.service());
                header(text, "instance", alive.instanceId());
                header(text, "host", alive.host());
                header(text, "port", alive.port());
                header(text, "ttl", alive.ttlSeconds());
                header(text, "seq", alive.seq());
            }
            case Bye bye -> {
                text.append("BYE\n");
                header(text, "service", bye.service());
                header(text, "instance", bye.instanceId());
            }
            case Search search -> {
                text.append("SEARCH\n");
                header(text, "service", search.service());
                header(text, "searcher", search.searcherId());
            }
        }
        byte[] bytes = text.toString().getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_DATAGRAM_SIZE) {
            throw new IllegalArgumentException("mensagem maior que um datagrama seguro: " + bytes.length + " bytes");
        }
        return ByteBuffer.wrap(bytes);
    }

    /** Qualquer processo da rede pode enviar para o grupo, então o conteúdo é sempre validado. */
    public static DiscoveryMessage decode(ByteBuffer datagram) {
        String text = StandardCharsets.UTF_8.decode(datagram).toString();
        String[] lines = text.split("\r?\n");
        String[] startLine = lines[0].trim().split(" ");
        if (startLine.length != 2 || !PROTOCOL.equals(startLine[0])) {
            throw new MalformedMessageException("protocolo desconhecido: " + lines[0]);
        }

        Map<String, String> headers = new HashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon > 0) {
                headers.put(lines[i].substring(0, colon).trim().toLowerCase(), lines[i].substring(colon + 1).trim());
            }
        }

        try {
            return switch (startLine[1]) {
                case "ALIVE" -> new Alive(
                        headers.get("service"),
                        headers.get("instance"),
                        headers.get("host"),
                        Integer.parseInt(headers.getOrDefault("port", "")),
                        Integer.parseInt(headers.getOrDefault("ttl", "")),
                        Long.parseLong(headers.getOrDefault("seq", "0")));
                case "BYE" -> new Bye(headers.get("service"), headers.get("instance"));
                case "SEARCH" -> new Search(headers.getOrDefault("service", DiscoveryMessage.ALL_SERVICES),
                        headers.get("searcher"));
                default -> throw new MalformedMessageException("tipo de mensagem desconhecido: " + startLine[1]);
            };
        } catch (IllegalArgumentException e) {
            throw e instanceof MalformedMessageException mme ? mme : new MalformedMessageException(e.getMessage());
        }
    }

    private static void header(StringBuilder text, String name, Object value) {
        text.append(name).append(": ").append(value).append('\n');
    }

    public static final class MalformedMessageException extends IllegalArgumentException {
        public MalformedMessageException(String message) {
            super(message);
        }
    }
}
