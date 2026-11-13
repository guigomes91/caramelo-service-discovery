package dev.caramelo.discovery.dashboard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.caramelo.discovery.client.DiscoveryListener;
import dev.caramelo.discovery.client.ReceivedPacket;
import dev.caramelo.discovery.net.MulticastSettings;
import dev.caramelo.discovery.protocol.DiscoveryMessage;
import dev.caramelo.discovery.registry.ServiceInstance;
import dev.caramelo.discovery.registry.ServiceRegistry;
import dev.caramelo.discovery.registry.ServiceRegistry.RegistryEvent;
import dev.caramelo.discovery.server.ServiceAnnouncer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;

/**
 * O navegador não entra em grupo multicast, então o painel participa do grupo pelo backend
 * e expõe join/drop/block/announce por HTTP e os eventos recebidos por SSE.
 */
public final class DashboardServer implements AutoCloseable {

    private static final int RECENT_PACKETS = 150;

    private final MulticastSettings settings;
    private final ServiceRegistry registry = new ServiceRegistry();
    private final DiscoveryListener listener;
    private final Map<String, ServiceAnnouncer> announcers = new ConcurrentHashMap<>();
    private final Deque<Map<String, Object>> recentPackets = new ConcurrentLinkedDeque<>();
    private final SseHub hub = new SseHub();
    private final HttpServer http;
    private final Path frontendDir;
    private final Instant startedAt = Instant.now();

    private DashboardServer(MulticastSettings settings, int httpPort, Path frontendDir) throws IOException {
        this.settings = settings;
        this.frontendDir = frontendDir;
        this.listener = new DiscoveryListener(settings, this::onPacket);
        registry.onEvent(this::onRegistryEvent);

        this.http = HttpServer.create(new InetSocketAddress(httpPort), 0);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/api/", this::handleApi);
        http.createContext("/", this::handleStatic);
    }

    public static DashboardServer start(MulticastSettings settings, int httpPort, Path frontendDir) throws IOException {
        DashboardServer server = new DashboardServer(settings, httpPort, frontendDir);
        server.http.start();
        server.listener.search(DiscoveryMessage.ALL_SERVICES);
        System.out.println("Painel em http://localhost:" + httpPort + "  (frontend: " + frontendDir + ")");
        return server;
    }

    private void onPacket(ReceivedPacket packet) {
        registry.accept(packet);
        Map<String, Object> event = packetJson(packet);
        recentPackets.addFirst(event);
        while (recentPackets.size() > RECENT_PACKETS) {
            recentPackets.pollLast();
        }
        hub.broadcast(Json.write(event));
    }

    private void onRegistryEvent(RegistryEvent event) {
        hub.broadcast(Json.write(map("type", "registry", "event", event.type(), "instance", instanceJson(event.instance()))));
    }

    private void broadcastStatus(String note) {
        hub.broadcast(Json.write(map("type", "status", "status", statusJson(), "note", note)));
    }


    private void handleApi(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        try {
            if (method.equals("OPTIONS")) {
                send(exchange, 204, null);
                return;
            }
            switch (method + " " + path) {
                case "GET /api/events" -> hub.stream(exchange, Json.write(snapshotJson()));
                case "GET /api/snapshot" -> send(exchange, 200, snapshotJson());
                case "GET /api/instances" -> send(exchange, 200, registry.instances().stream().map(this::instanceJson).toList());
                case "POST /api/membership/join" -> {
                    listener.join();
                    listener.search(DiscoveryMessage.ALL_SERVICES);
                    broadcastStatus("join(): inscrito no grupo " + settings.group().getHostAddress());
                    send(exchange, 200, statusJson());
                }
                case "POST /api/membership/leave" -> {
                    listener.leave();
                    broadcastStatus("drop(): saiu do grupo. As instâncias vão expirar pelo TTL");
                    send(exchange, 200, statusJson());
                }
                case "POST /api/search" -> {
                    String service = query.getOrDefault("service", DiscoveryMessage.ALL_SERVICES);
                    listener.search(service.isBlank() ? DiscoveryMessage.ALL_SERVICES : service);
                    send(exchange, 202, map("searched", service));
                }
                case "POST /api/sources/block" -> {
                    InetAddress source = InetAddress.getByName(require(query, "address"));
                    listener.block(source);
                    broadcastStatus("block(" + source.getHostAddress() + "): datagramas dessa origem descartados pelo kernel");
                    send(exchange, 200, statusJson());
                }
                case "POST /api/sources/unblock" -> {
                    InetAddress source = InetAddress.getByName(require(query, "address"));
                    listener.unblock(source);
                    broadcastStatus("unblock(" + source.getHostAddress() + ")");
                    send(exchange, 200, statusJson());
                }
                case "POST /api/announcers" -> {
                    int interval = Integer.parseInt(query.getOrDefault("interval", "5"));
                    var config = new ServiceAnnouncer.Config(
                            require(query, "service").trim(),
                            query.getOrDefault("host", settings.localAddress().getHostAddress()),
                            Integer.parseInt(require(query, "port")),
                            Duration.ofSeconds(interval),
                            Duration.ofSeconds(Integer.parseInt(query.getOrDefault("ttl", String.valueOf(interval * 3)))));
                    ServiceAnnouncer announcer = ServiceAnnouncer.start(settings, config);
                    announcers.put(announcer.instanceId(), announcer);
                    broadcastStatus("announcer " + announcer.instanceId() + " iniciado");
                    send(exchange, 201, announcerJson(announcer));
                }
                default -> {
                    if (method.equals("DELETE") && path.startsWith("/api/announcers/")) {
                        String id = path.substring("/api/announcers/".length());
                        ServiceAnnouncer announcer = announcers.remove(id);
                        if (announcer == null) {
                            send(exchange, 404, map("error", "announcer não encontrado: " + id));
                            return;
                        }
                        announcer.close();
                        broadcastStatus("announcer " + id + " parado com BYE");
                        send(exchange, 200, statusJson());
                    } else {
                        send(exchange, 404, map("error", "rota não encontrada: " + method + " " + path));
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            send(exchange, 400, map("error", e.getMessage()));
        } catch (UnsupportedOperationException e) {
            send(exchange, 501, map("error", "o sistema operacional não suporta essa operação: " + e.getMessage()));
        } catch (IOException e) {
            send(exchange, 500, map("error", e.getMessage()));
        }
    }


    private Map<String, Object> snapshotJson() {
        return map("type", "snapshot",
                "serverTime", Instant.now().toEpochMilli(),
                "status", statusJson(),
                "instances", registry.instances().stream().map(this::instanceJson).toList(),
                "packets", new ArrayList<>(recentPackets));
    }

    private Map<String, Object> statusJson() {
        List<String> blocked = listener.blockedSources().stream().map(InetAddress::getHostAddress).sorted().toList();
        return map("group", settings.group().getHostAddress(),
                "port", settings.port(),
                "interfaceName", settings.networkInterface().getName(),
                "interfaceDisplayName", settings.networkInterface().getDisplayName(),
                "localAddress", settings.localAddress().getHostAddress(),
                "joined", listener.isJoined(),
                "searcherId", listener.searcherId(),
                "blockedSources", blocked,
                "announcers", announcers.values().stream().map(this::announcerJson).toList(),
                "stats", map("received", listener.receivedCount(), "malformed", listener.malformedCount(),
                        "discarded", listener.discardedCount()),
                "startedAt", startedAt.toEpochMilli());
    }

    private Map<String, Object> instanceJson(ServiceInstance instance) {
        return map("service", instance.service(),
                "instanceId", instance.instanceId(),
                "host", instance.host(),
                "port", instance.port(),
                "ttlSeconds", instance.ttl().toSeconds(),
                "firstSeen", instance.firstSeen().toEpochMilli(),
                "lastSeen", instance.lastSeen().toEpochMilli(),
                "expiresAt", instance.expiresAt().toEpochMilli(),
                "seq", instance.seq(),
                "source", instance.source(),
                "local", announcers.containsKey(instance.instanceId()));
    }

    private Map<String, Object> announcerJson(ServiceAnnouncer announcer) {
        var config = announcer.config();
        return map("instanceId", announcer.instanceId(),
                "service", config.service(),
                "host", config.host(),
                "port", config.port(),
                "intervalSeconds", config.interval().toSeconds(),
                "ttlSeconds", config.ttl().toSeconds(),
                "seq", announcer.lastSeq());
    }

    private static Map<String, Object> packetJson(ReceivedPacket packet) {
        Map<String, Object> fields = switch (packet.message()) {
            case DiscoveryMessage.Alive a -> map("kind", "ALIVE", "service", a.service(), "instanceId", a.instanceId(),
                    "host", a.host(), "port", a.port(), "ttlSeconds", a.ttlSeconds(), "seq", a.seq());
            case DiscoveryMessage.Bye b -> map("kind", "BYE", "service", b.service(), "instanceId", b.instanceId());
            case DiscoveryMessage.Search s -> map("kind", "SEARCH", "service", s.service(), "searcherId", s.searcherId());
        };
        Map<String, Object> event = map("type", "packet",
                "at", packet.receivedAt().toEpochMilli(),
                "sourceAddress", packet.source().getAddress().getHostAddress(),
                "sourcePort", packet.source().getPort());
        event.putAll(fields);
        return event;
    }

    private static Map<String, Object> map(Object... keyValues) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }


    private static void send(HttpExchange exchange, int status, Object body) throws IOException {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        if (body == null) {
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
            return;
        }
        byte[] bytes = Json.write(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> params = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return params;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            params.put(key, value);
        }
        return params;
    }

    private static String require(Map<String, String> query, String key) {
        String value = query.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("parâmetro obrigatório: " + key);
        }
        return value;
    }

    private void handleStatic(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        Path file = frontendDir.resolve(path.equals("/") ? "index.html" : path.substring(1)).normalize();
        if (!file.startsWith(frontendDir) || !Files.isRegularFile(file)) {
            byte[] notFound = "404".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(404, notFound.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(notFound);
            }
            return;
        }
        String name = file.getFileName().toString();
        String type = name.endsWith(".html") ? "text/html; charset=utf-8"
                : name.endsWith(".css") ? "text/css; charset=utf-8"
                : name.endsWith(".js") ? "text/javascript; charset=utf-8"
                : name.endsWith(".svg") ? "image/svg+xml"
                : "application/octet-stream";
        byte[] bytes = Files.readAllBytes(file);
        exchange.getResponseHeaders().set("Content-Type", type);
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Procura a pasta frontend/ a partir do diretório atual (raiz do projeto ou backend/). */
    public static Path locateFrontend(String explicit) {
        if (explicit != null) {
            return Path.of(explicit).toAbsolutePath().normalize();
        }
        for (String candidate : List.of("frontend", "../frontend")) {
            Path path = Path.of(candidate).toAbsolutePath().normalize();
            if (Files.isRegularFile(path.resolve("index.html"))) {
                return path;
            }
        }
        return Path.of("frontend").toAbsolutePath().normalize();
    }

    @Override
    public void close() {
        announcers.values().forEach(ServiceAnnouncer::close);
        announcers.clear();
        try {
            listener.close();
        } catch (IOException ignored) {
        }
        registry.close();
        http.stop(0);
    }
}
