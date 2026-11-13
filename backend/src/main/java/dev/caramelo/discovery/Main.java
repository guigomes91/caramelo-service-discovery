package dev.caramelo.discovery;

import dev.caramelo.discovery.client.DiscoveryListener;
import dev.caramelo.discovery.dashboard.DashboardServer;
import dev.caramelo.discovery.net.MulticastSettings;
import dev.caramelo.discovery.net.NetworkInterfaces;
import dev.caramelo.discovery.protocol.DiscoveryMessage;
import dev.caramelo.discovery.registry.ServiceRegistry;
import dev.caramelo.discovery.server.ServiceAnnouncer;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;

/**
 * <pre>
 *   java -jar caramelo-discovery.jar announce  --service orders-service --port 8080
 *   java -jar caramelo-discovery.jar watch
 *   java -jar caramelo-discovery.jar dashboard --http-port 8090
 *   java -jar caramelo-discovery.jar interfaces
 * </pre>
 * Opções comuns: --group 239.255.42.99 --mport 4446 --iface eth0
 */
public final class Main {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            usage();
            return;
        }
        Args args = new Args(argv, 1);
        switch (argv[0]) {
            case "announce" -> announce(args);
            case "watch" -> watch(args);
            case "dashboard" -> dashboard(args);
            case "interfaces" -> NetworkInterfaces.listUsable().forEach(System.out::println);
            default -> usage();
        }
    }

    private static MulticastSettings settings(Args args) throws java.io.IOException {
        MulticastSettings settings = MulticastSettings.of(
                args.get("group", MulticastSettings.DEFAULT_GROUP),
                args.getInt("mport", MulticastSettings.DEFAULT_PORT),
                args.get("iface", null));
        System.out.println("Grupo: " + settings.describe());
        return settings;
    }

    private static void announce(Args args) throws Exception {
        MulticastSettings settings = settings(args);
        int interval = args.getInt("interval", 5);
        var config = new ServiceAnnouncer.Config(
                args.get("service", "orders-service"),
                args.get("host", settings.localAddress().getHostAddress()),
                args.getInt("port", 8080),
                Duration.ofSeconds(interval),
                Duration.ofSeconds(args.getInt("ttl", interval * 3)));

        ServiceAnnouncer announcer = ServiceAnnouncer.start(settings, config, message -> log("→ " + describe(message)));
        System.out.println("Anunciando " + announcer.instanceId() + " em " + config.host() + ":" + config.port()
                + " (Ctrl+C envia BYE)");

        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            announcer.close();
            done.countDown();
        }));
        done.await();
    }

    private static void watch(Args args) throws Exception {
        MulticastSettings settings = settings(args);
        ServiceRegistry registry = new ServiceRegistry();
        registry.onEvent(event -> log("%-12s %s %s (seq %d)".formatted(event.type(), event.instance().instanceId(),
                event.instance().address(), event.instance().seq())));

        try (DiscoveryListener listener = new DiscoveryListener(settings, registry::accept)) {
            listener.search(DiscoveryMessage.ALL_SERVICES);
            log("SEARCH * enviado, aguardando anúncios...");
            Runtime.getRuntime().addShutdownHook(new Thread(registry::close));
            Thread.currentThread().join();
        }
    }

    private static void dashboard(Args args) throws Exception {
        MulticastSettings settings = settings(args);
        Path frontend = DashboardServer.locateFrontend(args.get("frontend", null));
        DashboardServer server = DashboardServer.start(settings, args.getInt("http-port", 8090), frontend);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }

    private static String describe(DiscoveryMessage message) {
        return switch (message) {
            case DiscoveryMessage.Alive a -> "ALIVE %s %s:%d seq=%d ttl=%ds".formatted(a.instanceId(), a.host(),
                    a.port(), a.seq(), a.ttlSeconds());
            case DiscoveryMessage.Bye b -> "BYE   " + b.instanceId();
            case DiscoveryMessage.Search s -> "SEARCH " + s.service();
        };
    }

    private static void log(String line) {
        System.out.println("[" + LocalTime.now().format(TIME) + "] " + line);
    }

    private static void usage() {
        System.out.println("""
                Uso: java -jar caramelo-discovery.jar <comando> [opções]

                Comandos:
                  announce    anuncia uma instância (--service, --port, --host, --interval, --ttl)
                  watch       entra no grupo e lista as instâncias descobertas
                  dashboard   painel web + API (--http-port 8090, --frontend ../frontend)
                  interfaces  lista as interfaces com IPv4 e multicast

                Opções comuns: --group 239.255.42.99  --mport 4446  --iface <nome>
                """);
    }
}
