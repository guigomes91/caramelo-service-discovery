package dev.caramelo.discovery.server;

import dev.caramelo.discovery.net.MulticastSettings;
import dev.caramelo.discovery.protocol.DiscoveryMessage;
import dev.caramelo.discovery.protocol.DiscoveryMessage.Alive;
import dev.caramelo.discovery.protocol.DiscoveryMessage.Bye;
import dev.caramelo.discovery.protocol.DiscoveryMessage.Search;
import dev.caramelo.discovery.protocol.MessageCodec;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Anuncia uma instância no grupo: ALIVE periódico, resposta a SEARCH e BYE ao fechar.
 */
public final class ServiceAnnouncer implements AutoCloseable {

    public record Config(String service, String host, int port, Duration interval, Duration ttl) {
        public Config {
            if (service == null || service.isBlank() || service.contains("\n")) {
                throw new IllegalArgumentException("nome de serviço inválido");
            }
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("porta do serviço fora do intervalo: " + port);
            }
            if (interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("intervalo precisa ser positivo");
            }
            // Com TTL <= intervalo a instância expira entre dois anúncios. O padrão de 3x tolera dois ALIVE perdidos.
            if (ttl.compareTo(interval) <= 0) {
                throw new IllegalArgumentException("ttl precisa ser maior que o intervalo de anúncio");
            }
        }
    }

    private final MulticastSettings settings;
    private final Config config;
    private final String instanceId;
    private final DatagramChannel channel;
    private final MembershipKey membershipKey;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong seq = new AtomicLong();
    private final Consumer<DiscoveryMessage> onSent;
    private volatile boolean closed;

    private ServiceAnnouncer(MulticastSettings settings, Config config, Consumer<DiscoveryMessage> onSent)
            throws IOException {
        this.settings = settings;
        this.config = config;
        this.onSent = onSent;
        this.instanceId = config.service() + "-" + UUID.randomUUID().toString().substring(0, 8);

        this.channel = DatagramChannel.open(StandardProtocolFamily.INET);
        try {
            channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, settings.networkInterface());
            // TTL 1: o datagrama não passa do primeiro roteador
            channel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, 1);
            // Processos na mesma máquina também recebem o anúncio
            channel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
            // Announcers, watch e painel dividem a mesma porta
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            // Bind na porta do grupo para receber SEARCH
            channel.bind(new InetSocketAddress(settings.port()));
            this.membershipKey = channel.join(settings.group(), settings.networkInterface());
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }

        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("announcer-" + instanceId).factory());
    }

    public static ServiceAnnouncer start(MulticastSettings settings, Config config) throws IOException {
        return start(settings, config, message -> { });
    }

    public static ServiceAnnouncer start(MulticastSettings settings, Config config, Consumer<DiscoveryMessage> onSent)
            throws IOException {
        ServiceAnnouncer announcer = new ServiceAnnouncer(settings, config, onSent);
        announcer.scheduler.scheduleAtFixedRate(announcer::announceSafely, 0,
                config.interval().toMillis(), TimeUnit.MILLISECONDS);
        Thread.ofVirtual().name("announcer-search-" + announcer.instanceId).start(announcer::listenForSearches);
        return announcer;
    }

    private void announceSafely() {
        try {
            send(new Alive(config.service(), instanceId, config.host(), config.port(),
                    (int) config.ttl().toSeconds(), seq.incrementAndGet()));
        } catch (IOException e) {
            if (!closed) {
                System.err.println("[announcer] falha ao enviar ALIVE: " + e.getMessage());
            }
        }
    }

    private void send(DiscoveryMessage message) throws IOException {
        channel.send(MessageCodec.encode(message), settings.groupSocketAddress());
        onSent.accept(message);
    }

    /**
     * Responde SEARCH com um ALIVE extra. O atraso aleatório (como o MX do SSDP) evita que
     * todas as instâncias respondam ao mesmo tempo e estourem o buffer de quem perguntou.
     */
    private void listenForSearches() {
        ByteBuffer buffer = ByteBuffer.allocate(MessageCodec.MAX_DATAGRAM_SIZE);
        while (!closed) {
            try {
                buffer.clear();
                channel.receive(buffer);
                buffer.flip();
                if (MessageCodec.decode(buffer) instanceof Search search && search.matches(config.service())) {
                    long jitterMillis = ThreadLocalRandom.current().nextLong(20, 500);
                    scheduler.schedule(this::announceSafely, jitterMillis, TimeUnit.MILLISECONDS);
                }
            } catch (ClosedChannelException e) {
                return;
            } catch (IllegalArgumentException e) {
                // datagrama malformado ou de outro protocolo
            } catch (IOException e) {
                if (!closed) {
                    System.err.println("[announcer] erro ao receber: " + e.getMessage());
                }
            } catch (java.util.concurrent.RejectedExecutionException e) {
                return;
            }
        }
    }

    public String instanceId() {
        return instanceId;
    }

    public Config config() {
        return config;
    }

    public long lastSeq() {
        return seq.get();
    }

    /** Envia BYE para o grupo remover a instância sem esperar o TTL. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        scheduler.shutdownNow();
        try {
            send(new Bye(config.service(), instanceId));
        } catch (IOException e) {
            System.err.println("[announcer] não consegui enviar BYE: " + e.getMessage());
        }
        membershipKey.drop();
        try {
            channel.close();
        } catch (IOException ignored) {
        }
    }
}
