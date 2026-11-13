package dev.caramelo.discovery.client;

import dev.caramelo.discovery.net.MulticastSettings;
import dev.caramelo.discovery.protocol.DiscoveryMessage;
import dev.caramelo.discovery.protocol.MessageCodec;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Escuta o grupo e entrega as mensagens recebidas ao handler. A {@link MembershipKey}
 * devolvida pelo join() controla a saída do grupo (drop) e o filtro de origens (block/unblock).
 */
public final class DiscoveryListener implements AutoCloseable {

    private final MulticastSettings settings;
    private final Consumer<ReceivedPacket> handler;
    private final DatagramChannel channel;
    private final String searcherId = "listener-" + UUID.randomUUID().toString().substring(0, 8);
    private final Set<InetAddress> blockedSources = Collections.synchronizedSet(new LinkedHashSet<>());
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong malformed = new AtomicLong();
    private final AtomicLong discarded = new AtomicLong();
    private volatile MembershipKey membershipKey;
    private volatile boolean closed;

    public DiscoveryListener(MulticastSettings settings, Consumer<ReceivedPacket> handler) throws IOException {
        this.settings = settings;
        this.handler = handler;

        this.channel = DatagramChannel.open(StandardProtocolFamily.INET);
        try {
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(settings.port()));
            // Opções de envio, usadas pelo SEARCH
            channel.setOption(StandardSocketOptions.IP_MULTICAST_IF, settings.networkInterface());
            channel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, 1);
            channel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
            join();
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }

        Thread.ofVirtual().name("discovery-listener").start(this::receiveLoop);
    }

    /** Os bloqueios pertencem à chave, então são reaplicados a cada novo join(). */
    public synchronized MembershipKey join() throws IOException {
        if (membershipKey != null && membershipKey.isValid()) {
            return membershipKey;
        }
        MembershipKey key = channel.join(settings.group(), settings.networkInterface());
        synchronized (blockedSources) {
            for (InetAddress source : blockedSources) {
                key.block(source);
            }
        }
        membershipKey = key;
        return key;
    }

    /** drop() faz o kernel enviar o IGMP leave e parar de entregar os datagramas do grupo. */
    public synchronized void leave() {
        if (membershipKey != null) {
            membershipKey.drop();
            membershipKey = null;
        }
    }

    public boolean isJoined() {
        MembershipKey key = membershipKey;
        return key != null && key.isValid();
    }

    /**
     * O kernel descarta os datagramas desse IP antes do receive(). O filtro é por IP,
     * então bloqueia todas as instâncias que rodam na mesma máquina.
     */
    public synchronized void block(InetAddress source) throws IOException {
        if (isJoined()) {
            membershipKey.block(source);
        }
        blockedSources.add(source);
    }

    public synchronized void unblock(InetAddress source) {
        if (isJoined() && blockedSources.contains(source)) {
            membershipKey.unblock(source);
        }
        blockedSources.remove(source);
    }

    public Set<InetAddress> blockedSources() {
        synchronized (blockedSources) {
            return Set.copyOf(blockedSources);
        }
    }

    /** Pede para as instâncias do serviço se anunciarem agora, como o M-SEARCH do SSDP. */
    public void search(String service) throws IOException {
        DiscoveryMessage.Search search = new DiscoveryMessage.Search(service, searcherId);
        channel.send(MessageCodec.encode(search), settings.groupSocketAddress());
    }

    private void receiveLoop() {
        ByteBuffer buffer = ByteBuffer.allocate(MessageCodec.MAX_DATAGRAM_SIZE);
        while (!closed) {
            try {
                buffer.clear();
                InetSocketAddress source = (InetSocketAddress) channel.receive(buffer);
                buffer.flip();

                // No Linux (IP_MULTICAST_ALL ligado por padrão) o socket ainda recebe o grupo
                // se outro socket da máquina estiver inscrito na mesma porta.
                if (!isJoined()) {
                    discarded.incrementAndGet();
                    continue;
                }

                DiscoveryMessage message = MessageCodec.decode(buffer);
                received.incrementAndGet();
                handler.accept(new ReceivedPacket(message, source, Instant.now()));
            } catch (ClosedChannelException e) {
                return;
            } catch (IllegalArgumentException e) {
                malformed.incrementAndGet();
            } catch (IOException e) {
                if (!closed) {
                    System.err.println("[listener] erro ao receber: " + e.getMessage());
                }
            } catch (RuntimeException e) {
                System.err.println("[listener] handler falhou: " + e);
            }
        }
    }

    public MulticastSettings settings() {
        return settings;
    }

    public String searcherId() {
        return searcherId;
    }

    public long receivedCount() {
        return received.get();
    }

    public long malformedCount() {
        return malformed.get();
    }

    public long discardedCount() {
        return discarded.get();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        leave();
        channel.close();
    }
}
