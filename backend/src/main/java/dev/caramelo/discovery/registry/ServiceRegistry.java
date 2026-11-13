package dev.caramelo.discovery.registry;

import dev.caramelo.discovery.client.ReceivedPacket;
import dev.caramelo.discovery.protocol.DiscoveryMessage.Alive;
import dev.caramelo.discovery.protocol.DiscoveryMessage.Bye;
import dev.caramelo.discovery.protocol.DiscoveryMessage.Search;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Instâncias descobertas no grupo. Uma instância sai do registro ao enviar BYE
 * ou quando o TTL vence sem um novo ALIVE.
 */
public final class ServiceRegistry implements AutoCloseable {

    public enum EventType { REGISTERED, RENEWED, DEREGISTERED, EXPIRED }

    public record RegistryEvent(EventType type, ServiceInstance instance) {
    }

    private final Map<String, ServiceInstance> instances = new ConcurrentHashMap<>();
    private final List<Consumer<RegistryEvent>> listeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService sweeper;
    private final Clock clock;

    public ServiceRegistry() {
        this(Clock.systemUTC());
    }

    public ServiceRegistry(Clock clock) {
        this.clock = clock;
        this.sweeper = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("registry-sweeper").factory());
        sweeper.scheduleAtFixedRate(this::expireStale, 500, 500, TimeUnit.MILLISECONDS);
    }

    public void onEvent(Consumer<RegistryEvent> listener) {
        listeners.add(listener);
    }

    public void accept(ReceivedPacket packet) {
        switch (packet.message()) {
            case Alive alive -> register(alive, packet);
            case Bye bye -> deregister(bye);
            case Search ignored -> { }
        }
    }

    private void register(Alive alive, ReceivedPacket packet) {
        Instant now = clock.instant();
        var outcome = new Object() {
            EventType type;
            ServiceInstance instance;
        };
        instances.compute(alive.instanceId(), (id, previous) -> {
            // UDP não garante ordem: ALIVE atrasado ou duplicado (seq menor ou igual) é descartado
            if (previous != null && alive.seq() <= previous.seq()) {
                return previous;
            }
            outcome.type = previous == null ? EventType.REGISTERED : EventType.RENEWED;
            outcome.instance = new ServiceInstance(alive.service(), alive.instanceId(), alive.host(), alive.port(),
                    Duration.ofSeconds(alive.ttlSeconds()), previous == null ? now : previous.firstSeen(), now,
                    alive.seq(), packet.source().getAddress().getHostAddress() + ":" + packet.source().getPort());
            return outcome.instance;
        });
        if (outcome.type != null) {
            publish(outcome.type, outcome.instance);
        }
    }

    private void deregister(Bye bye) {
        ServiceInstance removed = instances.remove(bye.instanceId());
        if (removed != null) {
            publish(EventType.DEREGISTERED, removed);
        }
    }

    private void expireStale() {
        Instant now = clock.instant();
        instances.values().forEach(instance -> {
            if (instance.isExpired(now) && instances.remove(instance.instanceId(), instance)) {
                publish(EventType.EXPIRED, instance);
            }
        });
    }

    private void publish(EventType type, ServiceInstance instance) {
        RegistryEvent event = new RegistryEvent(type, instance);
        for (Consumer<RegistryEvent> listener : listeners) {
            try {
                listener.accept(event);
            } catch (RuntimeException e) {
                System.err.println("[registry] listener falhou: " + e);
            }
        }
    }

    public List<ServiceInstance> instances() {
        return instances.values().stream()
                .sorted(Comparator.comparing(ServiceInstance::service).thenComparing(ServiceInstance::firstSeen))
                .toList();
    }

    public List<ServiceInstance> lookup(String service) {
        return instances().stream().filter(instance -> instance.service().equals(service)).toList();
    }

    @Override
    public void close() {
        sweeper.shutdownNow();
    }
}
