package dev.caramelo.discovery.client;

import dev.caramelo.discovery.protocol.DiscoveryMessage;

import java.net.InetSocketAddress;
import java.time.Instant;

/** Uma mensagem válida que chegou do grupo, com o remetente real do datagrama. */
public record ReceivedPacket(DiscoveryMessage message, InetSocketAddress source, Instant receivedAt) {
}
