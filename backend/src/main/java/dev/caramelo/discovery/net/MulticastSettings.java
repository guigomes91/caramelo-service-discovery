package dev.caramelo.discovery.net;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;

/**
 * @param group            endereço Classe D (224.0.0.0 a 239.255.255.255)
 * @param port             porta UDP compartilhada por todos os membros
 * @param networkInterface interface usada no join() e no IP_MULTICAST_IF
 * @param localAddress     IPv4 dessa interface, anunciado como "host" das instâncias
 */
public record MulticastSettings(InetAddress group, int port, NetworkInterface networkInterface,
                                Inet4Address localAddress) {

    /** 239.0.0.0/8 é a faixa de escopo administrativo (RFC 2365), reservada para uso privado. */
    public static final String DEFAULT_GROUP = "239.255.42.99";
    public static final int DEFAULT_PORT = 4446;

    public MulticastSettings {
        if (!group.isMulticastAddress()) {
            throw new IllegalArgumentException(group.getHostAddress() + " não é um endereço de multicast (Classe D)");
        }
        if (!(group instanceof Inet4Address)) {
            // Os canais são abertos com StandardProtocolFamily.INET e o grupo precisa ser da mesma família.
            throw new IllegalArgumentException("o grupo precisa ser IPv4 para um canal INET");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("porta fora do intervalo: " + port);
        }
    }

    public static MulticastSettings of(String group, int port, String interfaceName) throws java.io.IOException {
        NetworkInterface nif = NetworkInterfaces.findMulticastInterface(interfaceName);
        return new MulticastSettings(InetAddress.getByName(group), port, nif, NetworkInterfaces.ipv4Of(nif));
    }

    public InetSocketAddress groupSocketAddress() {
        return new InetSocketAddress(group, port);
    }

    public String describe() {
        return "%s:%d via %s (%s)".formatted(group.getHostAddress(), port, networkInterface.getName(),
                localAddress.getHostAddress());
    }
}
