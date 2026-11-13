package dev.caramelo.discovery.net;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/** Escolhe a interface usada no join() e no IP_MULTICAST_IF. */
public final class NetworkInterfaces {

    /** Adaptadores virtuais (Hyper-V, WSL, Docker) não enxergam a LAN física, então ficam como última opção. */
    private static final List<String> VIRTUAL_HINTS =
            List.of("virtual", "vethernet", "vmware", "hyper-v", "docker", "wsl", "vbox", "loopback pseudo");

    private NetworkInterfaces() {
    }

    /**
     * @param preferredName nome da interface (ex.: "eth0"); nulo ou vazio faz a escolha automática
     */
    public static NetworkInterface findMulticastInterface(String preferredName) throws IOException {
        if (preferredName != null && !preferredName.isBlank()) {
            NetworkInterface nif = NetworkInterface.getByName(preferredName);
            if (nif == null) {
                throw new IOException("interface não encontrada: " + preferredName + ". Disponíveis: " + listUsable());
            }
            if (!isUsable(nif)) {
                throw new IOException("interface " + preferredName + " está inativa, sem IPv4 ou sem multicast");
            }
            return nif;
        }

        NetworkInterface virtualFallback = null;
        NetworkInterface loopbackFallback = null;
        for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!isUsable(nif)) {
                continue;
            }
            if (nif.isLoopback()) {
                loopbackFallback = nif;
            } else if (looksVirtual(nif)) {
                virtualFallback = virtualFallback == null ? nif : virtualFallback;
            } else {
                return nif;
            }
        }
        if (virtualFallback != null) {
            return virtualFallback;
        }
        if (loopbackFallback != null) {
            return loopbackFallback;
        }
        throw new IOException("nenhuma interface ativa com IPv4 e suporte a multicast foi encontrada");
    }

    public static Inet4Address ipv4Of(NetworkInterface nif) {
        return nif.inetAddresses()
                .filter(Inet4Address.class::isInstance)
                .map(Inet4Address.class::cast)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("interface sem IPv4: " + nif.getName()));
    }

    public static List<String> listUsable() throws SocketException {
        return Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
                .filter(NetworkInterfaces::isUsable)
                .map(nif -> nif.getName() + " (" + nif.getDisplayName() + ", " + ipv4Of(nif).getHostAddress() + ")")
                .toList();
    }

    private static boolean isUsable(NetworkInterface nif) {
        try {
            return nif.isUp() && nif.supportsMulticast()
                    && nif.inetAddresses().anyMatch(Inet4Address.class::isInstance);
        } catch (SocketException e) {
            return false;
        }
    }

    private static boolean looksVirtual(NetworkInterface nif) {
        String text = (nif.getName() + " " + nif.getDisplayName()).toLowerCase(Locale.ROOT);
        return nif.isVirtual() || VIRTUAL_HINTS.stream().anyMatch(text::contains);
    }
}
