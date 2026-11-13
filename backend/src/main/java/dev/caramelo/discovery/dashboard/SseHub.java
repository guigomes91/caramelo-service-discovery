package dev.caramelo.discovery.dashboard;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Repassa os eventos do grupo ao navegador por Server-Sent Events. */
final class SseHub {

    private final Set<BlockingQueue<String>> clients = ConcurrentHashMap.newKeySet();

    void broadcast(String json) {
        for (BlockingQueue<String> queue : clients) {
            queue.offer(json); // com a fila cheia, o cliente lento perde o evento
        }
    }

    void stream(HttpExchange exchange, String firstEvent) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(200, 0);

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(1_000);
        clients.add(queue);
        try (OutputStream out = exchange.getResponseBody()) {
            write(out, "data: " + firstEvent + "\n\n");
            while (true) {
                String event = queue.poll(15, TimeUnit.SECONDS);
                // ": ping" é um comentário SSE que mantém a conexão aberta
                write(out, event == null ? ": ping\n\n" : "data: " + event + "\n\n");
            }
        } catch (IOException | InterruptedException e) {
            // conexão fechada pelo navegador ou servidor desligando
        } finally {
            clients.remove(queue);
            exchange.close();
        }
    }

    private static void write(OutputStream out, String chunk) throws IOException {
        out.write(chunk.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
