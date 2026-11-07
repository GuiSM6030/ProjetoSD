package bbs;

import bbs.Bbs.Envelope;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.*;

public final class JavaBot {
    public static void main(String[] args) throws Exception {
        String broker = System.getenv().getOrDefault("BROKER_FRONTEND", "tcp://broker:5555");
        String pubsub = System.getenv().getOrDefault("PUBSUB_XPUB", "tcp://pubsub-proxy:5558");
        String username = System.getenv().getOrDefault("BOT_NAME", "java-bot");
        List<String> servers = Arrays.asList(System.getenv().getOrDefault("SERVER_IDS", "server-1,server-2").split(","));
        try (ZContext context = new ZContext()) {
            ZMQ.Socket sub = context.createSocket(SocketType.SUB);
            sub.connect(pubsub);
            sub.subscribe("geral".getBytes(StandardCharsets.UTF_8));

            while (!Thread.currentThread().isInterrupted()) {
                boolean logged = false;
                for (String server : servers) {
                    Envelope response = request(context, broker, server,
                        Envelope.newBuilder().setSentAtUnixMs(System.currentTimeMillis()).setLogicalClock(1)
                            .setLoginRequest(Bbs.LoginRequest.newBuilder().setUsername(username).build()).build());
                    if (response != null && response.hasLoginResponse() && response.getLoginResponse().getAccepted()) {
                        System.out.println("Login Java realizado em " + server);
                        logged = true;
                        break;
                    }
                }
                if (logged) {
                    Envelope request = Envelope.newBuilder().setSentAtUnixMs(System.currentTimeMillis()).setLogicalClock(2)
                        .setPublishRequest(Bbs.PublishRequest.newBuilder().setChannelName("geral").setContent("mensagem Java").build()).build();
                    for (String server : servers) {
                        Envelope response = request(context, broker, server, request);
                        if (response != null) System.out.println("Resposta Java: " + response.getPublishResponse());
                        break;
                    }
                }
                Thread.sleep(3000);
            }
        }
    }

    private static Envelope request(ZContext context, String broker, String server, Envelope request) {
        ZMQ.Socket socket = context.createSocket(SocketType.REQ);
        socket.setReceiveTimeOut(1500);
        socket.connect(broker);
        socket.send(server.getBytes(StandardCharsets.UTF_8), ZMQ.SNDMORE);
        socket.send(request.toByteArray(), 0);
        byte[] data = socket.recv();
        socket.close();
        if (data == null) return null;
        try { return Envelope.parseFrom(data); } catch (Exception error) { return null; }
    }
}
