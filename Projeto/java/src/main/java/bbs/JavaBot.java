package bbs;

import bbs.Bbs.Envelope;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public final class JavaBot {
    private final String broker = env("BROKER_FRONTEND", "tcp://broker:5555");
    private final String pubsub = env("PUBSUB_XPUB", "tcp://pubsub-proxy:5558");
    private final String username = env("BOT_NAME", "java-bot");
    private final String initialChannel = env("CHANNEL_NAME", "geral");
    private final String identity = username + "-" + UUID.randomUUID();
    private final List<String> servers = Arrays.asList(env(
        "SERVER_IDS",
        "server-1,server-2,java-server-1,java-server-2,cpp-server-1,cpp-server-2"
    ).split(","));
    private final Random random = new Random();
    private long logicalClock;
    private String currentServer;

    public static void main(String[] args) throws Exception {
        new JavaBot().run();
    }

    private void run() throws Exception {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);
            subscriber.setLinger(0);
            subscriber.connect(pubsub);
            if (!login(context)) {
                throw new IllegalStateException("Nenhum servidor aceitou o login.");
            }
            Thread.sleep(250);

            List<String> channels = prepareChannels(context, subscriber);
            String[] samples = {
                "mensagem distribuída", "novo evento", "teste pubsub", "olá bbs"
            };
            while (!Thread.currentThread().isInterrupted()) {
                String channel = channels.get(random.nextInt(channels.size()));
                for (int index = 1; index <= 10; index++) {
                    receivePublications(context, subscriber, 0);
                    String content = username + ": "
                        + samples[random.nextInt(samples.length)] + " " + index;
                    Envelope response = send(context, createRequest(
                        "publish",
                        Bbs.PublishRequest.newBuilder()
                            .setChannelName(channel)
                            .setContent(content)
                            .build()
                    ));
                    System.out.printf(
                        "PUBLICAÇÃO canal=%s conteúdo=%s resposta=%s%n",
                        channel,
                        content,
                        response == null ? "timeout" : response.getPublishResponse()
                    );
                    receivePublications(context, subscriber, 1000);
                }
            }
        }
    }

    private boolean login(ZContext context) {
        for (String server : servers) {
            Envelope response = request(context, server, Envelope.newBuilder()
                .setLoginRequest(Bbs.LoginRequest.newBuilder()
                    .setUsername(username)
                    .build())
                .build());
            if (response != null && response.hasLoginResponse()
                && response.getLoginResponse().getAccepted()) {
                currentServer = server;
                System.out.printf("Login Java realizado em %s.%n", server);
                return true;
            }
        }
        return false;
    }

    private List<String> prepareChannels(ZContext context, ZMQ.Socket subscriber) {
        Envelope listed = send(context, createRequest("list", Bbs.ListChannelsRequest.getDefaultInstance()));
        List<String> channels = new ArrayList<>();
        if (listed != null && listed.hasListChannelsResponse()) {
            channels.addAll(listed.getListChannelsResponse().getChannelNamesList());
        }
        if (!channels.contains(initialChannel)) {
            createChannel(context, initialChannel);
        }
        for (int index = 1; index <= 5; index++) {
            String name = "canal-" + index;
            if (!channels.contains(name)) {
                createChannel(context, name);
            }
        }
        listed = send(context, createRequest("list", Bbs.ListChannelsRequest.getDefaultInstance()));
        channels.clear();
        if (listed != null && listed.hasListChannelsResponse()) {
            channels.addAll(listed.getListChannelsResponse().getChannelNamesList());
        }
        if (channels.isEmpty()) {
            throw new IllegalStateException("Nenhum canal está disponível.");
        }

        List<String> subscriptions = new ArrayList<>();
        while (subscriptions.size() < Math.min(3, channels.size())) {
            String channel = channels.get(random.nextInt(channels.size()));
            if (!subscriptions.contains(channel)) {
                subscriptions.add(channel);
                subscriber.subscribe(channel.getBytes(StandardCharsets.UTF_8));
            }
        }
        System.out.println("Inscrito nos canais: " + subscriptions);
        return channels;
    }

    private void createChannel(ZContext context, String name) {
        Envelope response = send(context, createRequest(
            "create",
            Bbs.CreateChannelRequest.newBuilder().setName(name).build()
        ));
        if (response != null && response.hasCreateChannelResponse()
            && !response.getCreateChannelResponse().getCreated()
            && !response.getCreateChannelResponse().getErrorCode().equals("CHANNEL_EXISTS")) {
            System.err.println("Falha ao criar canal " + name + ": "
                + response.getCreateChannelResponse().getDetail());
        }
    }

    private Envelope createRequest(String kind, Object payload) {
        Envelope.Builder request = Envelope.newBuilder()
            .setSentAtUnixMs(System.currentTimeMillis())
            .setMessageId(UUID.randomUUID().toString());
        switch (kind) {
            case "publish" -> request.setPublishRequest((Bbs.PublishRequest) payload);
            case "create" -> request.setCreateChannelRequest((Bbs.CreateChannelRequest) payload);
            case "list" -> request.setListChannelsRequest((Bbs.ListChannelsRequest) payload);
            default -> throw new IllegalArgumentException("Tipo de requisição desconhecido.");
        }
        request.setLogicalClock(++logicalClock);
        return request.build();
    }

    private Envelope send(ZContext context, Envelope request) {
        if (currentServer == null) {
            if (!login(context)) {
                return null;
            }
        }
        Envelope response = request(context, currentServer, request);
        if (response != null && !response.hasErrorResponse()) {
            return response;
        }
        String oldServer = currentServer;
        currentServer = null;
        for (String server : servers) {
            if (server.equals(oldServer)) {
                continue;
            }
            if (!loginOn(context, server)) {
                continue;
            }
            response = request(context, server, request);
            if (response != null && !response.hasErrorResponse()) {
                currentServer = server;
                return response;
            }
        }
        return null;
    }

    private boolean loginOn(ZContext context, String server) {
        Envelope response = request(context, server, Envelope.newBuilder()
            .setSentAtUnixMs(System.currentTimeMillis())
            .setMessageId(UUID.randomUUID().toString())
            .setLogicalClock(++logicalClock)
            .setLoginRequest(Bbs.LoginRequest.newBuilder()
                .setUsername(username)
                .build())
            .build());
        if (response != null && response.hasLoginResponse()
            && response.getLoginResponse().getAccepted()) {
            currentServer = server;
            return true;
        }
        return false;
    }

    private Envelope request(ZContext context, String server, Envelope request) {
        try (ZMQ.Socket socket = context.createSocket(SocketType.REQ)) {
            socket.setIdentity(identity.getBytes(StandardCharsets.UTF_8));
            socket.setReceiveTimeOut(1500);
            socket.setSendTimeOut(1500);
            socket.setLinger(0);
            socket.connect(broker);
            System.out.printf("ENVIADO para %s:%n%s%n", server, request);
            if (!socket.send(server.getBytes(StandardCharsets.UTF_8), ZMQ.SNDMORE)
                || !socket.send(request.toByteArray(), 0)) {
                return null;
            }
            byte[] data = socket.recv();
            if (data == null) {
                return null;
            }
            Envelope response = Envelope.parseFrom(data);
            logicalClock = Math.max(logicalClock, response.getLogicalClock()) + 1;
            System.out.printf("RECEBIDO de %s:%n%s%n", server, response);
            return response;
        } catch (Exception error) {
            System.err.printf("Servidor %s indisponível: %s%n", server, error.getMessage());
            return null;
        }
    }

    private void receivePublications(
        ZContext context,
        ZMQ.Socket subscriber,
        long timeoutMs
    ) {
        ZMQ.Poller poller = context.createPoller(1);
        poller.register(subscriber, ZMQ.Poller.POLLIN);
        long deadline = System.currentTimeMillis() + timeoutMs;
        do {
            long wait = Math.max(0, deadline - System.currentTimeMillis());
            if (poller.poll(wait) <= 0 || !poller.pollin(0)) {
                return;
            }
            byte[] topic = subscriber.recv();
            byte[] raw = subscriber.recv();
            if (topic == null || raw == null) {
                return;
            }
            try {
                Envelope publication = Envelope.parseFrom(raw);
                logicalClock = Math.max(logicalClock, publication.getLogicalClock()) + 1;
                System.out.printf(
                    "RECEBIDO canal=%s conteúdo=%s enviado_em=%d recebido_em=%d%n",
                    new String(topic, StandardCharsets.UTF_8),
                    publication.getPublication().getContent(),
                    publication.getSentAtUnixMs(),
                    System.currentTimeMillis()
                );
            } catch (com.google.protobuf.InvalidProtocolBufferException error) {
                System.err.println("Publicação Protobuf inválida: " + error.getMessage());
            }
        } while (System.currentTimeMillis() < deadline);
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }
}
