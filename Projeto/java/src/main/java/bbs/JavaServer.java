package bbs;

import bbs.Bbs.Envelope;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class JavaServer {
    private static final Map<ZMQ.Socket, String> users = new HashMap<>();

    public static void main(String[] args) throws Exception {
        String id = System.getenv().getOrDefault("SERVER_ID", "java-server-1");
        String backend = System.getenv().getOrDefault("BROKER_BACKEND", "tcp://broker:5556");
        String pubsub = System.getenv().getOrDefault("PUBSUB_XSUB", "tcp://pubsub-proxy:5557");
        String database = System.getenv().getOrDefault("DATABASE_PATH", "/data/java.sqlite3");

        Connection db = DriverManager.getConnection("jdbc:sqlite:" + database);
        db.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, logged_at_ms INTEGER)");
        db.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT, created_at_ms INTEGER)");
        db.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS publications (message_id TEXT PRIMARY KEY, channel_name TEXT, username TEXT, content TEXT, sent_at_ms INTEGER)");

        try (ZContext context = new ZContext()) {
            ZMQ.Socket dealer = context.createSocket(org.zeromq.SocketType.DEALER);
            dealer.setIdentity(id.getBytes(StandardCharsets.UTF_8));
            dealer.connect(backend);

            ZMQ.Socket pub = context.createSocket(org.zeromq.SocketType.PUB);
            pub.connect(pubsub);

            dealer.send("READY", ZMQ.SNDMORE);
            dealer.send(id, 0);
            System.out.println("Servidor Java " + id + " conectado ao broker.");

            while (!Thread.currentThread().isInterrupted()) {
                byte[][] frames = dealer.recvMultipart();
                if (frames == null || frames.length != 2) continue;
                byte[] client = frames[0];
                Envelope request = Envelope.parseFrom(frames[1]);
                Envelope response = process(client, request, db);
                dealer.send(client, ZMQ.SNDMORE);
                dealer.send(response.toByteArray(), 0);
            }
        }
    }

    private static Envelope process(byte[] client, Envelope request, Connection db) throws Exception {
        Envelope.Builder response = Envelope.newBuilder()
            .setSentAtUnixMs(System.currentTimeMillis())
            .setMessageId(UUID.randomUUID().toString())
            .setLogicalClock(request.getLogicalClock() + 1);
        String key = new String(client, StandardCharsets.UTF_8);
        if (request.hasLoginRequest()) {
            String username = request.getLoginRequest().getUsername();
            users.put(clientToSocket(client), username);
            try (PreparedStatement insert = db.prepareStatement("INSERT INTO logins(username, logged_at_ms) VALUES (?, ?)")) {
                insert.setString(1, username);
                insert.setLong(2, request.getSentAtUnixMs());
                insert.executeUpdate();
            }
            return response.setLoginResponse(Bbs.LoginResponse.newBuilder().setAccepted(true).setDetail("Login realizado.").build()).build();
        }
        if (!users.containsKey(clientToSocket(client))) {
            return response.setErrorResponse(Bbs.ErrorResponse.newBuilder().setErrorCode("LOGIN_REQUIRED").build()).build();
        }
        if (request.hasListChannelsRequest()) {
            Bbs.ListChannelsResponse.Builder list = Bbs.ListChannelsResponse.newBuilder();
            try (var rows = db.createStatement().executeQuery("SELECT name FROM channels ORDER BY name")) {
                while (rows.next()) list.addChannelNames(rows.getString(1));
            }
            return response.setListChannelsResponse(list.build()).build();
        }
        if (request.hasCreateChannelRequest()) {
            String name = request.getCreateChannelRequest().getName();
            try (PreparedStatement insert = db.prepareStatement("INSERT INTO channels(name, created_by, created_at_ms) VALUES (?, ?, ?)")) {
                insert.setString(1, name);
                insert.setString(2, users.get(clientToSocket(client)));
                insert.setLong(3, request.getSentAtUnixMs());
                insert.executeUpdate();
                return response.setCreateChannelResponse(Bbs.CreateChannelResponse.newBuilder().setCreated(true).build()).build();
            } catch (java.sql.SQLException duplicate) {
                return response.setCreateChannelResponse(Bbs.CreateChannelResponse.newBuilder().setCreated(false).setErrorCode("CHANNEL_EXISTS").build()).build();
            }
        }
        if (request.hasPublishRequest()) {
            Bbs.PublishRequest publish = request.getPublishRequest();
            return response.setPublishResponse(Bbs.PublishResponse.newBuilder().setPublished(true).build()).build();
        }
        return response.setErrorResponse(Bbs.ErrorResponse.newBuilder().setErrorCode("UNKNOWN_REQUEST").build()).build();
    }

    private static String clientToSocket(byte[] client) {
        return new String(client, StandardCharsets.UTF_8);
    }
}
