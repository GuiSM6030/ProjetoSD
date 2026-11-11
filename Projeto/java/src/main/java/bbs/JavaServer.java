package bbs;

import bbs.Bbs.ControlEnvelope;
import bbs.Bbs.PeerRequest;
import bbs.Bbs.PeerResponse;
import bbs.Bbs.ReferenceRequest;
import bbs.Bbs.ReplicationEvent;
import bbs.Bbs.ServerInfo;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.sqlite.SQLiteConfig;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public final class JavaServer {
    private static final Pattern USERNAME = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    private static final Pattern CHANNEL = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]{0,31}");
    private static final int REPLICATION_BATCH_SIZE = 100;

    private static final class ReplicationBatch {
        private final List<ReplicationEvent> events;
        private final long nextSequence;

        private ReplicationBatch(List<ReplicationEvent> events, long nextSequence) {
            this.events = events;
            this.nextSequence = nextSequence;
        }
    }

    private final String id = env("SERVER_ID", "java-server-1");
    private final String backend = env("BROKER_BACKEND", "tcp://broker:5556");
    private final String pubsub = env("PUBSUB_XSUB", "tcp://pubsub-proxy:5557");
    private final String pubsubSubscribers = env("PUBSUB_XPUB", "tcp://pubsub-proxy:5558");
    private final String reference = env("REFERENCE_ENDPOINT", "tcp://reference:5559");
    private final String database = env("DATABASE_PATH", "/data/java.sqlite3");
    private final CopyOnWriteArrayList<String> serverIds = new CopyOnWriteArrayList<>(splitNames(env(
        "SERVER_IDS",
        "server-1,server-2,java-server-1,java-server-2,cpp-server-1,cpp-server-2"
    )));
    private final Map<String, String> users = new ConcurrentHashMap<>();
    private final AtomicLong logicalClock = new AtomicLong();
    private final ZContext context = new ZContext();
    private volatile long physicalOffset;
    private volatile int rank;
    private volatile String coordinatorName = "";

    public static void main(String[] args) throws Exception {
        new JavaServer().run();
    }

    private void run() throws Exception {
        initializeDatabase();
        rank = referenceRequest("rank", id, 0).getReferenceResponse().getRank();
        refreshServers();
        startPeerService(5561, false);
        startPeerService(5560, true);
        startReplicationWorker();

        ZMQ.Socket dealer = context.createSocket(SocketType.DEALER);
        dealer.setIdentity(id.getBytes(StandardCharsets.UTF_8));
        dealer.connect(backend);
        ZMQ.Socket publisher = context.createSocket(SocketType.PUB);
        publisher.connect(pubsub);
        ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);
        subscriber.subscribe("servers".getBytes(StandardCharsets.UTF_8));
        subscriber.connect(pubsubSubscribers);

        dealer.send("READY", ZMQ.SNDMORE);
        dealer.send(id, 0);
        System.out.println("Servidor Java " + id + " conectado ao broker.");

        int clientsSinceHeartbeat = 0;
        int messagesSinceSync = 0;
        while (!Thread.currentThread().isInterrupted()) {
            byte[] client = dealer.recv();
            byte[] payload = dealer.recv();
            if (client == null || payload == null) {
                continue;
            }
            String clientId = new String(client, StandardCharsets.UTF_8);
            ControlEnvelope announcement = receiveAnnouncement(subscriber);
            if (announcement != null) {
                acceptAnnouncement(announcement);
            }

            EnvelopeResult result = process(client, payload, publisher);
            System.out.printf(
                "RECEBIDO do cliente %s:%n%s%n",
                clientId,
                result.request
            );
            System.out.printf(
                "ENVIADO ao cliente %s:%n%s%n",
                clientId,
                result.response
            );
            persistExchange(
                result.request,
                result.response,
                result.username,
                result.channelName
            );
            dealer.send(client, ZMQ.SNDMORE);
            dealer.send(result.response.toByteArray(), 0);
            clientsSinceHeartbeat++;
            messagesSinceSync += 2;
            if (clientsSinceHeartbeat >= 10) {
                heartbeat();
                clientsSinceHeartbeat = 0;
            }
            if (messagesSinceSync >= 15) {
                synchronizeClock();
                messagesSinceSync -= 15;
            }
            System.out.printf(
                "Cliente %s: %s -> %s%n",
                clientId,
                result.request.getPayloadCase(),
                result.response.getPayloadCase()
            );
        }
    }

    private EnvelopeResult process(byte[] client, byte[] raw, ZMQ.Socket publisher) {
        Bbs.Envelope request;
        try {
            request = Bbs.Envelope.parseFrom(raw);
        } catch (com.google.protobuf.InvalidProtocolBufferException error) {
            return invalidEnvelope(raw);
        }
        receiveClock(request.getLogicalClock());
        String clientId = new String(client, StandardCharsets.UTF_8);
        String username = users.getOrDefault(clientId, "");
        String channelName = "";
        Bbs.Envelope.Builder response = Bbs.Envelope.newBuilder()
            .setSentAtUnixMs(now())
            .setMessageId(UUID.randomUUID().toString());

        if (request.getSentAtUnixMs() <= 0) {
            response.setErrorResponse(Bbs.ErrorResponse.newBuilder()
                .setErrorCode("MISSING_TIMESTAMP")
                .setDetail("Toda mensagem deve incluir um timestamp de envio válido."));
        } else if (request.hasLoginRequest()) {
            username = request.getLoginRequest().getUsername();
            if (!USERNAME.matcher(username).matches()) {
                response.setLoginResponse(Bbs.LoginResponse.newBuilder()
                    .setErrorCode("INVALID_USERNAME")
                    .setDetail("Use de 1 a 32 letras, números, '_' ou '-'."));
            } else {
                users.put(clientId, username);
                recordLogin(
                    username,
                    request.getSentAtUnixMs(),
                    request.getMessageId()
                );
                response.setLoginResponse(Bbs.LoginResponse.newBuilder()
                    .setAccepted(true)
                    .setDetail("Login realizado."));
            }
        } else if (username.isEmpty()) {
            response.setErrorResponse(Bbs.ErrorResponse.newBuilder()
                .setErrorCode("LOGIN_REQUIRED")
                .setDetail("Faça login antes de acessar os canais."));
        } else if (request.hasListChannelsRequest()) {
            response.setListChannelsResponse(Bbs.ListChannelsResponse.newBuilder()
                .addAllChannelNames(listChannels()));
        } else if (request.hasHistoryRequest()) {
            String requestedChannel = request.getHistoryRequest().getChannelName();
            if (!requestedChannel.isEmpty() && !CHANNEL.matcher(requestedChannel).matches()) {
                response.setErrorResponse(Bbs.ErrorResponse.newBuilder()
                    .setErrorCode("INVALID_CHANNEL_NAME")
                    .setDetail("Nome de canal inválido."));
            } else if (!requestedChannel.isEmpty() && !channelExists(requestedChannel)) {
                response.setErrorResponse(Bbs.ErrorResponse.newBuilder()
                    .setErrorCode("CHANNEL_NOT_FOUND")
                    .setDetail("O canal não existe neste servidor."));
            } else {
                response.setHistoryResponse(Bbs.HistoryResponse.newBuilder()
                    .addAllPublications(publicationHistory(requestedChannel)));
            }
        } else if (request.hasCreateChannelRequest()) {
            channelName = request.getCreateChannelRequest().getName();
            if (!CHANNEL.matcher(channelName).matches()) {
                response.setCreateChannelResponse(Bbs.CreateChannelResponse.newBuilder()
                    .setErrorCode("INVALID_CHANNEL_NAME")
                    .setDetail("Nome de canal inválido."));
            } else if (createChannel(
                channelName,
                username,
                request.getSentAtUnixMs(),
                request.getMessageId()
            )) {
                response.setCreateChannelResponse(Bbs.CreateChannelResponse.newBuilder()
                    .setCreated(true)
                    .setDetail("Canal criado."));
            } else {
                response.setCreateChannelResponse(Bbs.CreateChannelResponse.newBuilder()
                    .setErrorCode("CHANNEL_EXISTS")
                    .setDetail("Esse canal já existe."));
            }
        } else if (request.hasPublishRequest()) {
            Bbs.PublishRequest publication = request.getPublishRequest();
            channelName = publication.getChannelName();
            if (!CHANNEL.matcher(channelName).matches()) {
                response.setPublishResponse(Bbs.PublishResponse.newBuilder()
                    .setErrorCode("INVALID_CHANNEL_NAME")
                    .setDetail("Nome de canal inválido."));
            } else if (publication.getContent().isBlank()
                || publication.getContent().length() > 1024) {
                response.setPublishResponse(Bbs.PublishResponse.newBuilder()
                    .setErrorCode("INVALID_CONTENT")
                    .setDetail("O conteúdo deve ter entre 1 e 1024 caracteres."));
            } else if (!channelExists(channelName)) {
                response.setPublishResponse(Bbs.PublishResponse.newBuilder()
                    .setErrorCode("CHANNEL_NOT_FOUND")
                    .setDetail("O canal não existe neste servidor."));
            } else {
                recordPublication(
                    request.getMessageId(),
                    channelName,
                    username,
                    publication.getContent(),
                    request.getSentAtUnixMs()
                );
                Bbs.Envelope event = newEnvelope();
                event = event.toBuilder()
                    .setMessageId(request.getMessageId())
                    .setPublication(Bbs.Publication.newBuilder()
                        .setChannelName(channelName)
                        .setContent(publication.getContent())
                        .setUsername(username)
                        .setSentAtUnixMs(request.getSentAtUnixMs()))
                    .build();
                publisher.send(channelName.getBytes(StandardCharsets.UTF_8), ZMQ.SNDMORE);
                publisher.send(event.toByteArray(), 0);
                System.out.printf("PUBLICADO no tópico %s:%n%s%n", channelName, event);
                response.setPublishResponse(Bbs.PublishResponse.newBuilder()
                    .setPublished(true)
                    .setDetail("Publicação aceita pelo servidor."));
            }
        } else {
            response.setErrorResponse(Bbs.ErrorResponse.newBuilder()
                .setErrorCode("UNKNOWN_REQUEST")
                .setDetail("Tipo de mensagem não reconhecido."));
        }

        response.setLogicalClock(logicalClock.incrementAndGet());
        return new EnvelopeResult(
            request,
            response.build(),
            username,
            channelName
        );
    }

    private EnvelopeResult invalidEnvelope(byte[] raw) {
        Bbs.Envelope request = Bbs.Envelope.newBuilder()
            .setSentAtUnixMs(1)
            .setMessageId(UUID.nameUUIDFromBytes(raw).toString())
            .build();
        Bbs.Envelope response = newEnvelope().toBuilder()
            .setErrorResponse(Bbs.ErrorResponse.newBuilder()
                .setErrorCode("INVALID_PROTOBUF")
                .setDetail("A mensagem não é um Envelope Protobuf válido."))
            .build();
        return new EnvelopeResult(request, response, "", "");
    }

    private Bbs.Envelope newEnvelope() {
        return Bbs.Envelope.newBuilder()
            .setSentAtUnixMs(now())
            .setMessageId(UUID.randomUUID().toString())
            .setLogicalClock(logicalClock.incrementAndGet())
            .build();
    }

    private void persistExchange(
        Bbs.Envelope request,
        Bbs.Envelope response,
        String username,
        String channelName
    ) {
        String channel = channelName.isEmpty() ? null : channelName;
        try (Connection db = connect()) {
            recordEvent(db, ReplicationEvent.newBuilder()
                .setEventId("exchange:client_to_server:"
                    + request.getPayloadCase().name().toLowerCase()
                    + ":" + request.getMessageId())
                .setEventType("exchange")
                .setOrigin(id)
                .setDirection("client_to_server")
                .setMessageType(request.getPayloadCase().name().toLowerCase())
                .setMessageId(request.getMessageId())
                .setUsername(username)
                .setChannelName(channel == null ? "" : channel)
                .setContent(request.toString())
                .setSentAtUnixMs(request.getSentAtUnixMs())
                .setReceivedAtUnixMs(now())
                .build());
            recordEvent(db, ReplicationEvent.newBuilder()
                .setEventId("exchange:server_to_client:"
                    + response.getPayloadCase().name().toLowerCase()
                    + ":" + request.getMessageId())
                .setEventType("exchange")
                .setOrigin(id)
                .setDirection("server_to_client")
                .setMessageType(response.getPayloadCase().name().toLowerCase())
                .setMessageId(request.getMessageId())
                .setUsername(username)
                .setChannelName(channel == null ? "" : channel)
                .setContent(response.toString())
                .setSentAtUnixMs(response.getSentAtUnixMs())
                .setReceivedAtUnixMs(now())
                .build());
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao persistir a troca com o cliente.", error);
        }
    }

    private void initializeDatabase() throws Exception {
        Path.of(database).getParent().toFile().mkdirs();
        try (Connection db = connect(); Statement statement = db.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, logged_at_ms INTEGER NOT NULL)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at_ms INTEGER NOT NULL, created_by_origin TEXT NOT NULL DEFAULT '')");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS publications (message_id TEXT PRIMARY KEY, channel_name TEXT NOT NULL, username TEXT NOT NULL, content TEXT NOT NULL, sent_at_ms INTEGER NOT NULL)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS exchanged_messages (event_id TEXT PRIMARY KEY, direction TEXT NOT NULL, message_type TEXT NOT NULL, message_id TEXT NOT NULL, username TEXT NOT NULL, channel_name TEXT, content TEXT, sent_at_ms INTEGER NOT NULL, received_at_ms INTEGER NOT NULL)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS replication_events (event_id TEXT PRIMARY KEY, payload BLOB NOT NULL)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS replication_outbox (event_id TEXT PRIMARY KEY)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS replication_delivered (event_id TEXT NOT NULL, target TEXT NOT NULL, PRIMARY KEY(event_id, target))");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS replication_applied (event_id TEXT PRIMARY KEY)");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS replication_sync_state (peer TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL DEFAULT 0)");
            ensureColumn(db, "channels", "created_by_origin", "TEXT NOT NULL DEFAULT ''");
            ensureColumn(db, "exchanged_messages", "event_id", "TEXT NOT NULL DEFAULT ''");
        }
    }

    private void ensureColumn(
        Connection db,
        String table,
        String column,
        String definition
    ) throws SQLException {
        boolean found = false;
        try (Statement statement = db.createStatement();
             ResultSet columns = statement.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (columns.next()) {
                if (columns.getString("name").equals(column)) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            try (Statement statement = db.createStatement()) {
                statement.executeUpdate(
                    "ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition
                );
            }
        }
    }

    private Connection connect() throws SQLException {
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(30_000);
        config.setTransactionMode(SQLiteConfig.TransactionMode.IMMEDIATE);
        Connection db = DriverManager.getConnection(
            "jdbc:sqlite:" + database, config.toProperties()
        );
        try (Statement statement = db.createStatement()) {
            statement.execute("PRAGMA busy_timeout=30000");
            return db;
        } catch (SQLException error) {
            db.close();
            throw error;
        }
    }

    private void recordLogin(String username, long timestamp, String messageId) {
        ReplicationEvent event = ReplicationEvent.newBuilder()
            .setEventId("login:" + messageId)
            .setEventType("login")
            .setOrigin(id)
            .setUsername(username)
            .setSentAtUnixMs(timestamp)
            .build();
        applyLocalEvent(event, "INSERT INTO logins(username, logged_at_ms) VALUES(?, ?)");
    }

    private boolean createChannel(
        String name,
        String username,
        long timestamp,
        String messageId
    ) {
        ReplicationEvent event = ReplicationEvent.newBuilder()
            .setEventId("channel:" + messageId)
            .setEventType("channel")
            .setOrigin(id)
            .setCreatedByOrigin(id)
            .setCreatedBy(username)
            .setChannelName(name)
            .setCreatedAtUnixMs(timestamp)
            .build();
        try (Connection db = connect()) {
            db.setAutoCommit(false);
            try (PreparedStatement insert = db.prepareStatement(
                "INSERT INTO channels(name, created_by, created_at_ms, created_by_origin) VALUES(?, ?, ?, ?)"
            )) {
                insert.setString(1, name);
                insert.setString(2, username);
                insert.setLong(3, timestamp);
                insert.setString(4, id);
                insert.executeUpdate();
                recordEvent(db, event);
                db.commit();
                return true;
            } catch (SQLException conflict) {
                db.rollback();
                if (conflict.getErrorCode() == 19) {
                    return false;
                }
                throw conflict;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao criar canal.", error);
        }
    }

    private void recordPublication(
        String messageId, String channel, String username, String content, long timestamp
    ) {
        ReplicationEvent event = ReplicationEvent.newBuilder()
            .setEventId("publication:" + messageId)
            .setEventType("publication")
            .setOrigin(id)
            .setMessageId(messageId)
            .setChannelName(channel)
            .setUsername(username)
            .setContent(content)
            .setSentAtUnixMs(timestamp)
            .build();
        try (Connection db = connect()) {
            db.setAutoCommit(false);
            try (PreparedStatement insert = db.prepareStatement(
                "INSERT INTO publications(message_id, channel_name, username, content, sent_at_ms) VALUES(?, ?, ?, ?, ?)"
            )) {
                insert.setString(1, messageId);
                insert.setString(2, channel);
                insert.setString(3, username);
                insert.setString(4, content);
                insert.setLong(5, timestamp);
                insert.executeUpdate();
                recordEvent(db, event);
                db.commit();
            } catch (SQLException error) {
                db.rollback();
                throw error;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao persistir publicação.", error);
        }
    }

    private void applyLocalEvent(ReplicationEvent event, String sql) {
        try (Connection db = connect()) {
            db.setAutoCommit(false);
            try (PreparedStatement statement = db.prepareStatement(sql)) {
                statement.setString(1, event.getUsername());
                statement.setLong(2, event.getSentAtUnixMs());
                statement.executeUpdate();
                recordEvent(db, event);
                db.commit();
            } catch (SQLException error) {
                db.rollback();
                throw error;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao persistir evento local.", error);
        }
    }

    private void recordEvent(Connection db, ReplicationEvent event) throws SQLException {
        try (PreparedStatement statement = db.prepareStatement(
            "INSERT OR IGNORE INTO replication_events(event_id, payload) VALUES(?, ?)"
        )) {
            statement.setString(1, event.getEventId());
            statement.setBytes(2, event.toByteArray());
            statement.executeUpdate();
        }
        try (PreparedStatement statement = db.prepareStatement(
            "INSERT OR IGNORE INTO replication_outbox(event_id) VALUES(?)"
        )) {
            statement.setString(1, event.getEventId());
            statement.executeUpdate();
        }
        try (PreparedStatement statement = db.prepareStatement(
            "INSERT OR IGNORE INTO replication_applied(event_id) VALUES(?)"
        )) {
            statement.setString(1, event.getEventId());
            statement.executeUpdate();
        }
    }

    private boolean applyRemoteEvent(ReplicationEvent event) {
        try (Connection db = connect()) {
            db.setAutoCommit(false);
            try (PreparedStatement check = db.prepareStatement(
                "SELECT 1 FROM replication_applied WHERE event_id=?"
            )) {
                check.setString(1, event.getEventId());
                try (ResultSet row = check.executeQuery()) {
                    if (row.next()) {
                        db.commit();
                        return true;
                    }
                }
            }
            switch (event.getEventType()) {
                case "login" -> {
                    try (PreparedStatement insert = db.prepareStatement(
                        "INSERT INTO logins(username, logged_at_ms) VALUES(?, ?)"
                    )) {
                        insert.setString(1, event.getUsername());
                        insert.setLong(2, event.getSentAtUnixMs());
                        insert.executeUpdate();
                    }
                }
                case "channel" -> applyChannel(db, event);
                case "publication" -> {
                    try (PreparedStatement insert = db.prepareStatement(
                        "INSERT OR IGNORE INTO publications(message_id, channel_name, username, content, sent_at_ms) VALUES(?, ?, ?, ?, ?)"
                    )) {
                        insert.setString(1, event.getMessageId());
                        insert.setString(2, event.getChannelName());
                        insert.setString(3, event.getUsername());
                        insert.setString(4, event.getContent());
                        insert.setLong(5, event.getSentAtUnixMs());
                        insert.executeUpdate();
                    }
                }
                case "exchange" -> {
                    try (PreparedStatement insert = db.prepareStatement(
                        "INSERT OR IGNORE INTO exchanged_messages(event_id, direction, message_type, message_id, username, channel_name, content, sent_at_ms, received_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    )) {
                        insert.setString(1, event.getEventId());
                        insert.setString(2, event.getDirection());
                        insert.setString(3, event.getMessageType());
                        insert.setString(4, event.getMessageId());
                        insert.setString(5, event.getUsername());
                        insert.setString(6, event.getChannelName());
                        insert.setString(7, event.getContent());
                        insert.setLong(8, event.getSentAtUnixMs());
                        insert.setLong(9, event.getReceivedAtUnixMs());
                        insert.executeUpdate();
                    }
                }
                default -> {
                    db.rollback();
                    return false;
                }
            }
            try (PreparedStatement insert = db.prepareStatement(
                "INSERT OR IGNORE INTO replication_events(event_id, payload) VALUES(?, ?)"
            )) {
                insert.setString(1, event.getEventId());
                insert.setBytes(2, event.toByteArray());
                insert.executeUpdate();
            }
            try (PreparedStatement insert = db.prepareStatement(
                "INSERT OR IGNORE INTO replication_applied(event_id) VALUES(?)"
            )) {
                insert.setString(1, event.getEventId());
                insert.executeUpdate();
            }
            db.commit();
            return true;
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao aplicar evento replicado.", error);
        }
    }

    private void applyChannel(Connection db, ReplicationEvent event) throws SQLException {
        try (PreparedStatement query = db.prepareStatement(
            "SELECT created_at_ms, created_by_origin, created_by FROM channels WHERE name=?"
        )) {
            query.setString(1, event.getChannelName());
            try (ResultSet row = query.executeQuery()) {
                if (row.next()) {
                    List<String> existing = List.of(
                        String.format("%020d", row.getLong(1)),
                        row.getString(2),
                        row.getString(3)
                    );
                    List<String> candidate = List.of(
                        String.format("%020d", event.getCreatedAtUnixMs()),
                        event.getCreatedByOrigin(),
                        event.getCreatedBy()
                    );
                    if (compareChannelKey(candidate, existing) >= 0) {
                        return;
                    }
                    try (PreparedStatement update = db.prepareStatement(
                        "UPDATE channels SET created_by=?, created_at_ms=?, created_by_origin=? WHERE name=?"
                    )) {
                        update.setString(1, event.getCreatedBy());
                        update.setLong(2, event.getCreatedAtUnixMs());
                        update.setString(3, event.getCreatedByOrigin());
                        update.setString(4, event.getChannelName());
                        update.executeUpdate();
                    }
                    return;
                }
            }
        }
        try (PreparedStatement insert = db.prepareStatement(
            "INSERT INTO channels(name, created_by, created_at_ms, created_by_origin) VALUES(?, ?, ?, ?)"
        )) {
            insert.setString(1, event.getChannelName());
            insert.setString(2, event.getCreatedBy());
            insert.setLong(3, event.getCreatedAtUnixMs());
            insert.setString(4, event.getCreatedByOrigin());
            insert.executeUpdate();
        }
    }

    private int compareChannelKey(List<String> first, List<String> second) {
        for (int index = 0; index < first.size(); index++) {
            int comparison = first.get(index).compareTo(second.get(index));
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private List<String> listChannels() {
        List<String> channels = new ArrayList<>();
        try (Connection db = connect(); Statement statement = db.createStatement();
             ResultSet rows = statement.executeQuery("SELECT name FROM channels ORDER BY name")) {
            while (rows.next()) {
                channels.add(rows.getString(1));
            }
            return channels;
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao listar canais.", error);
        }
    }

    private boolean channelExists(String name) {
        try (Connection db = connect(); PreparedStatement statement = db.prepareStatement(
            "SELECT 1 FROM channels WHERE name=?"
        )) {
            statement.setString(1, name);
            try (ResultSet row = statement.executeQuery()) {
                return row.next();
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao consultar canal.", error);
        }
    }

    private List<Bbs.Publication> publicationHistory(String channelName) {
        String sql = channelName.isEmpty()
            ? "SELECT channel_name, username, content, sent_at_ms FROM publications ORDER BY sent_at_ms, message_id"
            : "SELECT channel_name, username, content, sent_at_ms FROM publications WHERE channel_name=? ORDER BY sent_at_ms, message_id";
        List<Bbs.Publication> publications = new ArrayList<>();
        try (Connection db = connect(); PreparedStatement statement = db.prepareStatement(sql)) {
            if (!channelName.isEmpty()) {
                statement.setString(1, channelName);
            }
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    publications.add(Bbs.Publication.newBuilder()
                        .setChannelName(rows.getString(1))
                        .setUsername(rows.getString(2))
                        .setContent(rows.getString(3))
                        .setSentAtUnixMs(rows.getLong(4))
                        .build());
                }
            }
            return publications;
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao recuperar histórico.", error);
        }
    }

    private void startPeerService(int port, boolean replication) {
        Thread thread = new Thread(() -> servePeerRequests(port, replication));
        thread.setName(id + "-peer-" + port);
        thread.setDaemon(true);
        thread.start();
    }

    private void servePeerRequests(int port, boolean replication) {
        try (ZMQ.Socket socket = context.createSocket(SocketType.REP)) {
            socket.bind("tcp://*:" + port);
            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = socket.recv();
                if (raw == null) {
                    continue;
                }
                ControlEnvelope request;
                try {
                    request = ControlEnvelope.parseFrom(raw);
                } catch (com.google.protobuf.InvalidProtocolBufferException error) {
                    socket.send(errorResponse("Envelope Protobuf inválido").toByteArray(), 0);
                    continue;
                }
                PeerResponse response = handlePeer(
                    request.getPeerRequest(),
                    request.getLogicalClock(),
                    replication
                );
                socket.send(newControl().toBuilder()
                    .setPeerResponse(response)
                    .build()
                    .toByteArray(), 0);
            }
        }
    }

    private ControlEnvelope errorResponse(String detail) {
        return newControl().toBuilder()
            .setPeerResponse(PeerResponse.newBuilder().setOk(false).setDetail(detail))
            .build();
    }

    private PeerResponse handlePeer(
        PeerRequest request,
        long receivedClock,
        boolean replicationEndpoint
    ) {
        receiveClock(receivedClock);
        PeerResponse.Builder response = PeerResponse.newBuilder()
            .setOk(true)
            .setServerName(id)
            .setRank(rank);
        String action = request.getAction();
        if (replicationEndpoint && action.equals("replicate_pull")) {
            ReplicationBatch batch = readEvents(request.getAfterSequence());
            response.addAllEvents(batch.events);
            response.setNextSequence(batch.nextSequence);
        } else if (replicationEndpoint && action.equals("replicate_apply")) {
            for (ReplicationEvent event : request.getEventsList()) {
                if (applyRemoteEvent(event)) {
                    response.addAppliedEventIds(event.getEventId());
                }
            }
        } else if (!replicationEndpoint && action.equals("election")) {
            return response.build();
        } else if (!replicationEndpoint && action.equals("clock_report")) {
            response.setServerTimeUnixMs(now());
        } else if (!replicationEndpoint && action.equals("announce_coordinator")) {
            if (!request.getCoordinator().equals(id)) {
                response.setOk(false).setDetail("Anúncio direcionado a outro servidor.");
            } else {
                coordinatorName = id;
                announceCoordinator();
            }
        } else if (!replicationEndpoint && action.equals("clock")) {
            if (!coordinatorName.equals(id)) {
                response.setOk(false).setDetail("Este servidor não é o coordenador atual.");
            } else {
                List<Long> times = new ArrayList<>();
                times.add(request.getServerTimeUnixMs());
                times.add(now());
                for (String peer : serverIds) {
                    if (peer.equals(id) || peer.equals(request.getServerName())) {
                        continue;
                    }
                    PeerResponse report = peerRequest(peer, "clock_report");
                    if (report != null && report.getOk()) {
                        times.add(report.getServerTimeUnixMs());
                    }
                }
                response.setCorrectTimeUnixMs(average(times));
            }
        } else {
            response.setOk(false).setDetail("Ação desconhecida.");
        }
        return response.build();
    }

    private ReplicationBatch readEvents(long afterSequence) {
        List<ReplicationEvent> events = new ArrayList<>();
        long nextSequence = afterSequence;
        try (Connection db = connect(); PreparedStatement statement = db.prepareStatement(
            "SELECT rowid, payload FROM replication_events WHERE rowid > ? ORDER BY rowid LIMIT ?"
        )) {
            statement.setLong(1, afterSequence);
            statement.setInt(2, REPLICATION_BATCH_SIZE);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    nextSequence = rows.getLong(1);
                    events.add(ReplicationEvent.parseFrom(rows.getBytes(2)));
                }
            }
            return new ReplicationBatch(events, nextSequence);
        } catch (Exception error) {
            throw new IllegalStateException("Falha ao carregar eventos para replicação.", error);
        }
    }

    private void startReplicationWorker() {
        Thread thread = new Thread(this::replicationLoop);
        thread.setName(id + "-replication");
        thread.setDaemon(true);
        thread.start();
    }

    private void replicationLoop() {
        try {
            Thread.sleep(id.equals("server-1") ? 1000 : 5000);
            while (!Thread.currentThread().isInterrupted()) {
                for (String peer : serverIds) {
                    if (peer.equals(id)) {
                        continue;
                    }
                    try {
                        PeerRequest pull = PeerRequest.newBuilder()
                            .setAction("replicate_pull")
                            .setAfterSequence(replicationCursor(peer))
                            .build();
                        PeerResponse remote = sendPeerRequest(peer, pull, true);
                        if (remote != null && remote.getOk()) {
                            for (ReplicationEvent event : remote.getEventsList()) {
                                applyRemoteEvent(event);
                            }
                            setReplicationCursor(peer, remote.getNextSequence());
                        }
                        List<ReplicationEvent> pending = pendingEvents(peer);
                        if (!pending.isEmpty()) {
                            PeerResponse applied = peerRequest(peer, "replicate_apply", true, pending);
                            if (applied != null && applied.getOk()) {
                                markDelivered(applied.getAppliedEventIdsList(), peer);
                            }
                        }
                    } catch (RuntimeException error) {
                        System.err.printf(
                            "Falha na replicação de %s com %s: %s%n",
                            id, peer, error.getMessage()
                        );
                    }
                }
                Thread.sleep(5000);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private PeerResponse peerRequest(String peer, String action) {
        return peerRequest(peer, action, false, List.of());
    }

    private PeerResponse peerRequest(String peer, String action, boolean replication) {
        return peerRequest(peer, action, replication, List.of());
    }

    private PeerResponse peerRequest(
        String peer,
        String action,
        boolean replication,
        List<ReplicationEvent> events
    ) {
        PeerRequest.Builder peerRequest = PeerRequest.newBuilder().setAction(action)
            .addAllEvents(events);
        if (action.equals("clock")) {
            peerRequest.setServerName(id).setServerTimeUnixMs(now());
        }
        return sendPeerRequest(peer, peerRequest.build(), replication);
    }

    private PeerResponse sendPeerRequest(
        String peer,
        PeerRequest peerRequest,
        boolean replication
    ) {
        ControlEnvelope request = newControl().toBuilder()
            .setPeerRequest(peerRequest)
            .build();
        boolean replicationAction = peerRequest.getAction().startsWith("replicate_");
        if (replicationAction) {
            System.out.printf(
                "Replicação %s -> %s: ação=%s, eventos_enviados=%d%n",
                id, peer, peerRequest.getAction(), peerRequest.getEventsCount()
            );
        } else {
            System.out.printf("ENVIADO ao servidor %s:%n%s%n", peer, request);
        }
        try (ZMQ.Socket socket = context.createSocket(SocketType.REQ)) {
            socket.setSendTimeOut(1000);
            socket.setReceiveTimeOut(1000);
            socket.setLinger(0);
            socket.connect("tcp://" + peer + ":" + (replication ? 5560 : 5561));
            if (!socket.send(request.toByteArray(), 0)) {
                if (replicationAction) {
                    System.err.printf("Replicação %s -> %s: falha ao enviar %s.%n", id, peer, peerRequest.getAction());
                }
                return null;
            }
            byte[] raw = socket.recv();
            if (raw == null) {
                if (replicationAction) {
                    System.err.printf("Replicação %s -> %s: sem resposta para %s.%n", id, peer, peerRequest.getAction());
                }
                return null;
            }
            ControlEnvelope response = ControlEnvelope.parseFrom(raw);
            receiveClock(response.getLogicalClock());
            if (replicationAction && response.hasPeerResponse()) {
                PeerResponse peerResponse = response.getPeerResponse();
                System.out.printf(
                    "Replicação %s <- %s: ação=%s, ok=%s, eventos_recebidos=%d, aplicados=%d, cursor=%d%n",
                    id, peer, peerRequest.getAction(), peerResponse.getOk(),
                    peerResponse.getEventsCount(), peerResponse.getAppliedEventIdsCount(),
                    peerResponse.getNextSequence()
                );
            } else {
                System.out.printf("RECEBIDO do servidor %s:%n%s%n", peer, response);
            }
            return response.hasPeerResponse() ? response.getPeerResponse() : null;
        } catch (Exception error) {
            if (replicationAction) {
                System.err.printf(
                    "Falha na replicação de %s com %s durante %s: %s%n",
                    id, peer, peerRequest.getAction(), error.getMessage()
                );
            }
            return null;
        }
    }

    private long replicationCursor(String peer) {
        try (Connection db = connect(); PreparedStatement statement = db.prepareStatement(
            "SELECT last_sequence FROM replication_sync_state WHERE peer=?"
        )) {
            statement.setString(1, peer);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getLong(1) : 0;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao ler cursor de replicação.", error);
        }
    }

    private void setReplicationCursor(String peer, long sequence) {
        try (Connection db = connect(); PreparedStatement statement = db.prepareStatement(
            "INSERT INTO replication_sync_state(peer, last_sequence) VALUES(?, ?) ON CONFLICT(peer) DO UPDATE SET last_sequence=MAX(last_sequence, excluded.last_sequence)"
        )) {
            statement.setString(1, peer);
            statement.setLong(2, sequence);
            statement.executeUpdate();
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao atualizar cursor de replicação.", error);
        }
    }

    private List<ReplicationEvent> pendingEvents(String target) {
        List<ReplicationEvent> events = new ArrayList<>();
        try (Connection db = connect(); PreparedStatement statement = db.prepareStatement(
            "SELECT e.payload FROM replication_events e JOIN replication_outbox o ON o.event_id=e.event_id WHERE NOT EXISTS (SELECT 1 FROM replication_delivered d WHERE d.event_id=e.event_id AND d.target=?) ORDER BY e.rowid LIMIT ?"
        )) {
            statement.setString(1, target);
            statement.setInt(2, REPLICATION_BATCH_SIZE);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    events.add(ReplicationEvent.parseFrom(rows.getBytes(1)));
                }
            }
            return events;
        } catch (Exception error) {
            throw new IllegalStateException("Falha ao carregar outbox de replicação.", error);
        }
    }

    private void markDelivered(List<String> eventIds, String peer) {
        try (Connection db = connect(); PreparedStatement statement = db.prepareStatement(
            "INSERT OR IGNORE INTO replication_delivered(event_id, target) VALUES(?, ?)"
        )) {
            for (String eventId : eventIds) {
                statement.setString(1, eventId);
                statement.setString(2, peer);
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException error) {
            throw new IllegalStateException("Falha ao confirmar replicação.", error);
        }
    }

    private void heartbeat() {
        if (rank == 0) {
            rank = referenceRequest("rank", id, 0)
                .getReferenceResponse().getRank();
        }
        referenceRequest("heartbeat", id, rank);
        refreshServers();
    }

    private void refreshServers() {
        ControlEnvelope response = referenceRequest("list", "", 0);
        if (response.hasReferenceResponse()) {
            List<String> discovered = response.getReferenceResponse()
                .getServersList().stream()
                .map(ServerInfo::getName)
                .distinct()
                .toList();
            if (!discovered.isEmpty()) {
                serverIds.clear();
                serverIds.addAll(discovered);
            }
            if (!serverIds.contains(id)) {
                serverIds.add(id);
            }
            coordinatorName = coordinatorName.isEmpty()
                ? preferredCoordinator(response.getReferenceResponse().getServersList())
                : coordinatorName;
        }
    }

    private ControlEnvelope referenceRequest(String action, String name, int requestRank) {
        ControlEnvelope request = newControl().toBuilder()
            .setReferenceRequest(ReferenceRequest.newBuilder()
                .setAction(action)
                .setName(name)
                .setRank(requestRank))
            .build();
        System.out.printf("ENVIADO à referência:%n%s%n", request);
        try (ZMQ.Socket socket = context.createSocket(SocketType.REQ)) {
            socket.setSendTimeOut(1000);
            socket.setReceiveTimeOut(1000);
            socket.setLinger(0);
            socket.connect(reference);
            if (!socket.send(request.toByteArray(), 0)) {
                return ControlEnvelope.getDefaultInstance();
            }
            byte[] raw = socket.recv();
            if (raw == null) {
                return ControlEnvelope.getDefaultInstance();
            }
            ControlEnvelope response = ControlEnvelope.parseFrom(raw);
            receiveClock(response.getLogicalClock());
            System.out.printf("RECEBIDO da referência:%n%s%n", response);
            return response;
        } catch (Exception error) {
            System.err.println("Referência indisponível: " + error.getMessage());
            return ControlEnvelope.getDefaultInstance();
        }
    }

    private void synchronizeClock() {
        List<ServerInfo> candidates = new ArrayList<>();
        candidates.add(ServerInfo.newBuilder().setName(id).setRank(rank).build());
        for (String peer : serverIds) {
            if (peer.equals(id)) {
                continue;
            }
            PeerResponse response = peerRequest(peer, "election");
            if (response != null && response.getOk()) {
                candidates.add(ServerInfo.newBuilder()
                    .setName(response.getServerName())
                    .setRank(response.getRank())
                    .build());
            }
        }
        String winner = candidates.stream()
            .max(Comparator.comparingInt(ServerInfo::getRank)
                .thenComparing(ServerInfo::getName))
            .map(ServerInfo::getName)
            .orElse(id);
        if (winner.equals(id)) {
            coordinatorName = id;
            announceCoordinator();
            List<Long> times = new ArrayList<>();
            times.add(now());
            for (String peer : serverIds) {
                if (peer.equals(id)) {
                    continue;
                }
                PeerResponse report = peerRequest(peer, "clock_report");
                if (report != null && report.getOk()) {
                    times.add(report.getServerTimeUnixMs());
                }
            }
            adjustClock(average(times));
            return;
        }
        coordinatorName = winner;
        sendPeerRequest(
            winner,
            PeerRequest.newBuilder()
                .setAction("announce_coordinator")
                .setCoordinator(winner)
                .build(),
            false
        );
        PeerResponse clock = sendPeerRequest(
            winner,
            PeerRequest.newBuilder()
                .setAction("clock")
                .setServerName(id)
                .setServerTimeUnixMs(now())
                .build(),
            false
        );
        if (clock == null || !clock.getOk()) {
            coordinatorName = id;
            synchronizeClock();
        } else {
            adjustClock(clock.getCorrectTimeUnixMs());
        }
    }

    private String preferredCoordinator(List<ServerInfo> servers) {
        return servers.stream()
            .max(Comparator.comparingInt(ServerInfo::getRank)
                .thenComparing(ServerInfo::getName))
            .map(ServerInfo::getName)
            .orElse(id);
    }

    private void announceCoordinator() {
        ControlEnvelope announcement = newControl().toBuilder()
            .setCoordinatorAnnouncement(Bbs.CoordinatorAnnouncement.newBuilder()
                .setCoordinator(id))
            .build();
        try (ZMQ.Socket publisher = context.createSocket(SocketType.PUB)) {
            publisher.connect(pubsub);
            publisher.send("servers".getBytes(StandardCharsets.UTF_8), ZMQ.SNDMORE);
            publisher.send(announcement.toByteArray(), 0);
        }
    }

    private ControlEnvelope receiveAnnouncement(ZMQ.Socket subscriber) {
        ZMQ.Poller poller = context.createPoller(1);
        poller.register(subscriber, ZMQ.Poller.POLLIN);
        if (poller.poll(1) <= 0 || !poller.pollin(0)) {
            return null;
        }
        subscriber.recv();
        byte[] raw = subscriber.recv();
        if (raw == null) {
            return null;
        }
        try {
            return ControlEnvelope.parseFrom(raw);
        } catch (com.google.protobuf.InvalidProtocolBufferException error) {
            return null;
        }
    }

    private void acceptAnnouncement(ControlEnvelope announcement) {
        if (announcement.hasCoordinatorAnnouncement()) {
            coordinatorName = announcement.getCoordinatorAnnouncement().getCoordinator();
            receiveClock(announcement.getLogicalClock());
            System.out.println("Coordenador anunciado: " + coordinatorName);
        }
    }

    private long average(List<Long> times) {
        return times.stream().mapToLong(Long::longValue).sum() / Math.max(1, times.size());
    }

    private void adjustClock(long target) {
        long difference = target - now();
        physicalOffset += Math.max(-1000, Math.min(1000, difference));
    }

    private void receiveClock(long received) {
        logicalClock.updateAndGet(current -> Math.max(current, received) + 1);
    }

    private ControlEnvelope newControl() {
        return ControlEnvelope.newBuilder()
            .setSentAtUnixMs(now())
            .setMessageId(UUID.randomUUID().toString())
            .setLogicalClock(logicalClock.incrementAndGet())
            .build();
    }

    private long now() {
        return System.currentTimeMillis() + physicalOffset;
    }

    private static List<String> splitNames(String names) {
        List<String> result = new ArrayList<>();
        for (String name : names.split(",")) {
            if (!name.isBlank()) {
                result.add(name.trim());
            }
        }
        return result;
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }

    private record EnvelopeResult(
        Bbs.Envelope request,
        Bbs.Envelope response,
        String username,
        String channelName
    ) {}
}
