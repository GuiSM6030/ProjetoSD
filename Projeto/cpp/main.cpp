#include <zmq.hpp>
#include <sqlite3.h>
#include <google/protobuf/text_format.h>
#include "bbs.pb.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <thread>
#include <vector>
#include <optional>

using namespace std::chrono;

class BbsNode {
public:
    BbsNode()
        : id_(env("SERVER_ID", "cpp-server-1")),
          mode_(env("APP_MODE", "server")),
          database_path_(env("DATABASE_PATH", "/data/cpp.sqlite3")),
          server_ids_(split(env(
              "SERVER_IDS",
              "server-1,server-2,java-server-1,java-server-2,cpp-server-1,cpp-server-2"
          ))),
          logical_clock_(0),
          context_(1) {}

    void run() {
        if (mode_ == "bot") {
            run_bot();
        } else {
            run_server();
        }
    }

private:
    struct Statement {
        sqlite3_stmt *value = nullptr;

        Statement(sqlite3 *db, const char *sql) {
            if (sqlite3_prepare_v2(db, sql, -1, &value, nullptr) != SQLITE_OK) {
                throw std::runtime_error(sqlite3_errmsg(db));
            }
        }

        ~Statement() {
            sqlite3_finalize(value);
        }
    };

    struct ClientResult {
        bbs::Envelope request;
        bbs::Envelope response;
        std::string username;
        std::string channel;
    };

    std::string id_;
    std::string mode_;
    std::string database_path_;
    std::string broker_frontend_ = env("BROKER_FRONTEND", "tcp://broker:5555");
    std::string broker_backend_ = env("BROKER_BACKEND", "tcp://broker:5556");
    std::string pubsub_xsub_ = env("PUBSUB_XSUB", "tcp://pubsub-proxy:5557");
    std::string pubsub_xpub_ = env("PUBSUB_XPUB", "tcp://pubsub-proxy:5558");
    std::string reference_endpoint_ = env("REFERENCE_ENDPOINT", "tcp://reference:5559");
    std::vector<std::string> server_ids_;
    mutable std::mutex server_ids_mutex_;
    std::map<std::string, std::string> users_;
    std::atomic<uint64_t> logical_clock_;
    std::atomic<int> rank_{0};
    std::atomic<long long> physical_offset_{0};
    std::string coordinator_name_;
    mutable std::mutex coordinator_mutex_;
    zmq::context_t context_;

    static long long system_now_ms() {
        return duration_cast<milliseconds>(
            system_clock::now().time_since_epoch()
        ).count();
    }

    long long now_ms() const {
        return system_now_ms() + physical_offset_.load();
    }

    static std::string env(const char *name, const char *fallback) {
        const char *value = std::getenv(name);
        return value ? value : fallback;
    }

    static std::vector<std::string> split(const std::string &value) {
        std::vector<std::string> result;
        std::stringstream stream(value);
        std::string item;
        while (std::getline(stream, item, ',')) {
            if (!item.empty()) {
                result.push_back(item);
            }
        }
        return result;
    }

    std::vector<std::string> known_servers() const {
        std::lock_guard<std::mutex> lock(server_ids_mutex_);
        return server_ids_;
    }

    std::string next_message_id() const {
        return id_ + "-" + std::to_string(system_now_ms()) + "-"
            + std::to_string(logical_clock_.load());
    }

    bbs::ControlEnvelope new_control() {
        bbs::ControlEnvelope message;
        message.set_sent_at_unix_ms(now_ms());
        message.set_message_id(next_message_id());
        message.set_logical_clock(logical_clock_.fetch_add(1) + 1);
        return message;
    }

    bbs::Envelope new_envelope() {
        bbs::Envelope message;
        message.set_sent_at_unix_ms(now_ms());
        message.set_message_id(next_message_id());
        message.set_logical_clock(logical_clock_.fetch_add(1) + 1);
        return message;
    }

    void receive_clock(uint64_t received) {
        uint64_t current = logical_clock_.load();
        while (!logical_clock_.compare_exchange_weak(
            current, std::max(current, received) + 1
        )) {}
    }

    void set_coordinator(const std::string &name) {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        coordinator_name_ = name;
    }

    std::string coordinator() const {
        std::lock_guard<std::mutex> lock(coordinator_mutex_);
        return coordinator_name_;
    }

    static std::string payload_name(const bbs::Envelope &message) {
        const auto *descriptor = message.GetDescriptor();
        const auto *oneof = descriptor->FindOneofByName("payload");
        const auto *field = message.GetReflection()->GetOneofFieldDescriptor(
            message, oneof
        );
        return field ? field->name() : "unknown";
    }

    static void close_database(sqlite3 *database) {
        sqlite3_close_v2(database);
    }

    sqlite3 *open_database() const {
        sqlite3 *database = nullptr;
        if (sqlite3_open(database_path_.c_str(), &database) != SQLITE_OK) {
            std::string error = database ? sqlite3_errmsg(database) : "open failed";
            if (database) sqlite3_close(database);
            throw std::runtime_error(error);
        }
        if (sqlite3_busy_timeout(database, 30000) != SQLITE_OK) {
            std::string error = sqlite3_errmsg(database);
            sqlite3_close(database);
            throw std::runtime_error(error);
        }
        return database;
    }

    void execute(sqlite3 *database, const char *sql) const {
        char *error = nullptr;
        if (sqlite3_exec(database, sql, nullptr, nullptr, &error) != SQLITE_OK) {
            std::string message = error ? error : sqlite3_errmsg(database);
            sqlite3_free(error);
            throw std::runtime_error(message);
        }
    }

    void initialize_database() {
        std::filesystem::path path(database_path_);
        if (path.has_parent_path()) {
            std::filesystem::create_directories(path.parent_path());
        }
        sqlite3 *database = open_database();
        try {
            execute(database, "PRAGMA journal_mode=WAL");
            execute(database, "CREATE TABLE IF NOT EXISTS logins(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, logged_at_ms INTEGER NOT NULL)");
            execute(database, "CREATE TABLE IF NOT EXISTS channels(name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at_ms INTEGER NOT NULL, created_by_origin TEXT NOT NULL DEFAULT '')");
            execute(database, "CREATE TABLE IF NOT EXISTS publications(message_id TEXT PRIMARY KEY, channel_name TEXT NOT NULL, username TEXT NOT NULL, content TEXT NOT NULL, sent_at_ms INTEGER NOT NULL)");
            execute(database, "CREATE TABLE IF NOT EXISTS exchanged_messages(event_id TEXT PRIMARY KEY, direction TEXT NOT NULL, message_type TEXT NOT NULL, message_id TEXT NOT NULL, username TEXT NOT NULL, channel_name TEXT, content TEXT, sent_at_ms INTEGER NOT NULL, received_at_ms INTEGER NOT NULL)");
            execute(database, "CREATE TABLE IF NOT EXISTS replication_events(event_id TEXT PRIMARY KEY, payload BLOB NOT NULL)");
            execute(database, "CREATE TABLE IF NOT EXISTS replication_outbox(event_id TEXT PRIMARY KEY)");
            execute(database, "CREATE TABLE IF NOT EXISTS replication_delivered(event_id TEXT NOT NULL, target TEXT NOT NULL, PRIMARY KEY(event_id, target))");
            execute(database, "CREATE TABLE IF NOT EXISTS replication_applied(event_id TEXT PRIMARY KEY)");
            bool has_origin = false;
            {
                Statement columns(database, "PRAGMA table_info(channels)");
                while (sqlite3_step(columns.value) == SQLITE_ROW) {
                    const auto *name = reinterpret_cast<const char *>(
                        sqlite3_column_text(columns.value, 1)
                    );
                    if (name && std::string(name) == "created_by_origin") {
                        has_origin = true;
                        break;
                    }
                }
            }
            if (!has_origin) {
                execute(database, "ALTER TABLE channels ADD COLUMN created_by_origin TEXT NOT NULL DEFAULT ''");
            }
        } catch (...) {
            close_database(database);
            throw;
        }
        close_database(database);
    }

    void insert_event(sqlite3 *database, const bbs::ReplicationEvent &event, bool local) {
        std::string bytes;
        if (!event.SerializeToString(&bytes)) {
            throw std::runtime_error("Could not serialize replication event");
        }
        Statement stored(database, "INSERT OR IGNORE INTO replication_events(event_id, payload) VALUES(?, ?)");
        sqlite3_bind_text(stored.value, 1, event.event_id().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stored.value, 2, bytes.data(), static_cast<int>(bytes.size()), SQLITE_TRANSIENT);
        if (sqlite3_step(stored.value) != SQLITE_DONE) {
            throw std::runtime_error(sqlite3_errmsg(database));
        }
        Statement applied(database, "INSERT OR IGNORE INTO replication_applied(event_id) VALUES(?)");
        sqlite3_bind_text(applied.value, 1, event.event_id().c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(applied.value) != SQLITE_DONE) {
            throw std::runtime_error(sqlite3_errmsg(database));
        }
        if (local) {
            Statement outbox(database, "INSERT OR IGNORE INTO replication_outbox(event_id) VALUES(?)");
            sqlite3_bind_text(outbox.value, 1, event.event_id().c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(outbox.value) != SQLITE_DONE) {
                throw std::runtime_error(sqlite3_errmsg(database));
            }
        }
    }

    void record_login(const std::string &username, long long timestamp, const std::string &message_id) {
        sqlite3 *database = open_database();
        execute(database, "BEGIN IMMEDIATE");
        try {
            Statement insert(database, "INSERT INTO logins(username, logged_at_ms) VALUES(?, ?)");
            sqlite3_bind_text(insert.value, 1, username.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(insert.value, 2, timestamp);
            if (sqlite3_step(insert.value) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
            bbs::ReplicationEvent event;
            event.set_event_id("login:" + message_id);
            event.set_event_type("login");
            event.set_origin(id_);
            event.set_username(username);
            event.set_sent_at_unix_ms(timestamp);
            insert_event(database, event, true);
            execute(database, "COMMIT");
        } catch (...) {
            sqlite3_exec(database, "ROLLBACK", nullptr, nullptr, nullptr);
            close_database(database);
            throw;
        }
        close_database(database);
    }

    bool create_channel(
        const std::string &name,
        const std::string &username,
        long long timestamp,
        const std::string &message_id
    ) {
        sqlite3 *database = open_database();
        execute(database, "BEGIN IMMEDIATE");
        try {
            Statement insert(database, "INSERT INTO channels(name, created_by, created_at_ms, created_by_origin) VALUES(?, ?, ?, ?)");
            sqlite3_bind_text(insert.value, 1, name.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert.value, 2, username.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(insert.value, 3, timestamp);
            sqlite3_bind_text(insert.value, 4, id_.c_str(), -1, SQLITE_TRANSIENT);
            int result = sqlite3_step(insert.value);
            if (result == SQLITE_CONSTRAINT) {
                execute(database, "ROLLBACK");
                close_database(database);
                return false;
            }
            if (result != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
            bbs::ReplicationEvent event;
            event.set_event_id("channel:" + message_id);
            event.set_event_type("channel");
            event.set_origin(id_);
            event.set_created_by_origin(id_);
            event.set_created_by(username);
            event.set_channel_name(name);
            event.set_created_at_unix_ms(timestamp);
            insert_event(database, event, true);
            execute(database, "COMMIT");
        } catch (...) {
            sqlite3_exec(database, "ROLLBACK", nullptr, nullptr, nullptr);
            close_database(database);
            throw;
        }
        close_database(database);
        return true;
    }

    bool channel_exists(const std::string &name) const {
        sqlite3 *database = open_database();
        Statement query(database, "SELECT 1 FROM channels WHERE name=?");
        sqlite3_bind_text(query.value, 1, name.c_str(), -1, SQLITE_TRANSIENT);
        bool found = sqlite3_step(query.value) == SQLITE_ROW;
        close_database(database);
        return found;
    }

    std::vector<std::string> list_channels() const {
        sqlite3 *database = open_database();
        Statement query(database, "SELECT name FROM channels ORDER BY name");
        std::vector<std::string> channels;
        while (sqlite3_step(query.value) == SQLITE_ROW) {
            const auto *name = sqlite3_column_text(query.value, 0);
            channels.emplace_back(reinterpret_cast<const char *>(name));
        }
        close_database(database);
        return channels;
    }

    std::vector<bbs::Publication> publication_history(
        const std::string &channel
    ) const {
        sqlite3 *database = open_database();
        const char *sql = channel.empty()
            ? "SELECT channel_name, username, content, sent_at_ms FROM publications ORDER BY sent_at_ms, message_id"
            : "SELECT channel_name, username, content, sent_at_ms FROM publications WHERE channel_name=? ORDER BY sent_at_ms, message_id";
        Statement query(database, sql);
        if (!channel.empty()) {
            sqlite3_bind_text(query.value, 1, channel.c_str(), -1, SQLITE_TRANSIENT);
        }
        std::vector<bbs::Publication> publications;
        while (sqlite3_step(query.value) == SQLITE_ROW) {
            bbs::Publication publication;
            publication.set_channel_name(reinterpret_cast<const char *>(
                sqlite3_column_text(query.value, 0)
            ));
            publication.set_username(reinterpret_cast<const char *>(
                sqlite3_column_text(query.value, 1)
            ));
            publication.set_content(reinterpret_cast<const char *>(
                sqlite3_column_text(query.value, 2)
            ));
            publication.set_sent_at_unix_ms(sqlite3_column_int64(query.value, 3));
            publications.push_back(std::move(publication));
        }
        close_database(database);
        return publications;
    }

    void record_publication(
        const std::string &message_id,
        const std::string &channel,
        const std::string &username,
        const std::string &content,
        long long timestamp
    ) {
        sqlite3 *database = open_database();
        execute(database, "BEGIN IMMEDIATE");
        try {
            Statement insert(database, "INSERT OR IGNORE INTO publications(message_id, channel_name, username, content, sent_at_ms) VALUES(?, ?, ?, ?, ?)");
            sqlite3_bind_text(insert.value, 1, message_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert.value, 2, channel.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert.value, 3, username.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert.value, 4, content.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(insert.value, 5, timestamp);
            if (sqlite3_step(insert.value) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
            bbs::ReplicationEvent event;
            event.set_event_id("publication:" + message_id);
            event.set_event_type("publication");
            event.set_origin(id_);
            event.set_message_id(message_id);
            event.set_channel_name(channel);
            event.set_username(username);
            event.set_content(content);
            event.set_sent_at_unix_ms(timestamp);
            insert_event(database, event, true);
            execute(database, "COMMIT");
        } catch (...) {
            sqlite3_exec(database, "ROLLBACK", nullptr, nullptr, nullptr);
            close_database(database);
            throw;
        }
        close_database(database);
    }

    void persist_exchange(
        const bbs::Envelope &request,
        const bbs::Envelope &response,
        const std::string &username,
        const std::string &channel
    ) {
        const std::string request_type = payload_name(request);
        const std::string response_type = payload_name(response);
        auto save = [&](const std::string &direction, const std::string &type,
                        const std::string &body, long long timestamp) {
            sqlite3 *database = open_database();
            execute(database, "BEGIN IMMEDIATE");
            try {
                bbs::ReplicationEvent event;
                event.set_event_id(
                    "exchange:" + direction + ":" + type + ":" + request.message_id()
                );
                event.set_event_type("exchange");
                event.set_origin(id_);
                event.set_direction(direction);
                event.set_message_type(type);
                event.set_message_id(request.message_id());
                event.set_username(username);
                event.set_channel_name(channel);
                event.set_content(body);
                event.set_sent_at_unix_ms(timestamp);
                event.set_received_at_unix_ms(now_ms());
                Statement insert(database, "INSERT OR IGNORE INTO exchanged_messages(event_id, direction, message_type, message_id, username, channel_name, content, sent_at_ms, received_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
                sqlite3_bind_text(insert.value, 1, event.event_id().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 2, direction.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 3, type.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 4, request.message_id().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 5, username.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 6, channel.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 7, body.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(insert.value, 8, timestamp);
                sqlite3_bind_int64(insert.value, 9, event.received_at_unix_ms());
                if (sqlite3_step(insert.value) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
                insert_event(database, event, true);
                execute(database, "COMMIT");
            } catch (...) {
                sqlite3_exec(database, "ROLLBACK", nullptr, nullptr, nullptr);
                close_database(database);
                throw;
            }
            close_database(database);
        };
        save("client_to_server", request_type, request.DebugString(), request.sent_at_unix_ms());
        save("server_to_client", response_type, response.DebugString(), response.sent_at_unix_ms());
    }

    bbs::Envelope process_client_message(
        const std::string &client,
        const std::string &raw,
        zmq::socket_t &publisher,
        ClientResult &result
    ) {
        bbs::Envelope request;
        if (!request.ParseFromString(raw)) {
            bbs::Envelope response = new_envelope();
            response.mutable_error_response()->set_error_code("INVALID_PROTOBUF");
            response.mutable_error_response()->set_detail("A mensagem não é um Envelope Protobuf válido.");
            result = {request, response, "", ""};
            return response;
        }
        receive_clock(request.logical_clock());
        std::string username = users_.count(client) ? users_[client] : "";
        std::string channel;
        bbs::Envelope response = new_envelope();
        if (request.sent_at_unix_ms() <= 0) {
            response.mutable_error_response()->set_error_code("MISSING_TIMESTAMP");
            response.mutable_error_response()->set_detail("Toda mensagem deve incluir um timestamp de envio válido.");
        } else if (request.has_login_request()) {
            username = request.login_request().username();
            if (username.empty() || username.size() > 32
                || username.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != std::string::npos) {
                response.mutable_login_response()->set_error_code("INVALID_USERNAME");
                response.mutable_login_response()->set_detail("Nome de usuário inválido.");
            } else {
                users_[client] = username;
                record_login(username, request.sent_at_unix_ms(), request.message_id());
                response.mutable_login_response()->set_accepted(true);
                response.mutable_login_response()->set_detail("Login realizado.");
            }
        } else if (username.empty()) {
            response.mutable_error_response()->set_error_code("LOGIN_REQUIRED");
            response.mutable_error_response()->set_detail("Faça login antes de acessar os canais.");
        } else if (request.has_list_channels_request()) {
            for (const auto &name : list_channels()) {
                response.mutable_list_channels_response()->add_channel_names(name);
            }
        } else if (request.has_create_channel_request()) {
            channel = request.create_channel_request().name();
            if (!valid_channel(channel)) {
                response.mutable_create_channel_response()->set_error_code("INVALID_CHANNEL_NAME");
                response.mutable_create_channel_response()->set_detail("Nome de canal inválido.");
            } else if (create_channel(channel, username, request.sent_at_unix_ms(), request.message_id())) {
                response.mutable_create_channel_response()->set_created(true);
                response.mutable_create_channel_response()->set_detail("Canal criado.");
            } else {
                response.mutable_create_channel_response()->set_error_code("CHANNEL_EXISTS");
                response.mutable_create_channel_response()->set_detail("Esse canal já existe.");
            }
        } else if (request.has_publish_request()) {
            const auto &publication = request.publish_request();
            channel = publication.channel_name();
            if (!valid_channel(channel)) {
                response.mutable_publish_response()->set_error_code("INVALID_CHANNEL_NAME");
                response.mutable_publish_response()->set_detail("Nome de canal inválido.");
            } else if (publication.content().empty() || publication.content().size() > 1024
                       || publication.content().find_first_not_of(" \t\r\n") == std::string::npos) {
                response.mutable_publish_response()->set_error_code("INVALID_CONTENT");
                response.mutable_publish_response()->set_detail("O conteúdo deve ter entre 1 e 1024 caracteres.");
            } else if (!channel_exists(channel)) {
                response.mutable_publish_response()->set_error_code("CHANNEL_NOT_FOUND");
                response.mutable_publish_response()->set_detail("O canal não existe neste servidor.");
            } else {
                record_publication(
                    request.message_id(), channel, username,
                    publication.content(), request.sent_at_unix_ms()
                );
                bbs::Envelope event = new_envelope();
                event.set_message_id(request.message_id());
                event.mutable_publication()->set_channel_name(channel);
                event.mutable_publication()->set_content(publication.content());
                event.mutable_publication()->set_username(username);
                event.mutable_publication()->set_sent_at_unix_ms(request.sent_at_unix_ms());
                std::cout << "PUBLICADO no tópico " << channel << ":\n"
                    << event.DebugString();
                std::string bytes;
                event.SerializeToString(&bytes);
                publisher.send(zmq::buffer(channel), zmq::send_flags::sndmore);
                publisher.send(zmq::buffer(bytes), zmq::send_flags::none);
                response.mutable_publish_response()->set_published(true);
                response.mutable_publish_response()->set_detail("Publicação aceita pelo servidor.");
            }
        } else if (request.has_history_request()) {
            channel = request.history_request().channel_name();
            if (!channel.empty() && !valid_channel(channel)) {
                response.mutable_error_response()->set_error_code("INVALID_CHANNEL_NAME");
                response.mutable_error_response()->set_detail("Nome de canal inválido.");
            } else if (!channel.empty() && !channel_exists(channel)) {
                response.mutable_error_response()->set_error_code("CHANNEL_NOT_FOUND");
                response.mutable_error_response()->set_detail("O canal não existe neste servidor.");
            } else {
                for (const auto &publication : publication_history(channel)) {
                    response.mutable_history_response()->add_publications()->CopyFrom(publication);
                }
            }
        } else {
            response.mutable_error_response()->set_error_code("UNKNOWN_REQUEST");
            response.mutable_error_response()->set_detail("Tipo de mensagem não reconhecido.");
        }
        response.set_logical_clock(logical_clock_.fetch_add(1) + 1);
        result = {request, response, username, channel};
        return response;
    }

    bool valid_channel(const std::string &name) const {
        if (name.empty() || name.size() > 32
            || !std::isalnum(static_cast<unsigned char>(name.front()))) {
            return false;
        }
        return name.find_first_not_of(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        ) == std::string::npos;
    }

    void run_server() {
        initialize_database();
        auto reference = reference_request("rank", id_, 0);
        if (reference.has_reference_response()) {
            rank_.store(static_cast<int>(reference.reference_response().rank()));
        }
        refresh_servers();
        start_peer_service(5561, false);
        start_peer_service(5560, true);
        std::thread([this] { replication_loop(); }).detach();

        zmq::socket_t dealer(context_, zmq::socket_type::dealer);
        dealer.set(zmq::sockopt::routing_id, id_);
        dealer.connect(broker_backend_);
        zmq::socket_t publisher(context_, zmq::socket_type::pub);
        publisher.connect(pubsub_xsub_);
        zmq::socket_t subscriber(context_, zmq::socket_type::sub);
        subscriber.set(zmq::sockopt::subscribe, "servers");
        subscriber.connect(pubsub_xpub_);

        dealer.send(zmq::buffer("READY"), zmq::send_flags::sndmore);
        dealer.send(zmq::buffer(id_), zmq::send_flags::none);
        std::cout << "Servidor C++ " << id_ << " conectado ao broker.\n";
        int clients_since_heartbeat = 0;
        int messages_since_sync = 0;
        while (true) {
            zmq::pollitem_t items[] = {
                {dealer.handle(), 0, ZMQ_POLLIN, 0},
                {subscriber.handle(), 0, ZMQ_POLLIN, 0},
            };
            zmq::poll(items, 2, milliseconds(500));
            if (items[1].revents & ZMQ_POLLIN) {
                zmq::message_t topic;
                zmq::message_t payload;
                if (!subscriber.recv(topic) || !subscriber.recv(payload)) {
                    continue;
                }
                if (std::string(static_cast<char *>(topic.data()), topic.size()) == "servers") {
                    bbs::ControlEnvelope message;
                    if (message.ParseFromArray(payload.data(), static_cast<int>(payload.size()))
                        && message.has_coordinator_announcement()) {
                        set_coordinator(message.coordinator_announcement().coordinator());
                        receive_clock(message.logical_clock());
                    }
                }
            }
            if (!(items[0].revents & ZMQ_POLLIN)) {
                continue;
            }
            zmq::message_t client_frame;
            zmq::message_t payload_frame;
            if (!dealer.recv(client_frame) || !dealer.recv(payload_frame)) {
                continue;
            }
            std::string client(static_cast<char *>(client_frame.data()), client_frame.size());
            std::string raw(static_cast<char *>(payload_frame.data()), payload_frame.size());
            ClientResult result;
            process_client_message(client, raw, publisher, result);
            std::cout << "RECEBIDO do cliente " << client << ":\n"
                << result.request.DebugString();
            std::cout << "ENVIADO ao cliente " << client << ":\n"
                << result.response.DebugString();
            persist_exchange(result.request, result.response, result.username, result.channel);
            dealer.send(client_frame, zmq::send_flags::sndmore);
            dealer.send(zmq::buffer(result.response.SerializeAsString()), zmq::send_flags::none);
            clients_since_heartbeat++;
            messages_since_sync += 2;
            if (clients_since_heartbeat >= 10) {
                heartbeat();
                clients_since_heartbeat = 0;
            }
            if (messages_since_sync >= 15) {
                synchronize_clock();
                messages_since_sync -= 15;
            }
        }
    }

    bbs::ControlEnvelope reference_request(
        const std::string &action,
        const std::string &name,
        uint32_t rank
    ) {
        bbs::ControlEnvelope request = new_control();
        request.mutable_reference_request()->set_action(action);
        request.mutable_reference_request()->set_name(name);
        request.mutable_reference_request()->set_rank(rank);
        std::cout << "ENVIADO à referência:\n" << request.DebugString();
        zmq::socket_t socket(context_, zmq::socket_type::req);
        socket.set(zmq::sockopt::sndtimeo, 1000);
        socket.set(zmq::sockopt::rcvtimeo, 1000);
        socket.set(zmq::sockopt::linger, 0);
        socket.connect(reference_endpoint_);
        std::string bytes = request.SerializeAsString();
        if (!socket.send(zmq::buffer(bytes), zmq::send_flags::none)) {
            return {};
        }
        zmq::message_t frame;
        if (!socket.recv(frame)) {
            return {};
        }
        bbs::ControlEnvelope response;
        if (!response.ParseFromArray(frame.data(), static_cast<int>(frame.size()))) {
            return {};
        }
        receive_clock(response.logical_clock());
        std::cout << "RECEBIDO da referência:\n" << response.DebugString();
        return response;
    }

    void refresh_servers() {
        auto response = reference_request("list", "", 0);
        if (!response.has_reference_response()
            || !response.reference_response().ok()
            || response.reference_response().servers().empty()) {
            return;
        }
        std::vector<std::string> discovered;
        for (const auto &server : response.reference_response().servers()) {
            if (std::find(discovered.begin(), discovered.end(), server.name())
                == discovered.end()) {
                discovered.push_back(server.name());
            }
        }
        if (mode_ != "bot"
            && std::find(discovered.begin(), discovered.end(), id_) == discovered.end()) {
            discovered.push_back(id_);
        }
        std::lock_guard<std::mutex> lock(server_ids_mutex_);
        server_ids_ = std::move(discovered);
    }

    void heartbeat() {
        if (!rank_.load()) {
            auto response = reference_request("rank", id_, 0);
            if (response.has_reference_response()) {
                rank_.store(static_cast<int>(response.reference_response().rank()));
            }
        }
        reference_request("heartbeat", id_, static_cast<uint32_t>(rank_.load()));
        refresh_servers();
    }

    bbs::PeerResponse peer_request(
        const std::string &peer,
        const bbs::PeerRequest &peer_message,
        bool replication
    ) {
        bbs::ControlEnvelope request = new_control();
        request.mutable_peer_request()->CopyFrom(peer_message);
        zmq::socket_t socket(context_, zmq::socket_type::req);
        socket.set(zmq::sockopt::sndtimeo, 1000);
        socket.set(zmq::sockopt::rcvtimeo, 1000);
        socket.set(zmq::sockopt::linger, 0);
        socket.connect(
            "tcp://" + peer + (replication ? ":5560" : ":5561")
        );
        std::string bytes = request.SerializeAsString();
        if (!socket.send(zmq::buffer(bytes), zmq::send_flags::none)) {
            return {};
        }
        zmq::message_t frame;
        if (!socket.recv(frame)) {
            return {};
        }
        bbs::ControlEnvelope response;
        if (!response.ParseFromArray(frame.data(), static_cast<int>(frame.size()))
            || !response.has_peer_response()) {
            return {};
        }
        receive_clock(response.logical_clock());
        return response.peer_response();
    }

    void start_peer_service(int port, bool replication) {
        std::thread([this, port, replication] { serve_peer(port, replication); }).detach();
    }

    void serve_peer(int port, bool replication) {
        zmq::socket_t socket(context_, zmq::socket_type::rep);
        socket.set(zmq::sockopt::linger, 0);
        socket.bind("tcp://*:" + std::to_string(port));
        while (true) {
            zmq::message_t frame;
            if (!socket.recv(frame)) continue;
            bbs::ControlEnvelope request;
            bbs::PeerResponse response;
            try {
                if (request.ParseFromArray(frame.data(), static_cast<int>(frame.size()))
                    && request.has_peer_request()) {
                    response = handle_peer(request, replication);
                } else {
                    response.set_ok(false);
                    response.set_detail("Envelope Protobuf inválido.");
                }
            } catch (const std::exception &error) {
                std::cerr << "Falha ao atender pedido do par em " << id_
                          << ":" << port << ": " << error.what() << "\n";
                response.set_ok(false);
                response.set_detail(error.what());
            }
            bbs::ControlEnvelope reply = new_control();
            reply.mutable_peer_response()->CopyFrom(response);
            socket.send(zmq::buffer(reply.SerializeAsString()), zmq::send_flags::none);
        }
    }

    bbs::PeerResponse handle_peer(
        const bbs::ControlEnvelope &envelope,
        bool replication
    ) {
        receive_clock(envelope.logical_clock());
        const auto &request = envelope.peer_request();
        bbs::PeerResponse response;
        response.set_ok(true);
        response.set_server_name(id_);
        response.set_rank(static_cast<uint32_t>(rank_.load()));
        const std::string action = request.action();
        if (replication && action == "replicate_pull") {
            for (const auto &event : read_events()) {
                response.add_events()->CopyFrom(event);
            }
        } else if (replication && action == "replicate_apply") {
            for (const auto &event : request.events()) {
                if (apply_remote_event(event)) {
                    response.add_applied_event_ids(event.event_id());
                }
            }
        } else if (!replication && action == "election") {
            return response;
        } else if (!replication && action == "clock_report") {
            response.set_server_time_unix_ms(now_ms());
        } else if (!replication && action == "announce_coordinator") {
            if (request.coordinator() != id_) {
                response.set_ok(false);
                response.set_detail("Anúncio direcionado a outro servidor.");
            } else {
                set_coordinator(id_);
                announce_coordinator();
            }
        } else if (!replication && action == "clock") {
            if (coordinator() != id_) {
                response.set_ok(false);
                response.set_detail("Este servidor não é o coordenador atual.");
            } else {
                std::vector<long long> times{
                    request.server_time_unix_ms(),
                    now_ms()
                };
                for (const auto &peer : known_servers()) {
                    if (peer == id_ || peer == request.server_name()) continue;
                    bbs::PeerRequest report;
                    report.set_action("clock_report");
                    auto remote = peer_request(peer, report, false);
                    if (remote.ok()) times.push_back(remote.server_time_unix_ms());
                }
                response.set_correct_time_unix_ms(average(times));
            }
        } else {
            response.set_ok(false);
            response.set_detail("Ação desconhecida.");
        }
        return response;
    }

    std::vector<bbs::ReplicationEvent> read_events() const {
        sqlite3 *database = open_database();
        Statement query(database, "SELECT payload FROM replication_events");
        std::vector<bbs::ReplicationEvent> events;
        while (sqlite3_step(query.value) == SQLITE_ROW) {
            const void *data = sqlite3_column_blob(query.value, 0);
            int size = sqlite3_column_bytes(query.value, 0);
            bbs::ReplicationEvent event;
            if (event.ParseFromArray(data, size)) {
                events.push_back(event);
            }
        }
        close_database(database);
        return events;
    }

    bool apply_remote_event(const bbs::ReplicationEvent &event) {
        sqlite3 *database = open_database();
        execute(database, "BEGIN IMMEDIATE");
        try {
            Statement exists(database, "SELECT 1 FROM replication_applied WHERE event_id=?");
            sqlite3_bind_text(exists.value, 1, event.event_id().c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(exists.value) == SQLITE_ROW) {
                execute(database, "COMMIT");
                close_database(database);
                return true;
            }
            if (event.event_type() == "login") {
                Statement insert(database, "INSERT INTO logins(username, logged_at_ms) VALUES(?, ?)");
                sqlite3_bind_text(insert.value, 1, event.username().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(insert.value, 2, event.sent_at_unix_ms());
                if (sqlite3_step(insert.value) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
            } else if (event.event_type() == "channel") {
                apply_remote_channel(database, event);
            } else if (event.event_type() == "publication") {
                Statement insert(database, "INSERT OR IGNORE INTO publications(message_id, channel_name, username, content, sent_at_ms) VALUES(?, ?, ?, ?, ?)");
                sqlite3_bind_text(insert.value, 1, event.message_id().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 2, event.channel_name().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 3, event.username().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 4, event.content().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(insert.value, 5, event.sent_at_unix_ms());
                if (sqlite3_step(insert.value) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
            } else if (event.event_type() == "exchange") {
                Statement insert(database, "INSERT OR IGNORE INTO exchanged_messages(event_id, direction, message_type, message_id, username, channel_name, content, sent_at_ms, received_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
                sqlite3_bind_text(insert.value, 1, event.event_id().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 2, event.direction().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 3, event.message_type().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 4, event.message_id().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 5, event.username().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 6, event.channel_name().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert.value, 7, event.content().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(insert.value, 8, event.sent_at_unix_ms());
                sqlite3_bind_int64(insert.value, 9, event.received_at_unix_ms());
                if (sqlite3_step(insert.value) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
            } else {
                execute(database, "ROLLBACK");
                close_database(database);
                return false;
            }
            insert_event(database, event, false);
            execute(database, "COMMIT");
        } catch (...) {
            sqlite3_exec(database, "ROLLBACK", nullptr, nullptr, nullptr);
            close_database(database);
            throw;
        }
        close_database(database);
        return true;
    }

    void apply_remote_channel(sqlite3 *database, const bbs::ReplicationEvent &event) {
        bool exists = false;
        std::tuple<long long, std::string, std::string> current;
        {
            Statement query(database, "SELECT created_at_ms, created_by_origin, created_by FROM channels WHERE name=?");
            sqlite3_bind_text(query.value, 1, event.channel_name().c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(query.value) == SQLITE_ROW) {
                exists = true;
                const auto timestamp = sqlite3_column_int64(query.value, 0);
                const auto *origin = reinterpret_cast<const char *>(sqlite3_column_text(query.value, 1));
                const auto *creator = reinterpret_cast<const char *>(sqlite3_column_text(query.value, 2));
                current = std::make_tuple(
                    timestamp,
                    origin ? origin : "",
                    creator ? creator : ""
                );
            }
        }
        if (exists) {
            auto candidate = std::make_tuple(
                event.created_at_unix_ms(),
                event.created_by_origin(),
                event.created_by()
            );
            if (candidate < current) {
                Statement update(database, "UPDATE channels SET created_by=?, created_at_ms=?, created_by_origin=? WHERE name=?");
                sqlite3_bind_text(update.value, 1, event.created_by().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(update.value, 2, event.created_at_unix_ms());
                sqlite3_bind_text(update.value, 3, event.created_by_origin().c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(update.value, 4, event.channel_name().c_str(), -1, SQLITE_TRANSIENT);
                if (sqlite3_step(update.value) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
            }
            return;
        }
        Statement insert(database, "INSERT INTO channels(name, created_by, created_at_ms, created_by_origin) VALUES(?, ?, ?, ?)");
        sqlite3_bind_text(insert.value, 1, event.channel_name().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(insert.value, 2, event.created_by().c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(insert.value, 3, event.created_at_unix_ms());
        sqlite3_bind_text(insert.value, 4, event.created_by_origin().c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(insert.value) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(database));
    }

    std::vector<bbs::ReplicationEvent> pending_events(const std::string &peer) const {
        sqlite3 *database = open_database();
        Statement query(database, "SELECT e.payload FROM replication_events e JOIN replication_outbox o ON o.event_id=e.event_id WHERE NOT EXISTS (SELECT 1 FROM replication_delivered d WHERE d.event_id=e.event_id AND d.target=?)");
        sqlite3_bind_text(query.value, 1, peer.c_str(), -1, SQLITE_TRANSIENT);
        std::vector<bbs::ReplicationEvent> events;
        while (sqlite3_step(query.value) == SQLITE_ROW) {
            bbs::ReplicationEvent event;
            const void *data = sqlite3_column_blob(query.value, 0);
            int size = sqlite3_column_bytes(query.value, 0);
            if (event.ParseFromArray(data, size)) events.push_back(event);
        }
        close_database(database);
        return events;
    }

    void mark_delivered(const std::vector<std::string> &event_ids, const std::string &peer) {
        sqlite3 *database = open_database();
        for (const auto &event_id : event_ids) {
            Statement insert(database, "INSERT OR IGNORE INTO replication_delivered(event_id, target) VALUES(?, ?)");
            sqlite3_bind_text(insert.value, 1, event_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(insert.value, 2, peer.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(insert.value) != SQLITE_DONE) {
                std::string error = sqlite3_errmsg(database);
                close_database(database);
                throw std::runtime_error(error);
            }
        }
        close_database(database);
    }

    void replication_loop() {
        std::this_thread::sleep_for(id_ == "server-1" ? seconds(1) : seconds(5));
        while (true) {
            for (const auto &peer : known_servers()) {
                if (peer == id_) continue;
                try {
                    bbs::PeerRequest pull;
                    pull.set_action("replicate_pull");
                    auto remote = peer_request(peer, pull, true);
                    if (remote.ok()) {
                        for (const auto &event : remote.events()) {
                            apply_remote_event(event);
                        }
                    }
                    auto pending = pending_events(peer);
                    if (!pending.empty()) {
                        bbs::PeerRequest apply;
                        apply.set_action("replicate_apply");
                        for (const auto &event : pending) {
                            apply.add_events()->CopyFrom(event);
                        }
                        auto response = peer_request(peer, apply, true);
                        if (response.ok()) {
                            std::vector<std::string> applied(
                                response.applied_event_ids().begin(),
                                response.applied_event_ids().end()
                            );
                            mark_delivered(applied, peer);
                        }
                    }
                } catch (const std::exception &error) {
                    std::cerr << "Falha na replicação de " << id_
                              << " com " << peer << ": " << error.what() << "\n";
                }
            }
            std::this_thread::sleep_for(seconds(5));
        }
    }

    void announce_coordinator() {
        zmq::socket_t publisher(context_, zmq::socket_type::pub);
        publisher.set(zmq::sockopt::linger, 1000);
        publisher.connect(pubsub_xsub_);
        std::this_thread::sleep_for(milliseconds(100));
        bbs::ControlEnvelope announcement = new_control();
        announcement.mutable_coordinator_announcement()->set_coordinator(id_);
        std::string bytes = announcement.SerializeAsString();
        publisher.send(zmq::buffer("servers"), zmq::send_flags::sndmore);
        publisher.send(zmq::buffer(bytes), zmq::send_flags::none);
    }

    void synchronize_clock() {
        std::string winner = id_;
        uint32_t highest_rank = static_cast<uint32_t>(rank_.load());
        for (const auto &peer : known_servers()) {
            if (peer == id_) continue;
            bbs::PeerRequest election;
            election.set_action("election");
            auto response = peer_request(peer, election, false);
            if (response.ok()
                && std::make_pair(response.rank(), response.server_name())
                    > std::make_pair(highest_rank, winner)) {
                highest_rank = response.rank();
                winner = response.server_name();
            }
        }
        set_coordinator(winner);
        if (winner == id_) {
            announce_coordinator();
            std::vector<long long> times{now_ms()};
            for (const auto &peer : known_servers()) {
                if (peer == id_) continue;
                bbs::PeerRequest report;
                report.set_action("clock_report");
                auto response = peer_request(peer, report, false);
                if (response.ok()) times.push_back(response.server_time_unix_ms());
            }
            adjust_clock(average(times));
            return;
        }
        bbs::PeerRequest announce;
        announce.set_action("announce_coordinator");
        announce.set_coordinator(winner);
        peer_request(winner, announce, false);
        bbs::PeerRequest clock;
        clock.set_action("clock");
        clock.set_server_name(id_);
        clock.set_server_time_unix_ms(now_ms());
        auto response = peer_request(winner, clock, false);
        if (response.ok()) {
            adjust_clock(response.correct_time_unix_ms());
        } else {
            set_coordinator(id_);
            announce_coordinator();
        }
    }

    static long long average(const std::vector<long long> &values) {
        long long sum = 0;
        for (long long value : values) sum += value;
        return values.empty() ? 0 : sum / static_cast<long long>(values.size());
    }

    void adjust_clock(long long target) {
        long long difference = target - now_ms();
        difference = std::clamp(difference, -1000LL, 1000LL);
        physical_offset_.fetch_add(difference);
    }

    void run_bot() {
        const std::string username = env("BOT_NAME", "cpp-bot");
        const std::string initial_channel = env("CHANNEL_NAME", "geral");
        const std::string identity = username + "-" + std::to_string(system_now_ms());
        std::string current_server;
        refresh_servers();
        zmq::socket_t subscriber(context_, zmq::socket_type::sub);
        subscriber.connect(pubsub_xpub_);

        auto request = [&](const std::string &server, bbs::Envelope message)
            -> std::optional<bbs::Envelope> {
            zmq::socket_t socket(context_, zmq::socket_type::req);
            socket.set(zmq::sockopt::routing_id, identity);
            socket.set(zmq::sockopt::rcvtimeo, 1500);
            socket.set(zmq::sockopt::sndtimeo, 1500);
            socket.set(zmq::sockopt::linger, 0);
            socket.connect(broker_frontend_);
            std::string bytes = message.SerializeAsString();
            if (!socket.send(zmq::buffer(server), zmq::send_flags::sndmore)
                || !socket.send(zmq::buffer(bytes), zmq::send_flags::none)) {
                return std::nullopt;
            }
            zmq::message_t reply;
            if (!socket.recv(reply)) return std::nullopt;
            bbs::Envelope response;
            if (!response.ParseFromArray(reply.data(), static_cast<int>(reply.size()))) {
                return std::nullopt;
            }
            receive_clock(response.logical_clock());
            std::string text;
            google::protobuf::TextFormat::PrintToString(message, &text);
            std::cout << "ENVIADO para " << server << ":\n" << text;
            text.clear();
            google::protobuf::TextFormat::PrintToString(response, &text);
            std::cout << "RECEBIDO:\n" << text;
            return response;
        };

        auto login = [&](const std::string &server) {
            bbs::Envelope message = new_envelope();
            message.mutable_login_request()->set_username(username);
            auto response = request(server, message);
            return response && response->has_login_response()
                && response->login_response().accepted();
        };

        while (current_server.empty()) {
            refresh_servers();
            if (login("AUTO")) {
                current_server = "AUTO";
            }
            for (const auto &server : known_servers()) {
                if (!current_server.empty()) break;
                if (login(server)) {
                    current_server = server;
                    break;
                }
            }
            if (current_server.empty()) std::this_thread::sleep_for(seconds(2));
        }

        auto send_operation = [&](auto fill_request) {
            refresh_servers();
            bbs::Envelope message = new_envelope();
            fill_request(message);
            auto response = current_server.empty()
                ? std::optional<bbs::Envelope>()
                : request(current_server, message);
            if (response) return response;
            for (const auto &server : known_servers()) {
                if (server == current_server || !login(server)) continue;
                current_server = server;
                message = new_envelope();
                fill_request(message);
                return request(current_server, message);
            }
            return std::optional<bbs::Envelope>();
        };

        std::vector<std::string> channels;
        auto listing = send_operation([](bbs::Envelope &message) {
            message.mutable_list_channels_request();
        });
        if (listing && listing->has_list_channels_response()) {
            for (const auto &name : listing->list_channels_response().channel_names()) {
                channels.push_back(name);
            }
        }
        if (channels.size() < 5) {
            const std::string name =
                std::find(channels.begin(), channels.end(), initial_channel)
                    == channels.end()
                ? initial_channel
                : "canal-" + std::to_string(system_now_ms()) + "-"
                    + std::to_string(logical_clock_.load());
            send_operation([&](bbs::Envelope &message) {
                message.mutable_create_channel_request()->set_name(name);
            });
        }
        listing = send_operation([](bbs::Envelope &message) {
            message.mutable_list_channels_request();
        });
        channels.clear();
        if (listing && listing->has_list_channels_response()) {
            for (const auto &name : listing->list_channels_response().channel_names()) {
                channels.push_back(name);
            }
        }
        if (channels.empty()) throw std::runtime_error("Nenhum canal está disponível.");
        auto history = send_operation([](bbs::Envelope &message) {
            message.mutable_history_request();
        });
        if (history && history->has_history_response()) {
            for (const auto &publication : history->history_response().publications()) {
                std::cout << "HISTÓRICO canal=" << publication.channel_name()
                    << " usuário=" << publication.username()
                    << " enviado_em=" << publication.sent_at_unix_ms()
                    << " conteúdo=" << publication.content() << "\n";
            }
        } else if (history && history->has_error_response()) {
            std::cerr << "Falha ao recuperar histórico: "
                << history->error_response().detail() << "\n";
        }
        std::mt19937 generator(static_cast<unsigned int>(system_now_ms()));
        std::shuffle(channels.begin(), channels.end(), generator);
        for (size_t index = 0; index < std::min<size_t>(3, channels.size()); ++index) {
            subscriber.set(zmq::sockopt::subscribe, channels[index]);
        }
        std::cout << "Bot C++ inscrito em três canais.\n";
        std::this_thread::sleep_for(milliseconds(250));

        const std::vector<std::string> samples{
            "mensagem distribuída", "novo evento", "teste pubsub", "olá bbs"
        };
        while (true) {
            const std::string channel = channels[generator() % channels.size()];
            for (int index = 1; index <= 10; ++index) {
                bbs::Envelope message = new_envelope();
                message.mutable_publish_request()->set_channel_name(channel);
                message.mutable_publish_request()->set_content(
                    username + ": " + samples[generator() % samples.size()]
                    + " " + std::to_string(index)
                );
                auto response = request(current_server, message);
                if (response && response->has_publish_response()) {
                    std::cout << "PUBLICAÇÃO " << response->publish_response().detail() << "\n";
                }
                auto deadline = steady_clock::now() + seconds(1);
                while (steady_clock::now() < deadline) {
                    zmq::pollitem_t item{subscriber.handle(), 0, ZMQ_POLLIN, 0};
                    zmq::poll(&item, 1, milliseconds(50));
                    if (!(item.revents & ZMQ_POLLIN)) continue;
                    zmq::message_t topic;
                    zmq::message_t payload;
                    if (!subscriber.recv(topic) || !subscriber.recv(payload)) {
                        continue;
                    }
                    bbs::Envelope publication;
                    if (publication.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
                        receive_clock(publication.logical_clock());
                        std::cout << "RECEBIDO canal="
                            << std::string(static_cast<char *>(topic.data()), topic.size())
                            << " conteúdo=" << publication.publication().content()
                            << " enviado_em=" << publication.sent_at_unix_ms()
                            << " recebido_em=" << now_ms() << "\n";
                    }
                }
            }
        }
    }
};

int main() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    try {
        BbsNode node;
        node.run();
    } catch (const std::exception &error) {
        std::cerr << "Falha no processo BBS C++: " << error.what() << "\n";
        return 1;
    }
    return 0;
}
