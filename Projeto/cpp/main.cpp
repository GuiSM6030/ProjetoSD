#include <zmq.hpp>
#include <google/protobuf/util/time_util.h>
#include <sqlite3.h>
#include "bbs.pb.h"
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

using namespace std::chrono;

static long long now_ms() {
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static sqlite3 *open_db(const char *path) {
    sqlite3 *db = nullptr;
    sqlite3_open(path, &db);
    sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS logins(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, logged_at_ms INTEGER)", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS channels(name TEXT PRIMARY KEY, created_by TEXT, created_at_ms INTEGER)", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS publications(message_id TEXT PRIMARY KEY, channel_name TEXT, username TEXT, content TEXT, sent_at_ms INTEGER)", nullptr, nullptr, nullptr);
    return db;
}

static std::string env(const char *name, const char *fallback) {
    const char *value = std::getenv(name);
    return value ? value : fallback;
}

static bool receive(zmq::socket_t &socket, zmq::message_t &message) {
    return socket.recv(message, zmq::recv_flags::none).has_value();
}

static bbs::Envelope response(bbs::Envelope &request) {
    bbs::Envelope reply;
    reply.set_sent_at_unix_ms(now_ms());
    reply.set_message_id("cpp-server-" + std::to_string(now_ms()));
    reply.set_logical_clock(request.logical_clock() + 1);
    return reply;
}

static void server() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    std::string id = env("SERVER_ID", "cpp-server-1");
    std::string broker = env("BROKER_BACKEND", "tcp://broker:5556");
    sqlite3 *db = open_db(env("DATABASE_PATH", "/data/cpp.sqlite3").c_str());
    zmq::context_t context(1);
    zmq::socket_t dealer(context, zmq::socket_type::dealer);
    dealer.set(zmq::sockopt::identity, id);
    dealer.connect(broker);
    zmq::socket_t pub(context, zmq::socket_type::pub);
    pub.connect(env("PUBSUB_XSUB", "tcp://pubsub-proxy:5557"));

    zmq::message_t ready("READY");
    dealer.send(ready, zmq::send_flags::sndmore);
    dealer.send(zmq::buffer(id), zmq::send_flags::none);
    std::cout << "Servidor C++ " << id << " conectado ao broker." << std::endl;

    while (true) {
        zmq::message_t client;
        zmq::message_t payload;
        if (!receive(dealer, client) || !(dealer.recv(payload).has_value())) continue;
        bbs::Envelope request;
        if (!request.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) continue;
        bbs::Envelope reply = response(request);
        if (request.has_login_request()) {
            auto sql = "INSERT INTO logins(username, logged_at_ms) VALUES (?, ?)";
            sqlite3_stmt *stmt = nullptr;
            sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
            sqlite3_bind_text(stmt, 1, request.login_request().username().c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(stmt, 2, request.sent_at_unix_ms());
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
            auto *login = reply.mutable_login_response();
            login->set_accepted(true);
            login->set_detail("Login realizado.");
        } else if (request.has_list_channels_request()) {
            sqlite3_stmt *stmt = nullptr;
            sqlite3_prepare_v2(db, "SELECT name FROM channels ORDER BY name", -1, &stmt, nullptr);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                reply.mutable_list_channels_response()->add_channel_names(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0)));
            }
            sqlite3_finalize(stmt);
        } else if (request.has_create_channel_request()) {
            sqlite3_stmt *stmt = nullptr;
            sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO channels(name, created_by, created_at_ms) VALUES (?, ?, ?)", -1, &stmt, nullptr);
            sqlite3_bind_text(stmt, 1, request.create_channel_request().name().c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, "cpp-client", -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(stmt, 3, request.sent_at_unix_ms());
            sqlite3_step(stmt);
            bool created = sqlite3_changes(db) == 1;
            sqlite3_finalize(stmt);
            reply.mutable_create_channel_response()->set_created(created);
            if (!created) reply.mutable_create_channel_response()->set_error_code("CHANNEL_EXISTS");
        } else if (request.has_publish_request()) {
            auto publication = request.publish_request();
            std::string data;
            reply.mutable_publish_response()->set_published(true);
            bbs::Envelope event;
            event.set_sent_at_unix_ms(now_ms());
            event.set_message_id(request.message_id());
            event.set_logical_clock(request.logical_clock() + 1);
            event.mutable_publication()->set_channel_name(publication.channel_name());
            event.mutable_publication()->set_content(publication.content());
            event.SerializeToString(&data);
            pub.send(zmq::buffer(publication.channel_name()), zmq::send_flags::sndmore);
            pub.send(zmq::buffer(data), zmq::send_flags::none);
        } else {
            reply.mutable_error_response()->set_error_code("UNKNOWN_REQUEST");
        }
        std::string serialized;
        reply.SerializeToString(&serialized);
        dealer.send(client, zmq::send_flags::sndmore);
        dealer.send(zmq::buffer(serialized), zmq::send_flags::none);
    }
}

static void bot() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    std::string username = env("BOT_NAME", "cpp-bot");
    zmq::context_t context(1);
    zmq::socket_t sub(context, zmq::socket_type::sub);
    sub.connect(env("PUBSUB_XPUB", "tcp://pubsub-proxy:5558"));
    sub.set(zmq::sockopt::subscribe, "geral");

    while (true) {
        zmq::socket_t req(context, zmq::socket_type::req);
        req.connect(env("BROKER_FRONTEND", "tcp://broker:5555"));
        req.set(zmq::sockopt::rcvtimeo, 1500);
        bbs::Envelope login;
        login.set_sent_at_unix_ms(now_ms());
        login.set_message_id("cpp-bot-" + std::to_string(now_ms()));
        login.set_logical_clock(1);
        login.mutable_login_request()->set_username(username);
        std::string payload;
        login.SerializeToString(&payload);
        req.send(zmq::buffer("server-cpp-1"), zmq::send_flags::sndmore);
        req.send(zmq::buffer(payload), zmq::send_flags::none);
        zmq::message_t response;
        if (req.recv(response).has_value()) {
            std::cout << "Login C++ realizado em server-cpp-1." << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

int main(int argc, char **argv) {
    std::string mode = env("APP_MODE", "server");
    if (mode == "bot") bot(); else server();
    return 0;
}
