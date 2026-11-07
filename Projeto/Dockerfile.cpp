FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake protobuf-compiler libprotobuf-dev libzmq3-dev libsqlite3-dev curl ca-certificates && rm -rf /var/cache/apt/* /var/lib/apt/lists/*
RUN mkdir -p /usr/local/include && curl -fsSL https://raw.githubusercontent.com/zeromq/cppzmq/master/zmq.hpp -o /usr/local/include/zmq.hpp
WORKDIR /build
COPY cpp/ cpp/
COPY protocol/bbs.proto protocol/bbs.proto
RUN cmake -S cpp -B build && cmake --build build -j2

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libprotobuf32 libzmq5 libsqlite3-0 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/build/bbs_cpp /app/bbs_cpp
CMD ["/app/bbs_cpp"]
