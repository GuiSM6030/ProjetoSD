# Projeto de Sistemas Distribuídos

## Arquitetura

O Compose executa dois clientes e dois servidores por linguagem: Python, Java e C++ orientado a objetos. Portanto, os seis bots e os seis servidores participam da mesma aplicação. O broker Req/Rep é implementado em C e encaminha os pedidos sem interpretar os envelopes.

```text
clientes REQ -> broker C :5555 -> servidores DEALER :5556
servidores PUB :5557 -> proxy XSUB/XPUB -> clientes SUB :5558
servidores <-> referência REQ/REP :5559
servidores <-> coordenação Berkeley REQ/REP :5561
servidores <-> replicação durável REQ/REP :5560
```

Cada servidor tem um arquivo SQLite e volume próprios. Os seis servidores replicam os dados entre si; os serviços Java e C++ não são apenas clientes do servidor Python. O Compose configura todos os bots para tentar os seis servidores.

## Protocolo

`protocol/bbs.proto` define os envelopes Protobuf usados por todas as linguagens. Os pedidos e respostas de login, canais e publicação usam `Envelope`; referência, eleição, relógio, anúncio de coordenador e replicação usam `ControlEnvelope`. Os dois incluem timestamp Unix em milissegundos, identificador e relógio lógico. Todos os conteúdos de aplicação enviados pela rede são serializados em Protobuf binário; nome de servidor e tópico Pub/Sub são frames de roteamento.

1. O cliente envia login, lista canais, cria canais e publica mensagens em canais.
2. O servidor valida o pedido e exige login para as operações de canal.
3. Uma publicação aceita é persistida e enviada no tópico com o nome do canal.
4. `PublishResponse` confirma apenas a aceitação pelo servidor, não a entrega a assinantes.

## Relógios e referência

Clientes e servidores mantêm relógios lógicos Lamport: incrementam antes de enviar e, ao receber, aplicam `max(local, recebido) + 1`. O relógio físico dos servidores aplica um deslocamento local, sem alterar o relógio do sistema.

O serviço `reference` usa Protobuf binário para atribuir ranks, fornecer a lista de servidores disponíveis e renovar heartbeats. O rank é estável para cada nome durante a execução da referência. Os seis nomes são configurados no Compose. Cada servidor envia heartbeat a cada dez pedidos de clientes recebidos; após 60 segundos sem heartbeat o servidor deixa de aparecer como disponível. Como a cadência depende de pedidos, um servidor ocioso pode expirar.

O servidor de maior rank entre os pares acessíveis coordena a sincronização Berkeley: coleta os horários, calcula a média inteira e a distribui. Uma falha do coordenador dispara nova eleição; o vencedor é anunciado no tópico `servers`. O ajuste de relógio é gradual e não sincroniza os relógios do host ou do container.

## Comportamento dos bots

Cada bot faz login, consulta os canais, garante pelo menos cinco canais, escolhe até três canais para inscrição e publica mensagens aleatórias em lotes de dez, com intervalo de um segundo. Os bots exibem os envelopes enviados e recebidos e, para publicações, o tópico e os timestamps.

PUB/SUB não mantém fila para assinantes desconectados e não fornece confirmação nem retransmissão. Uma mensagem pode ser perdida se um assinante ainda não estiver inscrito ou estiver indisponível.

## Persistência e consistência

Cada servidor persiste localmente logins, canais, publicações e as mensagens de pedido e resposta trocadas com clientes. O Compose monta um volume distinto para cada um dos seis bancos SQLite.

A replicação usa eventos Protobuf e outbox durável no canal interno REQ/REP `5560`. Cada evento local e sua outbox são gravados junto com a operação. O produtor envia eventos pendentes e marca a entrega por destino somente após o ACK. `replicate_pull` recupera o histórico de um servidor que voltou. Os eventos têm identificadores estáveis e são aplicados de forma idempotente, sem eco de eventos recebidos.

Os seis servidores implementam o mesmo formato de evento, permitindo replicação entre Python, Java e C++. Canais concorrentes convergem pela menor chave `(created_at_unix_ms, servidor de origem, usuário criador)`. A solução oferece convergência eventual e entrega pelo menos uma vez; não oferece transação distribuída nem linearizabilidade global. Uma resposta positiva de publicação não garante que todos os pares já tenham recebido o evento.

## Executar e testar

Na pasta `Projeto/`, execute:

```sh
docker compose up --build
```

O comando compila e inicia broker, proxy Pub/Sub, referência, seis servidores e seis bots. Os processos dos bots são contínuos; use `Ctrl+C` para encerrar. Para executar os testes Python e validar os builds:

```sh
python -m grpc_tools.protoc -Iprotocol --python_out=. protocol/bbs.proto
python -m unittest discover -s tests -v
mvn -f java/pom.xml package
docker compose config
docker compose build
```

O código Protobuf Python é gerado durante o build e não é versionado manualmente. `docker compose down -v` também remove os volumes e todos os dados persistidos.

## Arquivos principais

- `client/bot.py`, `java/src/main/java/bbs/JavaBot.java`, `cpp/main.cpp`: clientes Python, Java e C++.
- `server/server.py`, `java/src/main/java/bbs/JavaServer.java`, `cpp/main.cpp`: servidores Python, Java e C++.
- `reference.py` e `server/reference_client.py`: registro, ranks e heartbeat.
- `server/coordination.py`: eleição, sincronização Berkeley e protocolo de pares Python.
- `server/store.py`: armazenamento SQLite e eventos de replicação Python.
- `protocol/bbs.proto`: contrato Protobuf comum às três linguagens.
- `broker/broker.c` e `pubsub_proxy.py`: encaminhamento Req/Rep e proxy XSUB/XPUB.
- `docker-compose.yaml`: execução das três implementações em conjunto.
