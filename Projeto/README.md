# Projeto de Sistemas Distribuídos

## Arquitetura

O Compose executa dois clientes e dois servidores por linguagem: Python, Java e C++ orientado a objetos. Portanto, os seis bots e os seis servidores participam da mesma aplicação. O broker Req/Rep é implementado em C e encaminha os pedidos sem interpretar os envelopes. Ao abrir uma sessão com o destino `AUTO`, distribui os clientes entre os servidores registrados em round-robin e mantém afinidade por identidade para preservar o login; os bots podem selecionar outro servidor explicitamente em caso de falha.

Os servidores C++ reenviam periodicamente o registro `READY` ao broker para recuperar o roteamento caso o broker reinicie enquanto eles continuam ativos.

```text
clientes REQ -> broker C :5555 -> servidores DEALER :5556
servidores PUB :5557 -> proxy XSUB/XPUB -> clientes SUB :5558
servidores <-> referência REQ/REP :5559
servidores <-> coordenação Berkeley REQ/REP :5561
servidores <-> replicação durável REQ/REP :5560
```

Cada servidor tem um arquivo SQLite e volume próprios. Os seis servidores replicam os dados entre si; os serviços Java e C++ não são apenas clientes do servidor Python. Cada bot pode ser atendido por qualquer um dos seis servidores; se o servidor da sessão falhar, tenta outro servidor disponível.

## Protocolo

`protocol/bbs.proto` define os envelopes Protobuf usados por todas as linguagens. Os pedidos e respostas de login, canais e publicação usam `Envelope`; referência, eleição, relógio, anúncio de coordenador e replicação usam `ControlEnvelope`. Os dois incluem timestamp Unix em milissegundos, identificador e relógio lógico. Todos os conteúdos de aplicação enviados pela rede são serializados em Protobuf binário; nome de servidor e tópico Pub/Sub são frames de roteamento.

1. O cliente envia login, lista canais, cria canais e publica mensagens em canais.
2. O servidor valida o pedido, exige login para as operações de canal e permite recuperar o histórico completo ou filtrado por canal.
3. Uma publicação aceita é persistida e enviada no tópico com o nome do canal.
4. `PublishResponse` confirma apenas a aceitação pelo servidor, não a entrega a assinantes.

## Relógios e referência

Clientes e servidores mantêm relógios lógicos Lamport: incrementam antes de enviar e, ao receber, aplicam `max(local, recebido) + 1`. O relógio físico dos servidores aplica um deslocamento local, sem alterar o relógio do sistema.

O serviço `reference` usa Protobuf binário para atribuir ranks, fornecer a lista de servidores disponíveis e renovar heartbeats. O rank é estável para cada nome durante a execução da referência. Cada servidor envia heartbeat a cada dez pedidos de clientes recebidos; após 60 segundos sem heartbeat o servidor deixa de aparecer como disponível. Os bots consultam a referência ao selecionar um servidor, e os servidores renovam suas listas de pares no heartbeat. Isso permite adicionar processos durante a execução e retirar processos sem reiniciar os demais; a remoção da lista de disponíveis ocorre no timeout de heartbeat.

O servidor de maior rank entre os pares acessíveis coordena a sincronização Berkeley: coleta os horários, calcula a média inteira e a distribui. Uma falha do coordenador dispara nova eleição; o vencedor é anunciado no tópico `servers`. O ajuste de relógio é gradual e não sincroniza os relógios do host ou do container.

## Comportamento dos bots

Cada bot faz login e consulta os canais. Se houver menos de cinco, cria um canal; em seguida, inscreve-se em até três canais aleatórios e publica mensagens aleatórias em lotes de dez, com intervalo de um segundo. Os bots exibem os envelopes enviados e recebidos e, para publicações, o tópico e os timestamps.

PUB/SUB não mantém fila para assinantes desconectados e não fornece confirmação nem retransmissão. Uma mensagem pode ser perdida se um assinante ainda não estiver inscrito ou estiver indisponível.

Ao iniciar, cada bot também solicita o histórico armazenado no servidor, exibindo canal, autor, timestamp e conteúdo. A operação aceita um canal opcional; sem filtro, retorna todas as publicações que já foram replicadas para aquele servidor.

## Persistência e consistência

Cada servidor persiste localmente logins, canais, publicações e as mensagens de pedido e resposta trocadas com clientes. O Compose monta um volume distinto para cada um dos seis bancos SQLite.

A replicação usa eventos Protobuf e outbox durável no canal interno REQ/REP `5560`. Cada evento local e sua outbox são gravados junto com a operação. O produtor envia até 100 eventos pendentes por lote e marca a entrega por destino somente após o ACK. `replicate_pull` busca lotes incrementais usando um cursor persistido por par, permitindo recuperar o histórico de um servidor que voltou sem retransmitir todo o histórico a cada ciclo. Os eventos têm identificadores estáveis e são aplicados de forma idempotente, sem eco de eventos recebidos; os logs de replicação registram contagens por par em vez de despejar payloads completos.

Os seis servidores implementam o mesmo formato de evento, permitindo replicação entre Python, Java e C++. Canais concorrentes convergem pela menor chave `(created_at_unix_ms, servidor de origem, usuário criador)`. A solução oferece convergência eventual e entrega pelo menos uma vez; não oferece transação distribuída nem linearizabilidade global. Uma resposta positiva de publicação não garante que todos os pares já tenham recebido o evento.

Para acrescentar um servidor Python enquanto o Compose está ativo, inicie um container com ID e alias DNS únicos na rede `projeto-bbs-network`, usando o mesmo comando e imagem Python do serviço `server-1` e um volume de dados exclusivo:

```sh
docker volume create extra-server-data
docker run -d --name extra-server \
  --network projeto-bbs-network --network-alias extra-server \
  -e SERVER_ID=extra-server \
  -e BROKER_BACKEND=tcp://broker:5556 \
  -e DATABASE_PATH=/data/server.sqlite3 \
  -e PUBSUB_XSUB=tcp://pubsub-proxy:5557 \
  -e PUBSUB_XPUB=tcp://pubsub-proxy:5558 \
  -e REFERENCE_ENDPOINT=tcp://reference:5559 \
  -e SERVER_IDS=server-1,server-2,java-server-1,java-server-2,cpp-server-1,cpp-server-2,extra-server \
  -v extra-server-data:/data projeto-server-1
```

O nome `projeto-server-1` é a imagem construída pelo Compose para `server-1`. O alias deve corresponder a `SERVER_ID`, pois os pares usam esse nome para acessar as portas internas de coordenação e replicação. Para removê-lo, use `docker stop extra-server && docker rm extra-server`; ele deixa de ser anunciado após expirar o heartbeat (até 60 segundos).

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
