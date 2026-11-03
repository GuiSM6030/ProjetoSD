# Projeto de Sistemas Distribuídos

# Projeto de Sistemas Distribuídos

## Arquitetura

O sistema continua dividido em clientes Python, servidores Python e um broker Req/Rep em C. O broker encaminha pedidos entre as portas `5555` e `5556`. A Parte 2 adiciona um proxy Pub/Sub isolado em `pubsub_proxy.py`, com `XSUB` em `5557` e `XPUB` em `5558`.

```text
bot (REQ) -> broker 5555 -> servidor (DEALER)
servidor (PUB) -> proxy XSUB 5557 -> proxy XPUB 5558 -> bot (SUB)
servidores (REQ/REP) -> referência 5559
```

O nome do canal é usado como tópico e como primeiro frame da publicação. Assim, o bot pode assinar vários canais na mesma conexão SUB. A escolha das assinaturas pertence ao cliente; o servidor não altera as assinaturas.

## Protocolo

As mensagens entre bots e servidores são envelopes Protobuf definidos em `protocol/bbs.proto`. O `Envelope` inclui `sent_at_unix_ms`, `message_id` e `logical_clock`. Os tipos de publicação são `PublishRequest`, `PublishResponse` e `Publication`:

1. O bot envia ao servidor `{channel_name, content}` com timestamp e `message_id`.
2. O servidor verifica login, nome, conteúdo e existência do canal.
3. Em caso positivo, grava o evento e envia `{topic, Envelope(Publication)}` ao PUB.
4. O servidor responde com `PublishResponse`. Essa resposta confirma apenas aceitação pelo servidor, não entrega a um assinante.

Uma publicação positiva não é confirmação de recebimento Pub/Sub. A entrega do assinante é observada apenas pela mensagem recebida pelo SUB e pelo timestamp local de recebimento do bot.

Os timestamps são relógios Unix em milissegundos. O envio usa o relógio do processo que cria o `Envelope`; no recebimento, o bot registra outro timestamp local ao processar o frame. O `message_id` continua identificando/correlacionando a mensagem quando aplicável.

## Relógios

Cada bot e servidor inicia seu relógio lógico Lamport em zero. Antes de enviar um envelope, incrementa o contador e grava o resultado em `logical_clock`. Ao receber um envelope, atualiza-o para `max(contador_local, logical_clock_recebido) + 1`. Isso vale para pedidos, respostas e publicações; bots também atualizam o contador ao receber respostas e publicações. O campo Protobuf usa o número 3, sem reutilizar os números existentes.

O relógio físico de cada servidor não altera o relógio do sistema. O servidor mantém um deslocamento local aplicado aos timestamps que produz. A cada heartbeat, recebe da referência o horário Unix em milissegundos e aproxima o deslocamento em passos de até 100 ms, evitando saltos grandes no relógio usado pela aplicação.

## Referência, ranks e heartbeat

O container `reference` atende apenas os servidores em `tcp://reference:5559`, por REQ/REP com JSON. Os pedidos e respostas carregam horário de envio, `message_id` e relógio lógico; não passam pelo broker nem pelo proxy Pub/Sub. As ações são `rank` (cadastro e atribuição de rank), `list` (servidores disponíveis) e `heartbeat` (renova disponibilidade e retorna o horário da referência).

Os nomes `server-1` e `server-2` são configurados no Compose em `SERVER_IDS`, portanto recebem ranks estáveis 1 e 2 mesmo que iniciem simultaneamente. Nomes adicionais recebem o próximo rank disponível. Repetir a consulta mantém o mesmo rank. O pedido de lista é feito na inicialização e após cada heartbeat.

O servidor envia heartbeat após cada 10 mensagens de clientes recebidas, incluindo pedidos Protobuf inválidos que chegaram em um frame válido. A referência remove servidores sem heartbeat há mais de 60 segundos da lista disponível; o timeout usa tempo monotônico. O rank fica reservado enquanto a referência estiver em execução, para que uma reconexão do mesmo nome recupere o rank. A consulta de rank cadastra/reativa o servidor. Falhas da referência não param o processamento Req/Rep; o servidor tenta obter o rank novamente no próximo ciclo de heartbeat.

Como a cadência é baseada em mensagens, um servidor ocioso por mais de 60 segundos também sai da lista, mesmo que o processo esteja vivo. O estado de ranks é mantido em memória e é perdido se a referência reiniciar. A sincronização física é aproximada e unidirecional, sem estimativa de latência de rede; ela não sincroniza os relógios do host ou do container.

## Comportamento dos bots

Cada bot faz login, consulta os canais e tenta manter pelo menos cinco canais disponíveis. Duplicidades na criação são tratadas por `CHANNEL_EXISTS`. Em seguida, escolhe aleatoriamente canais disponíveis até possuir pelo menos três assinaturas.

Depois, o bot escolhe um canal disponível e tenta publicar dez mensagens aleatórias. A recepção SUB é consultada a cada fração do intervalo de um segundo, mantendo-a ativa durante todo o lote. Por limitação do PUB/SUB do ZeroMQ, assinantes lentos ou desconectados podem perder mensagens e não há confirmação nem retransmissão automática pelo proxy.

## Persistência

Cada servidor continua tendo seu próprio SQLite e volume. `store.py` cria as tabelas sem apagar os dados existentes. Além de logins e canais, são persistidos:

- `publications`: identificação, canal, autor, conteúdo e timestamp enviado;
- `exchanged_messages`: direção, tipo, correlação, usuário, canal, conteúdo e timestamps enviado/recebido pelo servidor.

As consultas usam parâmetros SQLite. Os volumes são separados por instância; portanto canais criados em servidores diferentes não são sincronizados automaticamente, assim como a Parte 1 já registrava essa limitação.

## Executar e testar

Na pasta `Projeto/`, execute:

```sh
docker compose up --build
```

O Compose inicia o broker, o proxy Pub/Sub, dois servidores e dois bots. Os timestamps enviados e recebidos aparecem nos logs dos bots.

```sh
python -m grpc_tools.protoc -Iprotocol --python_out=. protocol/bbs.proto
python -m unittest discover -s tests -v
docker compose config
docker compose build
```

O código Protobuf não é versionado manualmente; ele é gerado pelo `Dockerfile.python` durante o build. `docker compose down -v` remove os volumes e os dados persistidos.

## Limitações ZeroMQ

PUB/SUB não mantém fila enquanto um assinante está desconectado. O proxy não garante entrega, ordem global entre servidores, persistência de mensagens ou confirmação de recebimento. Uma `PublishResponse` aceita descreve somente o processamento do servidor. Em caso de falha de um servidor, o bot tenta o servidor seguinte, mas o estado permanece distribuído entre bancos separados.

## Arquivos

- `client/bot.py`: login, failover, gestão de canais, assinaturas SUB e publicação.
- `server/server.py`: processamento Req/Rep, validação, publicação e heartbeat.
- `server/reference_client.py` e `reference.py`: protocolo servidor-referência e registry.
- `clocks.py`: relógios lógico e físico com deslocamento local.
- `server/store.py`: persistência SQLite parametrizada.
- `pubsub_proxy.py`: transporte Pub/Sub com XSUB/XPUB.
- `broker/broker.c`: encaminhamento Req/Rep entre clientes e servidores.
- `protocol/bbs.proto`: contratos Protobuf.
- `docker-compose.yaml`: configuração dos containers.
