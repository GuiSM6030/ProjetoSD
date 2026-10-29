# Projeto de Sistemas Distribuídos

# Projeto de Sistemas Distribuídos

## Arquitetura

O sistema continua dividido em clientes Python, servidores Python e um broker Req/Rep em C. O broker encaminha pedidos entre as portas `5555` e `5556`. A Parte 2 adiciona um proxy Pub/Sub isolado em `pubsub_proxy.py`, com `XSUB` em `5557` e `XPUB` em `5558`.

```text
bot (REQ) -> broker 5555 -> servidor (DEALER)
servidor (PUB) -> proxy XSUB 5557 -> proxy XPUB 5558 -> bot (SUB)
```

O nome do canal é usado como tópico e como primeiro frame da publicação. Assim, o bot pode assinar vários canais na mesma conexão SUB. A escolha das assinaturas pertence ao cliente; o servidor não altera as assinaturas.

## Protocolo

Todas as mensagens são envelopes Protobuf definidos em `protocol/bbs.proto`. O `Envelope` inclui `sent_at_unix_ms` e `message_id`. Os novos tipos são `PublishRequest`, `PublishResponse` e `Publication`:

1. O bot envia ao servidor `{channel_name, content}` com timestamp e `message_id`.
2. O servidor verifica login, nome, conteúdo e existência do canal.
3. Em caso positivo, grava o evento e envia `{topic, Envelope(Publication)}` ao PUB.
4. O servidor responde com `PublishResponse`. Essa resposta confirma apenas aceitação pelo servidor, não entrega a um assinante.

Uma publicação positiva não é confirmação de recebimento Pub/Sub. A entrega do assinante é observada apenas pela mensagem recebida pelo SUB e pelo timestamp local de recebimento do bot.

Os timestamps são relógios Unix em milissegundos. O envio usa o relógio de quem cria o `Envelope`; no recebimento, o bot registra outro timestamp local ao processar o frame.

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
```

O código Protobuf não é versionado manualmente; ele é gerado pelo `Dockerfile.python` durante o build. `docker compose down -v` remove os volumes e os dados persistidos.

## Limitações ZeroMQ

PUB/SUB não mantém fila enquanto um assinante está desconectado. O proxy não garante entrega, ordem global entre servidores, persistência de mensagens ou confirmação de recebimento. Uma `PublishResponse` aceita descreve somente o processamento do servidor. Em caso de falha de um servidor, o bot tenta o servidor seguinte, mas o estado permanece distribuído entre bancos separados.

## Arquivos

- `client/bot.py`: login, failover, gestão de canais, assinaturas SUB e publicação.
- `server/server.py`: processamento Req/Rep, validação e publicação.
- `server/store.py`: persistência SQLite parametrizada.
- `pubsub_proxy.py`: transporte Pub/Sub com XSUB/XPUB.
- `broker/broker.c`: encaminhamento Req/Rep entre clientes e servidores.
- `protocol/bbs.proto`: contratos Protobuf.
- `docker-compose.yaml`: configuração dos containers.
