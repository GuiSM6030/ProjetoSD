# Projeto de Sistemas Distribuídos

## Parte 1

Nesta parte, os bots fazem login, consultam os canais existentes e criam o canal configurado caso ele ainda não exista. Não há entrada pelo teclado.

## Escolhas

- Os bots e servidores foram feitos em Python. O broker foi feito em C, então o projeto usa duas linguagens.
- A comunicação usa ZeroMQ. O broker encaminha cada pedido ao servidor indicado pelo cliente.
- As mensagens usam Protocol Buffers em formato binário. O `Envelope` tem o timestamp em milissegundos e o tipo de pedido ou resposta. O arquivo do protocolo é `protocol/bbs.proto`.
- Cada servidor grava seus dados em um banco SQLite próprio. O Compose usa um volume diferente para cada servidor. São salvos os logins aceitos, com timestamp, e os canais criados.

Os nomes de usuário podem ter até 32 letras, números, `_` ou `-`. Os canais seguem a mesma regra, mas devem começar com letra ou número. Se o canal já existe, o servidor responde com `CHANNEL_EXISTS`.

Os bots tentam primeiro o servidor `server-1`. Se ele não responder, tentam o `server-2` e fazem login nele. Como os bancos são separados, os canais não são copiados de um servidor para o outro.

## Executar

Na pasta `Projeto/`, execute:

```sh
docker compose up --build
```

O Compose inicia dois bots, dois servidores e o broker. Os programas mostram no terminal as mensagens enviadas e recebidas. Para parar, use `Ctrl+C`. O comando `docker compose down -v` também apaga os volumes com os dados.

## Arquivos

- `client/bot.py`: bot que faz login, lista canais e cria o canal configurado.
- `server/server.py`: recebe e responde aos pedidos.
- `server/store.py`: grava e consulta os dados SQLite.
- `broker/broker.c`: encaminha mensagens ZeroMQ entre bots e servidores.
- `protocol/bbs.proto`: formato das mensagens.
- `docker-compose.yaml`: configuração dos containers.
