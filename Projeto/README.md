# Projeto de Sistemas Distribuídos

Esse projeto é um sistema distribuído de troca de mensagens tipo BBS/IRC, com bots simulando usuários, servidores em diferentes linguagens e um mecanismo de coordenação e replicação entre nós.

A ideia principal é que o cliente não fale direto com o servidor “fixo”. Em vez disso, os bots enviam requisições para um broker, o servidor valida a operação, salva em disco e, quando necessário, publica no canal correto via Pub/Sub. Além disso, há referência para rank/heartbeat e eleição de coordenador para sincronizar relógios entre os servidores.

## Linguagens e estrutura

O projeto usa três implementações principais:
- Python
- Java
- C++

Cada uma delas tem servidor e bot, e o ambiente inteiro é orquestrado com Docker Compose.

Estrutura principal:
- `broker/` — broker Req/Rep em C
- `pubsub_proxy.py` — proxy XSUB/XPUB para canais
- `server/` — lógica do servidor em Python
- `java/` — servidor e bot em Java
- `cpp/` — servidor e bot em C++
- `protocol/` — contrato Protobuf compartilhado
- `tests/` — testes do projeto

## Como a comunicação funciona

A comunicação usa ZeroMQ e Protobuf.

### 1) Req/Rep para cliente e servidor
Os bots enviam mensagens para o broker, que distribui entre os servidores disponíveis. Isso é usado para login, lista de canais, criação de canais e publicação.

### 2) Pub/Sub para canais
Quando um servidor aceita uma publicação, ele faz `PUB` no tópico do canal. Os bots que estiverem inscritos no canal recebem a mensagem e exibem no console.

### 3) Referência para rank e heartbeat
Existe um serviço de referência que mantém a lista de servidores disponíveis e devolve o rank de cada um.

### 4) Coordenação entre servidores
Os servidores trocam mensagens entre si para decidir quem é o coordenador. O servidor com maior rank vira coordenador e faz a sincronização Berkeley do relógio.

### 5) Replicação
Cada servidor persiste os eventos locais e envia cópias para os outros servidores, para que todos tenham o histórico o mais parecido possível.

## Serialização e timestamps

A troca de mensagens foi feita em Protobuf, então tudo fica em binário e não em JSON/XML/texto puro.

Toda mensagem leva:
- timestamp de envio
- identificador da mensagem
- relógio lógico

Isso vale tanto para mensagens do cliente/servidor quanto para mensagens internas de coordenação e replicação.

## Relógio lógico

Cada processo mantém um relógio lógico.

A regra é simples:
- antes de enviar uma mensagem, o contador aumenta
- ao receber uma mensagem, o processo usa o máximo entre o valor atual e o recebido
- depois incrementa novamente e segue

Isso ajuda a manter a ordem parcial dos eventos, mesmo sem relógio físico perfeito.

## Relógio e coordenação

Os servidores também têm um relógio físico local, mas ele é ajustado por uma sincronização controlada. O coordenador calcula a média dos relógios dos servidores e manda a hora correta para os demais.

Se o coordenador cai, a eleição é feita novamente. O nome do servidor eleito é publicado no tópico `servers`, e os demais servidores atualizam o coordenador atual.

## Persistência em disco

Cada servidor tem um banco SQLite próprio, então não existe um banco central compartilhado.

Os dados guardados incluem:
- logins
- canais criados
- publicações
- mensagens trocadas
- eventos de replicação

Isso evita perder informações quando o processo reinicia e também ajuda a recuperar o histórico de mensagens.

## Replicação

A replicação foi implementada com eventos e outbox. Cada operação relevante gera um evento, esse evento é gravado localmente e depois enviado para os pares. Os outros servidores aplicam o evento quando recebem e marcam como processado.

A ideia é manter convergência eventual: todos os servidores tendem a ficar iguais ao longo do tempo, mesmo que não seja instantâneo. Não é consistência forte global, mas dá para manter o histórico distribuído.

## O que cada parte do projeto faz

### Parte 1 — Login e canais
- bot faz login
- lista canais disponíveis
- cria canais quando necessário
- persiste os dados em disco

### Parte 2 — Publicação em canais
- o bot se inscreve em canais
- o servidor publica a mensagem no tópico do canal
- os bots inscritos recebem a mensagem e exibem no console
- o projeto usa proxy Pub/Sub para isso

### Parte 3 — Relógio e heartbeat
- cada mensagem carrega relógio lógico
- os servidores fazem heartbeat com a referência
- a referência guarda rank e lista de servidores disponíveis

### Parte 4 — Eleição e coordenador
- maior rank vira coordenador
- coordenador sincroniza relógios dos servidores
- anúncio do coordenador é feito em tópico `servers`

### Parte 5 — Replicação
- eventos são replicados entre servidores
- dados ficam disponíveis mesmo se um nó cair
- histórico é preservado e sincronizado entre os pares

## Como rodar

Na pasta do projeto:

```bash
docker compose up --build
```

Isso sobe:
- broker
- proxy Pub/Sub
- referência
- servidores Python, Java e C++
- bots em Python, Java e C++

Para limpar tudo e começar do zero:

```bash
docker compose down -v
```

## Escolhas do projeto

A gente escolheu usar:
- ZeroMQ para comunicação distribuída
- Protobuf para serialização binária
- SQLite para persistência local por servidor
- Docker Desktop para execução do ambiente
- referência + eleição + replicação para deixar o sistema mais parecido com um ambiente distribuído real
