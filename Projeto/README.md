# Sistema de troca de mensagens

Este projeto foi feito para a disciplina de Sistemas Distribuídos. Bots entram no sistema, criam ou consultam canais e enviam mensagens. Os bots fazem isso sozinhos, sem precisar de alguém digitando durante a execução.

Há servidores e bots em Python, Java e C++. O Docker Compose inicia tudo junto.

## Como funciona

- O broker recebe os pedidos dos bots e os encaminha aos servidores.
- Os servidores cuidam do login, dos canais e das publicações. Cada servidor guarda seus dados em um banco SQLite próprio.
- Quando um bot publica em um canal, os outros bots inscritos nele recebem a mensagem.
- Os servidores trocam entre si cópias das alterações para manter os dados parecidos. Essa atualização pode levar alguns instantes.
- Um serviço acompanha quais servidores estão ativos. Os servidores escolhem um coordenador para ajudar a ajustar seus relógios.

As mensagens usam ZeroMQ para comunicação e Protocol Buffers para serem enviadas em formato binário. Elas também levam o horário de envio e um contador que ajuda a ordenar os acontecimentos.

## Pastas principais

- `broker/`: recebe e encaminha pedidos.
- `client/`: bot em Python.
- `server/`: servidor em Python.
- `java/`: servidor e bot em Java.
- `cpp/`: servidor e bot em C++.
- `protocol/`: formato das mensagens usadas pelo projeto.
- `tests/`: testes automatizados.
- `docker-compose.yaml`: lista os serviços que rodam juntos.

## Executar o sistema

É necessário ter Docker e Docker Compose instalados. No terminal, a partir da pasta principal do repositório, rode:

```bash
cd Projeto
docker compose up --build
```

O comando compila e inicia os servidores, os bots e os serviços de apoio. Os bots entram nos canais e publicam mensagens; as trocas aparecem no terminal.

Para parar os serviços, pressione `Ctrl+C` e rode:

```bash
docker compose down
```

Para apagar também os dados salvos pelos servidores e começar do zero:

```bash
docker compose down -v
```

## Rodar os testes

Os testes automatizados em Python verificam, entre outras coisas, os relógios, os canais, o serviço de referência e a cópia de dados entre servidores. Para rodá-los, a partir da pasta principal do repositório:

```bash
cd Projeto
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```
