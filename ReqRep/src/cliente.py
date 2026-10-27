import json
from time import sleep
import zmq

print("Cliente conectando com o broker ZeroMQ", flush=True)

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")

while True:
    # CRIAR NOVA TAREFA
    # criando tarefa e enviando
    msg = {
        "acao": "criar",
        "tarefa": {
            "nome": "fazer projeto de jogos digitais",
            "status": "fazer",
            "indice": 0
        }
    }
    msg = json.dumps(msg)
    print(msg, flush=True)
    socket.send_string(msg)

    # recebendo resposta
    msg = socket.recv_string()
    msg = json.loads(msg)
    print(msg, flush=True)

    sleep(4)

    # LISTANDO TAREFAS EXISTENTES
    msg = {
        "acao": "listar"
    }
    msg = json.dumps(msg)
    print(msg, flush=True)
    socket.send_string(msg)

    # recebendo resposta
    msg = socket.recv_string()
    msg = json.loads(msg)
    print(msg, flush=True)

    sleep(4)

    # EDITAR STATUS DE UMA TAREFA
    msg = {
        "acao": "atualizar",
        "indice": 0,
        "novo_status": "feito"
    }
    msg = json.dumps(msg)
    print(msg, flush=True)
    socket.send_string(msg)

    # recebendo resposta
    msg = socket.recv_string()
    msg = json.loads(msg)
    print(msg, flush=True)

    sleep(4)

    # APAGAR UMA TAREFA
    msg = {
        "acao": "deletar",
        "indice": 0
    }
    msg = json.dumps(msg)
    print(msg, flush=True)
    socket.send_string(msg)

    # recebendo resposta
    msg = socket.recv_string()
    msg = json.loads(msg)
    print(msg, flush=True)

    sleep(4)

