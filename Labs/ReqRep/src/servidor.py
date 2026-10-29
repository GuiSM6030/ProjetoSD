import zmq
import json

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

print("Servidor ZeroMQ escutando em tcp://broker:5556...", flush=True)

tarefas = list()

while True:
    msg = socket.recv()
    msg = json.loads(msg)
    print(msg["acao"])

    if msg["acao"] == "criar":
        tarefas.append(msg["tarefa"])
        msg = {"resposta": "criada!"}

    elif msg["acao"] == "listar":
        msg = {"resposta": tarefas}

    elif msg["acao"] == "deletar":
        tarefas.pop(msg["indice"])
        msg = {"resposta": "apagada!"}

    elif msg["acao"] == "atualizar":
        tarefas[msg["indice"]]["status"] = msg["novo_status"]
        msg = {"resposta": "editada!"}

    else:
        msg = {"error": "Ação não encontrada"}

    msg = json.dumps(msg)
    socket.send_string(msg)
