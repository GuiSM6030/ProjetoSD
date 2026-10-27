from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

@app.get("/")
def root():
    return {"message": "hello world"}

counter = 0

@app.get("/count")
def get_count():
    global counter
    counter += 1
    return counter

@app.get("/hello")
def hello_world():
    return "Hello, world"

@app.get("/hello/{name}")
def hello(name):
    return f"Hello, {name}"

@app.get("/hello/")
def hello(parameter = "World"):
    return f"Hello, {parameter}"

class Pessoa(BaseModel):
    nome: str
    sobrenome: str
    idade: int

@app.post("/pessoa/")
def criar_pessoa(pessoa: Pessoa):
    return pessoa

tarefas = list()

class Tarefa(BaseModel):
    tarefa: str
    prioridade: int
    feito: bool

@app.get("/")
def root():
    return tarefas

# retorna dados de nome, prioridade e status sobre uma tarefa existente recebendo sua posição na lista
@app.get("/tarefa/{pos}")
def get_tarefa(pos: int):
    return tarefas[pos]

# função que cria uma tarefa com nome, prioridade e status
@app.post("/adicionar/")
def criar_tarefa(tarefa: Tarefa):
    tarefa.feito = False
    tarefas.append(tarefa)
    return len(tarefas)

# função que muda o status de uma tarefa para feita
@app.put("/feito/{pos}")
def marcar_feito(pos: int):
    tarefas[pos].feito = True
    return tarefas[pos]

# função que deleta uma tarefa específica usando sua posição na lista
@app.delete("/deletar/{pos}")
def deletar_tarefa(pos: int):
    tarefa = tarefas.pop(pos)
    return tarefa