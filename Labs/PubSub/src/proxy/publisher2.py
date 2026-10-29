import random
from time import sleep

import zmq


context = zmq.Context()
pub = context.socket(zmq.PUB)
pub.connect("tcp://localhost:5555")

while True:
    # O tópico "numero" separa os números das mensagens de horário.
    number = random.randint(1, 6)
    pub.send_string(f"numero {number}")
    print(f"P2 enviou o número: {number}", flush=True)
    sleep(1)

pub.close()
context.close()