import zmq
from datetime import datetime
from time import sleep

context = zmq.Context()
pub = context.socket(zmq.PUB)
pub.connect("tcp://localhost:5555")

while True:
    # O tópico "hora" permite que os subscribers escolham apenas as mensagens de horário, sem receber os números do outro publisher.
    message = datetime.now().strftime("%H:%M:%S")
    pub.send_string(f"hora {message}")
    print(f"P1 enviou a hora: {message}", flush=True)
    sleep(1)

pub.close()
context.close()
