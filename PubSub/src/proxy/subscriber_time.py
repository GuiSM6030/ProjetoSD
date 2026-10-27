import zmq


context = zmq.Context()
sub = context.socket(zmq.SUB)
# Assinar "hora" faz este subscriber receber somente as mensagens de P1.
sub.setsockopt_string(zmq.SUBSCRIBE, "hora")
sub.connect("tcp://localhost:5556")

while True:
    message = sub.recv_string()
    print(f"S1 recebeu apenas a hora: {message}", flush=True)

sub.close()
context.close()