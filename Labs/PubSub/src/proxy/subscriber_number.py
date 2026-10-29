import zmq


context = zmq.Context()
sub = context.socket(zmq.SUB)
# Assinar "numero" faz este subscriber receber somente as mensagens de P2.
sub.setsockopt_string(zmq.SUBSCRIBE, "numero")
sub.connect("tcp://localhost:5556")

while True:
    message = sub.recv_string()
    print(f"S2 recebeu apenas o número: {message}", flush=True)

sub.close()
context.close()