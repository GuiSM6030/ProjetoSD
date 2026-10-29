import zmq

context = zmq.Context()
sub = context.socket(zmq.SUB)
# O terceiro subscriber assina os dois tópicos e, por isso, recebe tudo.
sub.setsockopt_string(zmq.SUBSCRIBE, "hora")
sub.setsockopt_string(zmq.SUBSCRIBE, "numero")
sub.connect("tcp://localhost:5556")

while True:
    message = sub.recv_string()
    print(f"S3 recebeu: {message}", flush=True)

sub.close()
context.close()
