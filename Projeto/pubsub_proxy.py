import zmq

context = zmq.Context()
xpub = context.socket(zmq.XPUB)
xsub = context.socket(zmq.XSUB)
xpub.setsockopt(zmq.LINGER, 0)
xsub.setsockopt(zmq.LINGER, 0)
xpub.bind("tcp://*:5558")
xsub.bind("tcp://*:5557")

print("Proxy Pub/Sub ativo: XSUB :5557, XPUB :5558", flush=True)
zmq.proxy(xpub, xsub)
