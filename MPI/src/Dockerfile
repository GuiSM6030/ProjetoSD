FROM python:3.13.7-alpine3.21
ENV OMPI_ALLOW_RUN_AS_ROOT=1
ENV OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1

RUN apk add build-base openssh openmpi openmpi-dev

WORKDIR /app

COPY ./*.c .

CMD ["sh"]
