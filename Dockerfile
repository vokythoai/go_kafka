# Build Stage
# MAINTAINER Thoai Vo <vokythoai@gmail.com>
FROM golang:1.13.0-alpine

ENV APP kafka_consumer
ENV TZ Asia/Ho_Chi_Minh
ENV CGO_ENABLED=0
ENV APP kafka_consumer

COPY go.mod /$APP/
COPY go.sum /$APP/
COPY . /$APP/
WORKDIR /$APP

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /go/bin/$APP main.go

RUN apk add git; \
    apk add build-base; \
    apk add --no-cache tzdata;

COPY ./docker_entrypoint.sh /docker_entrypoint.sh
COPY ./.version /.version
COPY ./ssl /ssl

EXPOSE 8080
ENTRYPOINT ["sh","/docker_entrypoint.sh"]
