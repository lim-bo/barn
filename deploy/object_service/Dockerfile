FROM golang:1.24.4-alpine as build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /main cmd/object_service/main.go

FROM alpine:latest
COPY --from=build /main /bin/main
COPY --from=build /app/config/cfg.yaml /config/

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.2.1/wait /wait
RUN chmod +x /wait
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

CMD /wait && /bin/main