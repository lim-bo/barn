FROM golang:1.24.4-alpine as build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /main cmd/gateway/main.go

FROM alpine:latest
COPY --from=build /main /bin/main
COPY --from=build /app/config/cfg.yaml /config/
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

CMD [ "/bin/main" ]