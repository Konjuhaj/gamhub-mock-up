FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/brazemock ./cmd/brazemock

FROM alpine:3.22
RUN adduser -D -u 10001 app && mkdir /data && chown app /data
COPY --from=build /out/brazemock /usr/local/bin/brazemock
USER app 
EXPOSE 8081
ENTRYPOINT ["brazemock"]

