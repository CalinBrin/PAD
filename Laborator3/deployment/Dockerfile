# syntax=docker/dockerfile:1
# One Dockerfile for every service, the tests and the tools (loadgen, client).

# --- sources + Go toolchain (tests and tools)
FROM golang:1.24-bookworm AS dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build ./...
CMD ["go", "test", "-race", "-count=1", "./..."]

# --- one service binary, selected with --build-arg SERVICE=<path>
FROM dev AS build
ARG SERVICE
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./${SERVICE}

FROM alpine:3.20
RUN adduser -D -H app && mkdir /data && chown app /data
COPY --from=build /out/app /app
USER app
ENTRYPOINT ["/app"]
