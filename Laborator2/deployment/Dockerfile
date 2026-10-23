# syntax=docker/dockerfile:1
# One Dockerfile for every service and for the tests.
# The Go code of the contracts (./gen) is generated here, it is not committed.

# --- protoc plugins (pinned versions)
FROM golang:1.24-bookworm AS plugins
RUN CGO_ENABLED=0 go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1 \
 && CGO_ENABLED=0 go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

# --- code generation from contracts/proto (protoc 27.2 from grpcio-tools)
FROM python:3.12-slim AS gen
RUN pip install --no-cache-dir grpcio-tools==1.66.2
COPY --from=plugins /go/bin/ /usr/local/bin/
WORKDIR /src
COPY contracts/proto contracts/proto
COPY scripts/gen.sh scripts/gen.sh
# tolerate CRLF line endings from a Windows checkout
RUN sed -i "s/\r$//" scripts/gen.sh && sh scripts/gen.sh

# --- optional: export ./gen to the host for the IDE
#     docker build -f deployment/Dockerfile --target gen-export --output type=local,dest=gen .
FROM scratch AS gen-export
COPY --from=gen /src/gen /

# --- sources + generated code + Go toolchain (used by the tests service)
FROM golang:1.24-bookworm AS dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=gen /src/gen ./gen
CMD ["go", "test", "-race", "-count=1", "./..."]

# --- one service binary, selected with --build-arg SERVICE=<path>
FROM dev AS build
ARG SERVICE
RUN CGO_ENABLED=0 go build -o /out/app ./${SERVICE}

FROM alpine:3.20
RUN adduser -D -H app && mkdir /data && chown app /data
COPY --from=build /out/app /app
USER app
ENTRYPOINT ["/app"]
