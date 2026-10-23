#!/bin/sh
# Generates the Go code of the contracts into ./gen (not committed).
# Needs protoc-gen-go and protoc-gen-go-grpc on PATH and either protoc or
# python3 with grpcio-tools. The Dockerfile runs this script; locally:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
#   pip install grpcio-tools==1.66.2
set -eu
cd "$(dirname "$0")/.."
if command -v protoc >/dev/null 2>&1; then PROTOC="protoc"; else PROTOC="python3 -m grpc_tools.protoc"; fi
rm -rf gen
$PROTOC -I contracts/proto \
  --go_out=. --go_opt=module=pad-lab2 \
  --go-grpc_out=. --go-grpc_opt=module=pad-lab2 \
  contracts/proto/broker/v1/broker.proto \
  contracts/proto/orders/v1/orders.proto \
  contracts/proto/payment/v1/payment.proto
echo "generated: $(find gen -name '*.go' | wc -l) files in ./gen"
