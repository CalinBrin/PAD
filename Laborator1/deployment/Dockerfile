# One Dockerfile for all four services. Select the service with --build-arg SERVICE=<path>.
FROM golang:1.22-alpine AS build
ARG SERVICE
WORKDIR /src
COPY go.mod ./
COPY src ./src
RUN CGO_ENABLED=0 go build -o /out/app ./${SERVICE}

FROM alpine:3.20
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
