FROM golang:1.23.4-alpine3.20 AS builder
WORKDIR /go/src/conditions-number
COPY . .
RUN \
    apk add protoc protobuf-dev make git && \
    make build

FROM scratch
COPY --from=builder /go/src/conditions-number/conditions-number /bin/conditions-number
ENTRYPOINT ["/bin/conditions-number"]
