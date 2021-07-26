FROM golang:1.16 AS builder
ENV GOPROXY https://goproxy.io
ENV CGO_ENABLED 0
WORKDIR /go/src/app
ADD . .
RUN go build -mod vendor -o /redmemd

FROM alpine:3.14
COPY --from=builder /redmemd /redmemd
CMD ["/redmemd"]