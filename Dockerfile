FROM golang:1.23-bullseye AS build

WORKDIR /go/src/f3

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /go/bin/f3 ./cmd/f3

FROM gcr.io/distroless/cc
COPY --from=build /go/bin/f3 /usr/bin/

ENTRYPOINT ["/usr/bin/f3"]
