FROM golang:1.21-bullseye AS build

WORKDIR /go/src/f3

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/f3 ./cmd/f3

FROM gcr.io/distroless/static-debian11
COPY --from=build /go/bin/f3 /usr/bin/

ENTRYPOINT ["/usr/bin/f3"]
