# Build from the repository root:
# docker build -f cmd/server/Dockerfile -t weave-server .
FROM golang:1.26.10-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY pkg ./pkg
COPY cmd/server ./cmd/server
RUN CGO_ENABLED=0 go build -trimpath -o /weave-server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /weave-server /weave-server
EXPOSE 8080
ENTRYPOINT ["/weave-server"]
