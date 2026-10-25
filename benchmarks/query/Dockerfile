FROM golang:1.26.10-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY internal/ internal/
COPY benchmarks/query/cmd/ benchmarks/query/cmd/
COPY benchmarks/query/internal/ benchmarks/query/internal/
RUN CGO_ENABLED=0 go build -trimpath -o /out/bench-server ./benchmarks/query/cmd/server && \
    CGO_ENABLED=0 go build -trimpath -o /out/bench-runner ./benchmarks/query/cmd/run

FROM scratch AS server
COPY --from=build /out/bench-server /bench-server
COPY --from=build /out/bench-runner /bench-runner
ENTRYPOINT ["/bench-server"]

FROM scratch AS runner
COPY --from=build /out/bench-runner /bench-runner
ENTRYPOINT ["/bench-runner"]

# Optional diagnostic image; no profiling endpoints or hooks in the engine.
FROM build AS profile-build
RUN CGO_ENABLED=0 go test -c -o /out/query-bench.test ./benchmarks/query/internal/bench

FROM scratch AS profile
COPY --from=profile-build /out/query-bench.test /query-bench.test
ENTRYPOINT ["/query-bench.test"]
