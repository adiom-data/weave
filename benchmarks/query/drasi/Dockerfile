ARG DRASI_IMAGE=ghcr.io/drasi-project/drasi-server@sha256:4d2d0f418f31152ef562622ac82a5fe9efb62bfd21826370709a4f04c67f36a5
FROM ${DRASI_IMAGE}
ARG TARGETARCH
USER root
COPY benchmarks/query/drasi/server.yaml /app/config/server.yaml
COPY benchmarks/query/drasi/plugins-${TARGETARCH}.lock /app/plugins/plugins.lock
RUN chown -R drasi:drasi /app/config /app/plugins
USER drasi
RUN drasi-server plugin install --from-config --locked --config /app/config/server.yaml --plugins-dir /app/plugins
ENTRYPOINT ["drasi-server", "--config", "/app/config/server.yaml", "--plugins-dir", "/app/plugins", "--disable-ui"]
