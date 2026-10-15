# Build stage
FROM rust:1-alpine AS build
ARG MINISTREAM_VERSION=v1.0.0
# Limit parallel rustc jobs to keep peak memory low (release profile uses LTO);
# override with --build-arg CARGO_BUILD_JOBS=<n> on hosts with more RAM
ARG CARGO_BUILD_JOBS=2
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY docs ./docs
ENV MINISTREAM_VERSION=${MINISTREAM_VERSION}
RUN cargo build --release --locked -j "${CARGO_BUILD_JOBS}" \
    && cp target/release/ministream target/release/generatepasswords /usr/local/bin/

# Runtime stage
FROM alpine:3 AS runtime
RUN apk --no-cache add curl
WORKDIR /app
COPY --from=build /usr/local/bin/ministream /usr/local/bin/generatepasswords ./
COPY config-templates/docker/config /app/config
COPY config-templates/docker/certs /app/certs
COPY config-templates/docker/data /app/data
EXPOSE 8080/tcp
HEALTHCHECK CMD curl -f http://localhost:8080/api/v1/utils/ping || exit 1
CMD ["/app/ministream", "-config", "/app/config/config.yaml"]
