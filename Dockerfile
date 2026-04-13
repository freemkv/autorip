# Docker build context must include both autorip/ and libfreemkv/ directories.
# The release workflow handles this by checking out both repos side-by-side.
# For local builds: docker build -t autorip -f autorip/Dockerfile ..

FROM rust:1.86-slim AS builder

WORKDIR /build
COPY libfreemkv/ libfreemkv/
COPY autorip/ autorip/

WORKDIR /build/autorip
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    libssl3 cron curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/autorip/target/release/autorip /usr/local/bin/autorip
COPY autorip/entrypoint.sh /entrypoint.sh
COPY autorip/udev-trigger.sh /usr/local/bin/udev-trigger.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/udev-trigger.sh

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
