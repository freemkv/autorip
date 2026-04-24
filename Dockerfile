FROM rust:1.86-slim AS builder

WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    libssl3 cron curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/autorip /usr/local/bin/autorip
COPY entrypoint.sh /entrypoint.sh
COPY udev-trigger.sh /usr/local/bin/udev-trigger.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/udev-trigger.sh

EXPOSE 8080

# Healthcheck — fails when /api/state can't be reached (web thread dead,
# port binding failure, process hung). Combined with the compose-side
# `restart: unless-stopped`, Docker auto-recovers a wedged container
# instead of leaving a dead-UI zombie running until somebody notices.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail --silent --max-time 4 http://127.0.0.1:8080/api/state > /dev/null || exit 1

ENTRYPOINT ["/entrypoint.sh"]
