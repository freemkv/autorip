# Build requires libfreemkv checked out at ../libfreemkv relative to this repo.
# The release workflow and docker-compose handle this automatically.

FROM rust:1.82-slim AS builder

WORKDIR /build/libfreemkv
COPY ../libfreemkv .

WORKDIR /build/autorip
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    libssl3 cron curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/autorip/target/release/autorip /usr/local/bin/autorip
COPY entrypoint.sh /entrypoint.sh
COPY udev-trigger.sh /usr/local/bin/udev-trigger.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/udev-trigger.sh

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
