# TODO: This build requires libfreemkv as a workspace dependency (path = "../libfreemkv").
# For a real build, either use a workspace-level Dockerfile or supply a pre-built binary.

FROM rust:1.82-slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    libssl3 eject cron \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/autorip /usr/local/bin/autorip
COPY entrypoint.sh /entrypoint.sh
COPY udev-trigger.sh /usr/local/bin/udev-trigger.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/udev-trigger.sh

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
