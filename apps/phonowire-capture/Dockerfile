# Official bookworm bootstrap pinned to a linux/amd64 manifest.
FROM rust:1-bookworm@sha256:4e4a7e7939c17991ab35f2b8c2e67593980f771d28f6b1254b1850f860fd0c7f AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY apps/phonowire-capture ./apps/phonowire-capture
RUN rustup toolchain install 1.98.1 --profile minimal \
    && cargo +1.98.1 build --locked --release -p phonowire-capture

FROM debian:bookworm-slim
COPY --from=build /src/target/release/phonowire-capture /usr/local/bin/phonowire-capture
COPY LICENSE NOTICE /usr/share/doc/phonowire-capture/
ENTRYPOINT ["/usr/local/bin/phonowire-capture"]
