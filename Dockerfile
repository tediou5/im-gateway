# build a Rust using rust-musl-builder image,
# and deploy it with a tiny Alpine Linux container.

# You can override this `--build-arg BASE_IMAGE=...`
# to use different version of Rust or OpenSSL.
ARG BASE_IMAGE=ekidd/rust-musl-builder:nightly-2021-12-23

# Our first FROM statement declares the build environment.
FROM ${BASE_IMAGE} AS builder

# Add our source code.
# ADD --chown=rust:rust . ./
ADD --chown=rust:rust ./link ./link
ADD --chown=rust:rust ./proxy ./proxy
ADD --chown=rust:rust ./local-sync ./local-sync
ADD --chown=rust:rust ./rskafka ./rskafka
ADD --chown=rust:rust ./sdk-link ./sdk-link
ADD --chown=rust:rust ./dicts ./dicts
ADD --chown=rust:rust ./Cargo.toml ./
ADD --chown=rust:rust ./Cargo.lock ./

RUN rustup update

# Build our application.
RUN cargo build --release

# # Now, we need to build our _real_ Docker container, copy to dest image.
# FROM alpine:latest
# RUN apk --no-cache add ca-certificates
# WORKDIR /hmessage-core-service
# COPY --from=builder \
#     /home/rust/src/target/x86_64-unknown-linux-musl/release/hmessage-core-service \
#     ./
# ADD ./tls ./tls
# EXPOSE 8080 20001
# CMD ["./hmessage-core-service", "0.0.0.0:8080"]