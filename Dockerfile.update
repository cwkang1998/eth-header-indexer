# Multistage build
FROM rust:1.81 AS builder

WORKDIR /usr/app
COPY . .
RUN cargo build --release

# Use a distroless docker image which should further reduce the size.
# This one is setup specifically for rust.
FROM gcr.io/distroless/cc-debian12
WORKDIR /usr/app
COPY --from=builder /usr/app/target/release/fossil_headers_db .

EXPOSE 8080
# defaults to update mode
CMD ["/usr/app/fossil_headers_db", "update"]
