# This tells docker to use the Rust official image
FROM rust:1.67.1 as builder
# Set working directory
WORKDIR /usr/src/hello
# Copy the files from local machine to the Docker image
COPY . .
# Build executable
RUN cargo install --path .

# Copy exe to slim image to shrink the image
FROM debian:buster-slim
COPY --from=builder /usr/local/cargo/bin/cicd-rust /usr/local/bin/cicd-rust

# Run the binary
ENTRYPOINT ["cicd-rust"]