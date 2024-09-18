ARG RUST_VERSION=1.80.0
ARG APP_NAME=nos_followers

################################################################################
# Create a stage for building the application.

FROM rust:${RUST_VERSION}-alpine AS build
ARG APP_NAME
WORKDIR /app

# Install host build dependencies.
RUN apk update \
    && apk add --no-cache --purge pkgconfig clang lld musl-dev git openssl-dev openssl-libs-static libc-dev

# Create and set the working directory
WORKDIR /app

# Leverage a cache mount to /usr/local/cargo/registry/ for downloaded dependencies,
# a cache mount to /usr/local/cargo/git/db for git dependencies, and a cache mount
# to /app/target/ for compiled dependencies which will speed up subsequent builds.
RUN --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --locked --release --bin $APP_NAME --bin pagerank && \
    cp ./target/release/$APP_NAME /bin/server && \
    cp ./target/release/pagerank /bin/pagerank

################################################################################
# Create a new stage for running the application that contains the minimal
# runtime dependencies for the application. This often uses a different base
# image from the build stage where the necessary files are copied from the build
# stage.
FROM alpine:3.20 AS final

WORKDIR /app

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/go/dockerfile-user-best-practices/
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser
USER appuser

# Copy the executables from the "build" stage.
COPY --from=build /bin/server /bin/
COPY --from=build /bin/pagerank /bin/

# Copy any necessary configuration or migration files
COPY ./migrations /app/migrations
COPY ./config/settings.yml /app/config/settings.yml

# Expose the port that the main server listens on.
EXPOSE 3000

# By default, run the main server binary. You can override this in `docker run`
ENTRYPOINT ["/bin/server"]
