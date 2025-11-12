# hash:sha256:b118bdea317b48ce2bb16739101c6ebf45fc07eae3905c9d3cde4809810ed14b
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:24.5.0-0-python3.12.4-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-askpass /git-askpass

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        jq \
        wget \
    && rm -rf /var/lib/apt/lists/*

COPY postInstall /
RUN --mount=type=secret,id=secrets . /run/secrets/secrets \
    && /postInstall
