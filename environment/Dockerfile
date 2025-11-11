# hash:sha256:9a841403a596d346215a2475deaf67a62333737e659182382607c4672e6f1190
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-askpass /git-askpass

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        jq \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=secret,id=git-access-token \
    GIT_ACCESS_TOKEN="$(cat /run/secrets/git-access-token)" \
    pip3 install -U --no-cache-dir \
    s3fs \
    gql[requests] \
    -e git+https://github.com/AllenNeuralDynamics/nmcp-scripting@585c81574c282800f2781df103471b37cc2399ce#egg=nmcpscripting

COPY postInstall /
RUN --mount=type=secret,id=secrets . /run/secrets/secrets \
    && /postInstall
