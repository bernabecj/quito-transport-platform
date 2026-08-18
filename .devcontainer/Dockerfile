# syntax=docker/dockerfile:1

# Dev container image for the Quito Transport Platform.
#
# What used to be devcontainer "features" (poetry, aws-cli, terraform,
# github-cli) is baked in here instead, so the toolchain lands in cached image
# layers rather than re-running install scripts on every container create.
#
# Python dependencies are installed at BUILD time into /opt/venv — deliberately
# outside the bind-mounted workspace. The workspace mount is the slow path on
# Windows hosts, and this project's dependency set is ~27k files / ~1 GB, so
# keeping the virtualenv off that mount is what makes container start fast.

FROM mcr.microsoft.com/devcontainers/python:3.11-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# --- System tooling: terraform, gh, aws-cli v2 -------------------------------
# One layer, one apt cycle. tflint/terragrunt are omitted: the terraform
# feature installed them by default but nothing in this repo references them.
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    apt-get update; \
    apt-get install -y --no-install-recommends gnupg unzip; \
    \
    curl -fsSL https://apt.releases.hashicorp.com/gpg \
      | gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg; \
    echo "deb [arch=${arch} signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com bookworm main" \
      > /etc/apt/sources.list.d/hashicorp.list; \
    \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
      -o /usr/share/keyrings/githubcli-archive-keyring.gpg; \
    chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg; \
    echo "deb [arch=${arch} signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
      > /etc/apt/sources.list.d/github-cli.list; \
    \
    apt-get update; \
    apt-get install -y --no-install-recommends terraform gh; \
    \
    case "${arch}" in \
      amd64) awscli_arch=x86_64 ;; \
      arm64) awscli_arch=aarch64 ;; \
      *) echo "unsupported architecture: ${arch}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${awscli_arch}.zip" -o /tmp/awscliv2.zip; \
    unzip -q /tmp/awscliv2.zip -d /tmp; \
    /tmp/aws/install; \
    rm -rf /tmp/aws /tmp/awscliv2.zip; \
    \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# --- Poetry ------------------------------------------------------------------
# Pinned to the version that generated poetry.lock (lock-version 2.1), so the
# lock is consumed rather than silently regenerated.
ENV POETRY_VERSION=2.4.1 \
    POETRY_HOME=/opt/poetry \
    POETRY_NO_INTERACTION=1
RUN python3 -m venv "$POETRY_HOME" \
 && "$POETRY_HOME/bin/pip" install --no-cache-dir "poetry==${POETRY_VERSION}" \
 && ln -s "$POETRY_HOME/bin/poetry" /usr/local/bin/poetry

# --- Project dependencies ----------------------------------------------------
# VIRTUAL_ENV + POETRY_VIRTUALENVS_CREATE=false makes poetry install into
# /opt/venv, both here and at runtime. That also neutralises the repo's
# poetry.toml (`in-project = true`), which would otherwise recreate the 1 GB
# .venv inside the workspace mount the next time anyone runs `poetry add`.
ENV VIRTUAL_ENV=/opt/venv \
    POETRY_VIRTUALENVS_CREATE=false \
    PATH=/opt/venv/bin:$PATH

# Add `--with airflow` here if you ever want apache-airflow importable for local
# DAG unit tests. It is omitted by default: Airflow runs via the Astro compose
# stack, and the group pulls in a large, tightly-constrained dependency tree.
ARG POETRY_GROUPS="--with dev"

WORKDIR /tmp/deps
COPY pyproject.toml poetry.lock ./
RUN set -eux; \
    python3 -m venv "$VIRTUAL_ENV"; \
    poetry install --no-root ${POETRY_GROUPS}; \
    chown -R vscode:vscode "$VIRTUAL_ENV"; \
    rm -rf /root/.cache/pypoetry /tmp/deps

# The ENV PATH above only survives in non-login shells: Debian's /etc/profile
# rebuilds PATH from scratch, which drops /opt/venv/bin for login shells — and
# VS Code's integrated terminal opens login shells, where `python` would then
# resolve to the base image's interpreter instead of the project venv.
RUN printf '%s\n' 'export PATH="/opt/venv/bin:$PATH"' > /etc/profile.d/10-project-venv.sh \
 && chmod 0644 /etc/profile.d/10-project-venv.sh

# --- Astro CLI ---------------------------------------------------------------
# Drives the local Airflow stack (`astro dev start`) in airflow/. This was
# previously installed by hand inside the container, so it disappeared on every
# rebuild. Deliberately placed after the dependency layer above: that layer is
# the expensive one, and keeping it earlier means bumping ASTRO_CLI_VERSION does
# not force a full reinstall of the Python environment.
ARG ASTRO_CLI_VERSION=""
RUN set -eux; \
    if [ -n "${ASTRO_CLI_VERSION}" ]; then \
      curl -sSL https://install.astronomer.io | bash -s -- "v${ASTRO_CLI_VERSION}"; \
    else \
      curl -sSL https://install.astronomer.io | bash -s; \
    fi; \
    astro version

# --- Host path translation for the Astro stack -------------------------------
# See the script header. Kept last so editing it does not invalidate the
# dependency layer above.
COPY .devcontainer/host-paths.sh /etc/profile.d/20-astro-host-paths.sh
RUN chmod 0644 /etc/profile.d/20-astro-host-paths.sh

WORKDIR /workspaces
