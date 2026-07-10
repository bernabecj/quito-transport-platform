#!/usr/bin/env bash
# Creates/updates the .venv-airflow environment used for Airflow DAG/operator
# tests (see README > Running Locally > Airflow environment).
#
# apache-airflow requires Python <3.13, while the rest of this project targets
# 3.13+, so it lives in its own venv rather than the main poetry-managed one.
# Safe to re-run: reuses the poetry-managed 3.11 interpreter and venv if present.
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON_VERSION="3.11"
VENV_DIR=".venv-airflow"
DATA_DIR="$(poetry config data-dir)"
PYTHON_BIN="$DATA_DIR/python/cpython@${PYTHON_VERSION}"*/bin/python${PYTHON_VERSION}

if ! compgen -G "$PYTHON_BIN" > /dev/null; then
  echo "Installing Python ${PYTHON_VERSION} via Poetry..."
  poetry python install "$PYTHON_VERSION"
fi
PYTHON_BIN=$(compgen -G "$PYTHON_BIN" | head -n1)

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating $VENV_DIR with $PYTHON_BIN..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
poetry install --with dev --with airflow

echo
echo "Done. Activate with: source $VENV_DIR/bin/activate"
