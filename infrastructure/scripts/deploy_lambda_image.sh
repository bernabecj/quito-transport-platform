#!/usr/bin/env bash
# Builds ingestion/Dockerfile and pushes it to the quito-transport-ingestion ECR repo.
# Run from the repo root. Requires the ECR repo to already exist —
# `terraform apply -target=aws_ecr_repository.ingestion` first.
#
# Terraform can't build Docker images itself, so this is a separate manual step:
#   1. terraform apply -target=aws_ecr_repository.ingestion
#   2. infrastructure/scripts/deploy_lambda_image.sh
#   3. terraform apply -target=aws_lambda_function.gtfs_ingestion \
#        -target=aws_lambda_function.weather_ingestion
set -euo pipefail

REPO_NAME="quito-transport-ingestion"
IMAGE_TAG="${IMAGE_TAG:-latest}"
REGION="$(aws configure get region || echo sa-east-1)"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
REPO_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

if ! aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "ECR repo '$REPO_NAME' not found — run 'terraform apply -target=aws_ecr_repository.ingestion' first." >&2
  exit 1
fi

echo "Logging in to ECR ..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

# oci-mediatypes=false is required: Lambda rejects OCI-format manifests/config,
# and Docker Desktop's containerd image store produces OCI by default otherwise.
echo "Building and pushing ${REPO_URI}:${IMAGE_TAG} from ingestion/Dockerfile ..."
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --sbom=false \
  --output "type=image,name=${REPO_URI}:${IMAGE_TAG},push=true,oci-mediatypes=false" \
  -f ingestion/Dockerfile .

echo "Done. Image URI: ${REPO_URI}:${IMAGE_TAG}"
