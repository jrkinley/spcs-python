#!/bin/bash
set -e

# Configuration - update these values
SNOWFLAKE_ACCOUNT="your_account"
SNOWFLAKE_ORG="your_org"
IMAGE_REPO="spcs_db.spcs_schema.image_repo"
IMAGE_NAME="imf_datamapper_api"
IMAGE_TAG="latest"

# Full image path
REPO_URL="${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}.registry.snowflakecomputing.com/${IMAGE_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "Building Docker image..."
docker build --platform linux/amd64 -t ${IMAGE_NAME}:${IMAGE_TAG} .

echo "Tagging image for Snowflake registry..."
docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${REPO_URL}

echo "Image built and tagged: ${REPO_URL}"
echo ""
echo "Next steps:"
echo "1. Login to Snowflake registry:"
echo "   docker login ${SNOWFLAKE_ORG}-${SNOWFLAKE_ACCOUNT}.registry.snowflakecomputing.com"
echo ""
echo "2. Push the image:"
echo "   docker push ${REPO_URL}"
