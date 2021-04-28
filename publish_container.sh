#!/bin/sh
IMAGE_NAME="bo-cdc-consumer-stg"
IMAGE_URL="$REPOSITORY_URL/$IMAGE_NAME"

echo "Retrieving aws authentication token and authenticating Docker client to registry...."
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $REPOSITORY_URL

echo "Building image...."
docker build -t $IMAGE_NAME .

echo "Tagging image...."
docker tag "$IMAGE_NAME:latest" "$IMAGE_URL:latest"

echo "Pushing image to repository...."
docker push "$IMAGE_URL:latest"