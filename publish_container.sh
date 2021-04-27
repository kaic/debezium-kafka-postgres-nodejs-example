#!/bin/sh
echo "Retrieving aws authentication token and authenticating Docker client to registry...."
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 046386798965.dkr.ecr.us-east-1.amazonaws.com

echo "Building image...."
docker build -t b-o-test-worker .

echo "Tagging image...."
docker tag b-o-test-worker:latest 046386798965.dkr.ecr.us-east-1.amazonaws.com/b-o-test-worker:latest

echo "Pushing image to registry...."
docker push 046386798965.dkr.ecr.us-east-1.amazonaws.com/b-o-test-worker:latest