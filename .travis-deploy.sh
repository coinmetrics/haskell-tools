#!/bin/bash

set -ex

# docker image
cp Dockerfile bin/ && docker build -t ${DOCKER_REPO}:latest bin
docker login -u="${DOCKER_USER}" -p="${DOCKER_PASSWORD}" ${DOCKER_REGISTRY}
docker push ${DOCKER_REPO}:latest
