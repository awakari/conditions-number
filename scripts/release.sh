#!/bin/bash

export SLUG=ghcr.io/awakari/conditions-number
export VERSION=$(git describe --tags --abbrev=0 | cut -c 2-)
echo "Releasing version: $VERSION"
docker tag awakari/conditions-number "${SLUG}":"${VERSION}"
docker push "${SLUG}":"${VERSION}"
