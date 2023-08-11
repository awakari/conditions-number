#!/bin/bash

export SLUG=ghcr.io/awakari/conditions-number
export VERSION=latest
docker tag awakari/conditions-number "${SLUG}":"${VERSION}"
docker push "${SLUG}":"${VERSION}"
