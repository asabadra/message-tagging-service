#!/usr/bin/bash

set -euo pipefail

dependencies=(
    python3-fedora-messaging
    python3-flask
    python3-gunicorn
    python3-kafka
    # kafka-python negotiates no compression of its own: it must be able to
    # decompress whatever codec the producer chose. Without the codec library
    # the batch raises UnsupportedCodecError from the consumer iterator, which
    # is outside the per-message error handling in kafka_backend(), so the
    # process exits and restarts onto the same batch forever.
    python3-snappy
    python3-koji
    python3-pip
    python3-prometheus_client
    python3-pyyaml
    python3-requests
)

dnf install -y \
    --setopt=deltarpm=0 \
    --setopt=install_weak_deps=false \
    --setopt=tsflags=nodocs \
    ${dependencies[@]}

dnf clean all
