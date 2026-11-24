#!/bin/bash
set -e

# Set by the Dockerfile; keep a default so the script still works if it is run
# outside the image.
CA_BUNDLE=${CA_BUNDLE:-/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem}

if [ -n "${CA_URL}" ] && [ ! -f "/tmp/.imported" ]; then
    # Since update-ca-trust doesn't work as a non-root user, let's just append to the bundle directly
    curl -k --silent --show-error "${CA_URL}" >> "${CA_BUNDLE}"
    # Create a file so we know not to import it again if the container is restarted
    touch /tmp/.imported
fi
