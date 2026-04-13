#!/bin/bash
set -e

# Write certificate to .klab if provided via environment variable
if [ -n "$KLAB_CERTIFICATE" ]; then
  echo "Writing certificate to /home/nifi/.klab/klab.cert..."
  echo "$KLAB_CERTIFICATE" | base64 -d > /home/nifi/.klab/klab.cert
fi

# Start NiFi
echo "Starting NiFi..."
exec /opt/nifi/scripts/start.sh
