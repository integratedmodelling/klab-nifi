#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f nifi_conf/nifi.properties ]]; then
  echo 'Existing nifi_conf/nifi.properties found; leaving configuration unchanged.'
  exit 0
fi
if [[ -d nifi_conf ]] && [[ -n "$(ls -A nifi_conf 2>/dev/null)" ]]; then
  echo 'ERROR: nifi_conf is nonempty but nifi.properties is absent. Inspect/backup it; refusing to overwrite.' >&2
  exit 1
fi
mkdir -p nifi_conf
cid=$(docker create --entrypoint /bin/true apache/nifi:2.4.0)
trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
docker cp "$cid:/opt/nifi/nifi-current/conf/." ./nifi_conf/
if [[ ! -f nifi_conf/nifi.properties ]]; then
  echo 'ERROR: initialization did not create nifi.properties' >&2
  exit 1
fi
# Files copied from image may be owned by a different UID; use the image's UID/GID.
owner=$(docker run --rm --entrypoint id apache/nifi:2.4.0 -u)
group=$(docker run --rm --entrypoint id apache/nifi:2.4.0 -g)
if command -v sudo >/dev/null 2>&1; then
  sudo chown -R "$owner:$group" ./nifi_conf
else
  echo "NOTE: ensure ./nifi_conf is writable by container UID:GID $owner:$group"
fi
echo 'NiFi configuration initialized. This contains local security material; do not commit nifi_conf.'
