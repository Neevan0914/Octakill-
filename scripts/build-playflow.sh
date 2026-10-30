#!/usr/bin/env bash
# Builds the PlayFlow upload: build/octakill-server.zip containing Server.x86_64.
set -euo pipefail
cd "$(dirname "$0")/.."

rm -rf build
mkdir -p build/server
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags="-s -w" -o build/server/Server.x86_64 .
chmod +x build/server/Server.x86_64
(cd build/server && zip -q -X ../octakill-server.zip Server.x86_64)
echo "Built build/octakill-server.zip"
