#!/bin/sh
set -eu

VERSION=${KITE_VERSION:-v0.1.0}
REPO=addisonhuddy/kite
OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
    Linux) os=linux ;;
    Darwin) os=macos ;;
    *) echo "kite: unsupported operating system: $OS" >&2; exit 1 ;;
esac

case "$ARCH" in
    x86_64|amd64) arch=x86_64 ;;
    arm64|aarch64) arch=aarch64 ;;
    *) echo "kite: unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

asset=kite-${os}-${arch}
url=https://github.com/$REPO/releases/download/$VERSION/$asset
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

download_failed() {
    echo "kite: failed to download $url" >&2
    echo "kite: check that release $VERSION exists at https://github.com/$REPO/releases" >&2
    echo "kite: pick another with KITE_VERSION=vX.Y.Z, or build from source: zig build -Doptimize=ReleaseSmall" >&2
    exit 1
}

if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$tmp/kite" || download_failed
elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$tmp/kite" || download_failed
else
    echo "kite: curl or wget is required to install kite" >&2
    exit 1
fi

chmod +x "$tmp/kite"
if [ -n "${KITE_INSTALL_DIR:-}" ]; then
    "$tmp/kite" --install --dir "$KITE_INSTALL_DIR" "$@"
else
    "$tmp/kite" --install "$@"
fi
