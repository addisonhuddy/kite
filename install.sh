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

if command -v curl >/dev/null 2>&1; then
    if ! curl -fsSL "$url" -o "$tmp/kite"; then
        echo "kite: failed to download $url" >&2
        exit 1
    fi
elif command -v wget >/dev/null 2>&1; then
    if ! wget -q "$url" -O "$tmp/kite"; then
        echo "kite: failed to download $url" >&2
        exit 1
    fi
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
