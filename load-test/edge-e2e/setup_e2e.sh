#!/usr/bin/env bash
# One-time setup on the Linux host (multipass VM or CloudLab node).
# Downloads the k3s and etcd binaries (arch-aware), checks redis-server, and
# reminds about the retcd binary (cross-compiled on the Mac, scp'd in).
set -eu
BIN=~/bench/bin
mkdir -p "$BIN"

K3S_VERSION=${K3S_VERSION:-v1.32.5+k3s1}
ETCD_VERSION=${ETCD_VERSION:-v3.5.24}

ARCH=$(uname -m)
case "$ARCH" in
  aarch64|arm64) K3S_SUFFIX="-arm64"; ETCD_ARCH="arm64" ;;
  x86_64)        K3S_SUFFIX="";       ETCD_ARCH="amd64" ;;
  *) echo "unsupported arch $ARCH"; exit 1 ;;
esac

if [ ! -x "$BIN/k3s" ]; then
  echo "fetching k3s $K3S_VERSION ($ARCH)..."
  curl -fsSL -o "$BIN/k3s" \
    "https://github.com/k3s-io/k3s/releases/download/${K3S_VERSION//+/%2B}/k3s${K3S_SUFFIX}"
  chmod +x "$BIN/k3s"
fi
"$BIN/k3s" --version | head -1

if [ ! -x "$BIN/etcd" ]; then
  echo "fetching etcd $ETCD_VERSION ($ETCD_ARCH)..."
  curl -fsSL "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-${ETCD_ARCH}.tar.gz" \
    | tar -xz -C "$BIN" --strip-components=1 \
      "etcd-${ETCD_VERSION}-linux-${ETCD_ARCH}/etcd"
fi
"$BIN/etcd" --version | head -1

command -v redis-server >/dev/null || { echo "redis-server missing: sudo apt-get install -y redis-server"; exit 1; }
redis-server --version

if [ ! -x "$BIN/retcd" ]; then
  echo "MISSING: $BIN/retcd"
  echo "  on the Mac: cd ~/me/fyp/retcd && GOOS=linux GOARCH=${ETCD_ARCH} CGO_ENABLED=0 go build -trimpath -o /tmp/retcd-linux ."
  echo "  then:       scp /tmp/retcd-linux <host>:~/bench/bin/retcd && chmod +x"
  exit 1
fi
echo "OK: all binaries present in $BIN"
