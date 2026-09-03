#!/usr/bin/env bash
# Update a deployed osmpbudynkiv2 server to the newest GitHub release.
#
# Run on the server, as root:
#   sudo bash scripts/download_release_and_update.sh
#   sudo bash scripts/download_release_and_update.sh --no-start
#
# Replaces the binary, web/ and this script itself. config.toml, .env and the
# systemd unit are deployment state rather than release artifacts -- they are
# never touched, and the example.* templates in the tarball are discarded with
# the temp dir rather than extracted over them. Paths and names below can be
# overridden from the environment for a deployment laid out differently.

set -euo pipefail

REPO=${REPO:-openstreetmap-polska/osmpbudynkiv2}
APP_DIR=${APP_DIR:-/opt/osmpbudynkiv2}
OWNER=${OWNER:-osmpbudynkiv2:osmpbudynkiv2}
SERVICE=${SERVICE:-osmpbudynkiv2}

# --no-start updates the files but leaves the service down. Needed whenever the
# database is not ready to be served: a first deploy before `init` has run, or
# an upgrade you want to follow with an offline `import`/`compare`/`queue`
# command, none of which may run while the server holds the database.
no_start=0
while [ $# -gt 0 ]; do
  case "$1" in
    -n|--no-start) no_start=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done

[ "$(id -u)" -eq 0 ] || { echo "run as root" >&2; exit 1; }

# `releases/latest` skips prereleases. This takes the first .tar.gz asset,
# which is unambiguous while the release workflow publishes exactly one.
url=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
      | grep -o '"browser_download_url": *"[^"]*\.tar\.gz"' | head -1 | cut -d'"' -f4)
[ -n "$url" ] || { echo "no release asset found" >&2; exit 1; }
echo "Downloading ${url##*/}"

# Staged inside APP_DIR so the install below is a same-filesystem operation.
tmp=$(mktemp -d "$APP_DIR/.update.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
curl -fSL --progress-bar -o "$tmp/rel.tar.gz" "$url"
tar -xzf "$tmp/rel.tar.gz" -C "$tmp"
src=$(find "$tmp" -mindepth 1 -maxdepth 1 -type d | head -1)
[ -x "$src/osmpbudynkiv2" ] && [ -d "$src/web" ] || { echo "unexpected archive layout" >&2; exit 1; }

# Exercise the new binary before overwriting the old one. glibc and libstdc++
# are backward- but not forward-compatible, so a release built on a newer
# distro than this server dies at exec ("version `GLIBC_2.xx' not found").
# Checked here because once `install` has run there is nothing to roll back to.
"$src/osmpbudynkiv2" --version || { echo "new binary will not run here -- aborting" >&2; exit 1; }

# DuckDB has no multi-writer support, so never swap under a running server.
# Restarted only if it was running: leaves a deliberately-stopped service
# (mid-import, say) stopped.
active=0; systemctl is-active --quiet "$SERVICE" && active=1 || true
[ "$active" -eq 1 ] && systemctl stop "$SERVICE" || true

# `install`, not `cp`: sets owner and mode in one step (tar extracted as root),
# and unlinks the destination first, so it works even against a binary that is
# still executing, where cp fails with ETXTBSY.
install -o "${OWNER%%:*}" -g "${OWNER##*:}" -m 755 "$src/osmpbudynkiv2" "$APP_DIR/osmpbudynkiv2"

# Self-update. Safe for the same reason `install` is used above: bash reads a
# script lazily by byte offset, so `cp`-ing over one mid-run makes the shell
# read the new bytes at its old offset and die ("unexpected EOF"). `install`
# unlinks first, so this process keeps its descriptor on the original inode and
# finishes normally -- the new script applies from the next run, meaning a
# change to it always lands one upgrade late. Guarded because releases built
# before this script existed do not carry it.
if [ -f "$src/download_release_and_update.sh" ]; then
  install -o "${OWNER%%:*}" -g "${OWNER##*:}" -m 755 \
    "$src/download_release_and_update.sh" "$APP_DIR/download_release_and_update.sh"
fi

rm -rf "$APP_DIR/web"
cp -a "$src/web" "$APP_DIR/web"
chown -R "$OWNER" "$APP_DIR/web"

if [ "$active" -eq 1 ] && [ "$no_start" -eq 0 ]; then
  systemctl start "$SERVICE"
elif [ "$active" -eq 1 ]; then
  echo "--no-start: leaving $SERVICE stopped (start it with: systemctl start $SERVICE)"
fi
echo "Updated to $("$APP_DIR/osmpbudynkiv2" --version)"
