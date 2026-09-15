#!/usr/bin/env bash
# Build and install kite to a user-local directory, optionally updating PATH.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat <<'EOF'
Usage: scripts/install.sh [--help]

Build and install kite. Set KITE_INSTALL_DIR to override the default
installation directory of ~/.local/bin.
EOF
}

if [[ ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if (( $# > 0 )); then
  usage >&2
  exit 2
fi

if ! command -v zig >/dev/null 2>&1; then
  echo "zig not found — download it from https://ziglang.org/download" >&2
  exit 1
fi

zig build

DEST=${KITE_INSTALL_DIR:-$HOME/.local/bin}
mkdir -p "$DEST"
install -m755 zig-out/bin/kite "$DEST/kite"
printf 'installed %s\n' "$DEST/kite"

path_has_dest=false
path_dest=${DEST%/}
old_ifs=$IFS
IFS=:
read -ra path_entries <<< "$PATH"
IFS=$old_ifs
for path_entry in "${path_entries[@]}"; do
  if [[ $path_entry == "$DEST" || $path_entry == "$path_dest" || $path_entry == "$path_dest/" ]]; then
    path_has_dest=true
    break
  fi
done

if [[ $path_has_dest == true ]]; then
  echo "kite is on your PATH — try: kite --help"
  exit 0
fi

shell_name=$(basename "${SHELL:-}")
rc=
case "$shell_name" in
  zsh)
    rc=$HOME/.zshrc
    ;;
  bash)
    if [[ $(uname -s) == Darwin ]]; then
      rc=$HOME/.bash_profile
    else
      rc=$HOME/.bashrc
    fi
    ;;
  fish)
    rc=$HOME/.config/fish/config.fish
    ;;
esac

rc_dest=$DEST
case "$DEST" in
  "$HOME")
    rc_dest='$HOME'
    ;;
  "$HOME"/*)
    rc_dest="\$HOME/${DEST#"$HOME"/}"
    ;;
esac

if [[ $shell_name == fish ]]; then
  path_line="fish_add_path \"$DEST\""
else
  path_line="export PATH=\"$rc_dest:\$PATH\""
fi

if [[ -n $rc && -t 0 ]]; then
  printf 'Add %s to your PATH in %s? [Y/n] ' "$DEST" "$rc"
  read -r answer
  if [[ -z $answer || $answer =~ ^[Yy]([Ee][Ss])?$ ]]; then
    mkdir -p "$(dirname "$rc")"
    if [[ ! -f $rc ]] || ! grep -qF "$path_line" "$rc"; then
      {
        printf '\n# added by kite installer\n'
        printf '%s\n' "$path_line"
      } >> "$rc"
      echo "added — restart your shell or run: source $rc"
    else
      echo "already present — restart your shell or run: source $rc"
    fi
    exit 0
  fi
fi

printf '%s\n' "$path_line"
echo "Add that line to your shell startup file and restart your shell."
