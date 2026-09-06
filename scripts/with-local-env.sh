#!/usr/bin/env bash
# Load this checkout's private connector settings for one command.
set -euo pipefail
# Do not echo credentials even if invoked through bash -x.
set +x

if [[ $# -eq 0 ]]; then
  echo "Usage: scripts/with-local-env.sh COMMAND [ARGUMENT ...]" >&2
  exit 2
fi

dbtv_repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
export DBTV_LOCAL_DIR="$dbtv_repo_dir/.local"
if [[ ! -f "$DBTV_LOCAL_DIR/env.sh" ]]; then
  echo "Missing .local/env.sh. See docs/local-connectors.md for setup." >&2
  exit 2
fi

# This is a trusted, user-owned shell file, excluded from Git.
set -a
source "$DBTV_LOCAL_DIR/env.sh"
set +a
exec "$@"
