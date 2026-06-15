#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config="${HEALTHCHECK_CONFIG:-${script_dir}/../config/healthcheck.json}"

if [[ $# -gt 0 && "$1" != --* ]]; then
  config="$1"
  shift
fi

exec python3 "${script_dir}/healthcheck.py" --config "${config}" "$@"
