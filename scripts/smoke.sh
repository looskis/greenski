#!/bin/sh
set -eu

if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi

: "${TEST_RECIPIENT:?Set TEST_RECIPIENT in the environment or .env}"

cargo run --quiet -- status
cargo run --quiet -- send --to "$TEST_RECIPIENT" --text "Greenski smoke test"
cargo run --quiet -- events --since 0 --limit 20
