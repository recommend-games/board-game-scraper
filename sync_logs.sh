#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
FEEDS_DIR="${BASE_DIR}/feeds"
USERS_DIR="${FEEDS_DIR}/r_g_users"
RESPONSES_DIR="${FEEDS_DIR}/r_g_responses"

GC_PROJECT="${1:-${PULL_QUEUE_PROJECT:-}}"
if [[ -z "${GC_PROJECT}" ]] && type dotenv &> /dev/null; then
    GC_PROJECT="$(dotenv --file "${BASE_DIR}/.env" get PULL_QUEUE_PROJECT)"
fi

if [[ -z "${GC_PROJECT}" ]]; then
    echo "No Google Cloud project given, please provide as command line argument or set \$PULL_QUEUE_PROJECT environment variable"
    exit 1
fi

USERS_BUCKET="${GC_PROJECT}-logs"
RESPONSES_BUCKET="${GC_PROJECT}-responses"

echo "Syncing GCS bucket <${USERS_BUCKET}> with <${USERS_DIR}>…"
gsutil -m rsync -r "gs://${USERS_BUCKET}/" "${USERS_DIR}/"

echo "Syncing GCS bucket <${RESPONSES_BUCKET}> with <${RESPONSES_DIR}>…"
gsutil -m rsync -r "gs://${RESPONSES_BUCKET}/" "${RESPONSES_DIR}/"

echo "Done."
