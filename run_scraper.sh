#!/usr/bin/env bash

set -euo pipefail

SCRAPER="${1:-}"
FEEDS_DIR="${2:-feeds}"
JOBS_DIR="${3:-jobs}"
JOB_DIR="${JOBS_DIR}/${SCRAPER}"
STATE_FILE='.state'
DATE="$(date --utc +'%Y-%m-%dT%H-%M-%S')"

if [[ -z "${SCRAPER}" ]]; then
    echo 'Scraper is required, aborting...'
    exit 1
fi

echo "Running scraper <${SCRAPER}>"
echo "Saving feeds to <${FEEDS_DIR}> and job data to <${JOB_DIR}>"

SAVEDIR="$(pwd)"
cd "$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"

function find_state() {
    DELETE=${3:-''}

    for DIR in "${1}"/*; do
        if [[ -d "${DIR}" && -f "${DIR}/${STATE_FILE}" && "$(cat "${DIR}/${STATE_FILE}")" == "${2}" ]]; then
            basename "${DIR}"
            if [[ -n "${DELETE}" ]]; then
                rm --recursive --force "${DIR}"
            fi
        fi
    done
}

mkdir --parents '.scrapy/httpcache' "${FEEDS_DIR}/${SCRAPER}" "${JOB_DIR}"

DELETED=$(find_state "${JOB_DIR}" 'finished' 'true')

if [[ -n "${DELETED}" ]]; then
    echo "Deleted finished jobs in <${JOB_DIR}>: ${DELETED}."
fi

RUNNING=$(find_state "${JOB_DIR}" 'running')

if [[ -n "${RUNNING}" ]]; then
    echo "Found a running job <$(echo "${RUNNING}" | tr -d '[:space:]')>, skipping <${SCRAPER}>..."
    cd "${SAVEDIR}"
    exit 0
fi

JOBTAG="${DATE}"
SHUT_DOWN="$(find_state "${JOB_DIR}" 'shutdown')"

if [[ -n "${SHUT_DOWN}" ]]; then
    JOBTAG="$(echo "${SHUT_DOWN}" | tr -d '[:space:]')"
    echo "Resuming previous job <${JOBTAG}> for spider <${SCRAPER}>."
else
    echo "Starting new job for spider <${SCRAPER}>."
fi

CURR_JOB="${JOB_DIR}/${JOBTAG}"

scrapy crawl "${SCRAPER}" \
    --output "${FEEDS_DIR}/%(name)s/%(class)s/%(time)s.jl" \
    --set "JOBDIR=${CURR_JOB}"

echo 'Done.'

cd "${SAVEDIR}"
