#!/usr/bin/env bash

set -euo pipefail

SCRAPER="${1:-}"

if [[ -z "${SCRAPER}" ]]; then
    echo 'Scraper is required, aborting...'
    exit 1
fi

echo "Running scraper <${SCRAPER}>"

SAVEDIR="$(pwd)"
cd "$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"

JOBSDIR='jobs'
STATE_FILE='.state'

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

mkdir --parents '.scrapy/httpcache' "${JOBSDIR}" "feeds/${SCRAPER}"

DATE=$(date --utc +'%Y-%m-%dT%H-%M-%S')

JOBDIR="${JOBSDIR}/${SCRAPER}"

DELETED=$(find_state "${JOBDIR}" 'finished' 'true')

if [[ -n "${DELETED}" ]]; then
    echo "Deleted finished jobs in <${JOBDIR}>: ${DELETED}."
fi

RUNNING=$(find_state "${JOBDIR}" 'running')

if [[ -n "${RUNNING}" ]]; then
    echo "Found a running job <$(echo "${RUNNING}" | tr -d '[:space:]')>, skipping <${SCRAPER}>..."
    exit 0
fi

JOBTAG="${DATE}"
SHUT_DOWN="$(find_state "${JOBDIR}" 'shutdown')"

if [[ -n "${SHUT_DOWN}" ]]; then
    JOBTAG="$(echo "${SHUT_DOWN}" | tr -d '[:space:]')"
    echo "Resuming previous job <${JOBTAG}> for spider <${SCRAPER}>."
else
    echo "Starting new job for spider <${SCRAPER}>."
fi

CURR_JOB="jobs/${SCRAPER}/${JOBTAG}"

scrapy crawl "${SCRAPER}" \
    --output 'feeds/%(name)s/%(class)s/%(time)s.jl' \
    --set "JOBDIR=${CURR_JOB}"

cd "${SAVEDIR}"
