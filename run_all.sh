#!/usr/bin/env bash

set -euo pipefail

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

mkdir --parents 'logs' "${JOBSDIR}"

DATE=$(date --utc +'%Y-%m-%dT%H-%M-%S')

for SCRAPER in $(scrapy list); do
    JOBDIR="${JOBSDIR}/${SCRAPER}"

    DELETED=$(find_state "${JOBDIR}" 'finished' 'true')

    if [[ -n "${DELETED}" ]]; then
        echo "Deleted finished jobs in <${JOBDIR}>: ${DELETED}."
    fi

    RUNNING=$(find_state "${JOBDIR}" 'running')

    if [[ -n "${RUNNING}" ]]; then
        echo -e "Found a running job <$(echo "${RUNNING}" | tr -d '[:space:]')>, skipping <${SCRAPER}>...\\n"
        continue
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

    nohup scrapy crawl "${SCRAPER}" \
        --output 'feeds/%(name)s/%(class)s/%(time)s.jl' \
        --set "JOBDIR=${CURR_JOB}" \
        >> "logs/${SCRAPER}.log" 2>&1 &

    echo -e "Started! Follow logs from <$(pwd)/logs/${SCRAPER}.log>.\\n"
done

cd "${SAVEDIR}"
