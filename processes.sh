#!/usr/bin/env bash

set -euo pipefail

SAVEDIR="$(pwd)"
cd "$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"

JOBSDIR='jobs'
STATE_FILE='.state'
PID_FILE='.pid'
COMMAND=${1:-''}

function find_pids() {
	for FILE in $(find "${1}" -type 'f' -name "${PID_FILE}" -not -empty); do
		STATE="$(dirname "${FILE}")/${STATE_FILE}"
		if [[ -f "${STATE}" && "$(cat "${STATE}")" == 'running' ]]; then
			tr -d '[:space:]' < "${FILE}"
			echo
		fi
	done
}

for PID in $(find_pids "${JOBSDIR}"); do
	if [[ "${COMMAND}" == 'stop' ]]; then
		echo "Terminate process <${PID}>..."
		kill -SIGTERM "${PID}"
	else
		echo "${PID}"
	fi
done

cd "${SAVEDIR}"
