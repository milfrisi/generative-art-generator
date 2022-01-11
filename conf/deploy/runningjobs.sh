#!/bin/bash
USER=$(get-properties -k user)
PROJECT=$(get-conf -k project)
RUNNING_JOBS=$(curl -H 'Accept: application/json' --negotiate -u : "$OOZIE_URL/v2/jobs?filter=user%3D${USER}%3Btext%3D${PROJECT}%3Bstatus%3DSUCCEEDED" | jq ".total")

echo ${RUNNING_JOBS}