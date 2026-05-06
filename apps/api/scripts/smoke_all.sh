#!/usr/bin/env bash
set -euo pipefail

echo "== OneHaven full local smoke suite =="
echo "Assumption: docker compose stack is already up."
echo

docker compose ps

echo
./apps/api/scripts/smoke_epic5.sh

echo
./apps/api/scripts/smoke_geo.sh

echo
./apps/api/scripts/smoke_source_freshness.sh

echo
./apps/api/scripts/smoke_pipeline_observability.sh

echo
./apps/api/scripts/smoke_application_logging.sh

echo
./apps/api/scripts/smoke_frontend.sh

echo
echo "Full local smoke suite passed."
