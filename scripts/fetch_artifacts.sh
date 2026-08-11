#!/usr/bin/env bash
# Fetch the frozen model artifacts from the GitHub release into ml/artifacts/.
# A fresh clone needs this once: the run is git-ignored (729 MB) and hosted as
# release assets per kickoff decision 3 (docs/PLAN.md). Needs `gh` logged in
# to a account that can read sebaleks/flight-delay-stream.
set -euo pipefail
RUN=20260730_145241
DIR="$(cd "$(dirname "$0")/.." && pwd)/ml/artifacts/${RUN}"
if [ -f "${DIR}/xgb_classifier.ubj" ] && [ -f "${DIR}/calibrator.joblib" ]; then
    echo "artifacts already present at ${DIR}"
    exit 0
fi
mkdir -p "${DIR}"
gh release download "model-${RUN}" --dir "${DIR}" --clobber
ls -la "${DIR}"
echo "artifacts fetched into ${DIR}"
