#!/usr/bin/env bash
# Fetch the frozen model artifacts from the GitHub release into ml/artifacts/.
# A fresh clone needs this once: the run is git-ignored (~729 MB total) and
# hosted as release assets per kickoff decision 3 (docs/PLAN.md).
#
# Two phases, so H2 never waits on the 438 MB regressor the streaming scorer
# does not load:
#   phase 1 (required for scoring): xgb_classifier.ubj, calibrator.joblib,
#            metrics.json, exceedance.json, _PUBLISHED.json
#   phase 2 (optional): xgb_regressor.ubj, logreg_pipeline.joblib — only
#            ml.serving.load_models' four-file completeness check and any
#            regression work need them; a phase-2 failure is a warning.
set -euo pipefail
RUN=20260730_145241
TAG="model-${RUN}"
# --repo pinned explicitly: a clone made from a local path (or any non-GitHub
# remote) gives gh nothing to infer the repository from — the dress rehearsal
# caught exactly that failing silently
REPO_SLUG="sebaleks/flight-delay-stream"
DIR="$(cd "$(dirname "$0")/.." && pwd)/ml/artifacts/${RUN}"
mkdir -p "${DIR}"

fetch() {
    if [ -s "${DIR}/$1" ]; then
        echo "  $1 already present"
    else
        gh release download "${TAG}" --repo "${REPO_SLUG}" --pattern "$1" \
            --dir "${DIR}" --clobber
        echo "  $1 fetched"
    fi
}

echo "phase 1: required for the streaming scorer"
for f in xgb_classifier.ubj calibrator.joblib metrics.json exceedance.json _PUBLISHED.json; do
    fetch "$f"
done
echo "READY FOR H2: classifier + calibrator present at ${DIR}"

echo "phase 2: optional (regressor + logreg baseline)"
for f in xgb_regressor.ubj logreg_pipeline.joblib; do
    fetch "$f" || echo "  WARNING: $f unavailable (upload may still be running)." \
        " Scoring works without it; ml.serving.load_models will insist on the" \
        " full four-file run — rerun this script later, or load the classifier" \
        " and calibrator directly."
done
ls -la "${DIR}"
