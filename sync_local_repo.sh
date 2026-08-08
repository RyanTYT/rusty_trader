#!/usr/bin/env bash
#
# sync_to_pub.sh
#
# Syncs new commits from the local "APP" repo (rust-highsierra branch)
# into the sibling public "APP_PUB" repo, using git format-patch + git am.
#
# Range is determined by DATE, not by a stored marker: it takes the
# author date of APP_PUB's current HEAD commit, and syncs every commit
# on APP's rust-highsierra branch authored strictly after that date.
# This works because every sync applies patches with
# --committer-date-is-author-date, so APP_PUB's HEAD author date always
# matches the original commit's date in APP.
#
# Usage:
#   ./sync_to_pub.sh              # sync all commits newer than APP_PUB's HEAD date
#   ./sync_to_pub.sh --dry-run    # just show what would be patched
#
# Run this script from anywhere; paths below are resolved relative to
# its own location (assumes APP and APP_PUB are sibling folders).

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIG — edit these to match your setup
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

APP_DIR="$SCRIPT_DIR/APP"          # local (private) repo
PUB_DIR="$SCRIPT_DIR/APP_PUB"      # public repo
PATCH_DIR="$APP_DIR/.sync-patches" # scratch dir for patch files

# Only commits on this branch (in APP) are ever synced to the public repo.
# Referenced by name via plumbing commands below — does NOT require this
# branch to be checked out, so it won't disturb whatever branch you're
# currently on in APP.
SYNC_BRANCH="rust-highsierra"

# Files that should NEVER be synced to the public repo
ALWAYS_EXCLUDE=(
  "trading-app/src/strategy/mod.rs"
  "trading-app/src/init_app.rs"
)

# Under trading-app/src/strategy, ONLY these files (+ the helpers dir)
# are allowed through. Everything else under that folder is excluded.
STRATEGY_DIR="trading-app/src/strategy"
STRATEGY_KEEP_FILES=(
  "trading-app/src/strategy/manual.rs"
  "trading-app/src/strategy/unknown.rs"
  "trading-app/src/strategy/noise.rs"
  "trading-app/src/strategy/threshold_rebalancing.rs"
  "trading-app/src/strategy/strategy.rs"
  "trading-app/src/strategy/portfolio_functions.rs"
)
STRATEGY_KEEP_DIR="trading-app/src/strategy/helpers"

# ---------------------------------------------------------------------------
# ARG PARSING
# ---------------------------------------------------------------------------

DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "unknown option: $1"; exit 1 ;;
  esac
done

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "error: APP_DIR ($APP_DIR) is not a git repo"; exit 1
fi
if [[ ! -d "$PUB_DIR/.git" ]]; then
  echo "error: PUB_DIR ($PUB_DIR) is not a git repo"; exit 1
fi

# ---------------------------------------------------------------------------
# DETERMINE RANGE — by date, based on APP_PUB's current HEAD
# ---------------------------------------------------------------------------

if ! git -C "$APP_DIR" show-ref --verify --quiet "refs/heads/$SYNC_BRANCH"; then
  echo "error: branch '$SYNC_BRANCH' not found in APP repo ($APP_DIR)"
  exit 1
fi

# Author date (ISO 8601, with timezone) of APP_PUB's latest commit.
CUTOFF_DATE="$(git -C "$PUB_DIR" log -1 --format=%aI)"
CUTOFF_SUBJECT="$(git -C "$PUB_DIR" log -1 --format=%s)"

echo "APP_PUB HEAD: \"$CUTOFF_SUBJECT\" ($CUTOFF_DATE)"
echo "Syncing commits on '$SYNC_BRANCH' authored after that date..."

COMMIT_COUNT="$(git -C "$APP_DIR" rev-list --count --since="$CUTOFF_DATE" "$SYNC_BRANCH")"
if [[ "$COMMIT_COUNT" -eq 0 ]]; then
  echo "Nothing to sync — no commits on '$SYNC_BRANCH' newer than APP_PUB's HEAD date."
  exit 0
fi
echo "Found $COMMIT_COUNT commit(s) newer than the cutoff."

# Resolve the date filter into an explicit two-dot commit range ourselves.
# NOTE: we deliberately do NOT hand `--since` + a bare single ref straight
# to `git format-patch` — format-patch special-cases a lone revision
# argument to mean "just that one commit" (unlike `git log`/`git rev-list`,
# which walk full history from it), so combining it with --since silently
# produces zero patches instead of the expected filtered range.
OLDEST_COMMIT="$(git -C "$APP_DIR" rev-list --since="$CUTOFF_DATE" "$SYNC_BRANCH" | tail -1)"
if git -C "$APP_DIR" rev-parse --verify -q "${OLDEST_COMMIT}^" >/dev/null; then
  PATCH_RANGE="${OLDEST_COMMIT}^..$SYNC_BRANCH"
else
  # OLDEST_COMMIT is the repo's root commit — no parent to range from.
  PATCH_RANGE="--root $SYNC_BRANCH"
fi

# ---------------------------------------------------------------------------
# BUILD EXCLUDE PATHSPECS FOR THE STRATEGY FOLDER
# ---------------------------------------------------------------------------

cd "$APP_DIR"

STRATEGY_EXCLUDES=()
while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  keep=false
  for k in "${STRATEGY_KEEP_FILES[@]}"; do
    [[ "$f" == "$k" ]] && keep=true && break
  done
  [[ "$f" == "$STRATEGY_KEEP_DIR"/* ]] && keep=true
  $keep || STRATEGY_EXCLUDES+=(":!$f")
done < <(git ls-tree -r --name-only "$SYNC_BRANCH" -- "$STRATEGY_DIR" 2>/dev/null || true)

EXCLUDE_PATHSPECS=()
for f in "${ALWAYS_EXCLUDE[@]}"; do
  EXCLUDE_PATHSPECS+=(":!$f")
done
EXCLUDE_PATHSPECS+=("${STRATEGY_EXCLUDES[@]}")

# ---------------------------------------------------------------------------
# GENERATE PATCHES
# ---------------------------------------------------------------------------

rm -rf "$PATCH_DIR"
mkdir -p "$PATCH_DIR"

cd "$APP_DIR"
git format-patch $PATCH_RANGE -o "$PATCH_DIR" -- . "${EXCLUDE_PATHSPECS[@]}"

PATCH_FILES=("$PATCH_DIR"/*.patch)
if [[ ! -e "${PATCH_FILES[0]}" ]]; then
  echo "No patches generated (all changes may have been in excluded paths)."
  exit 0
fi

echo "Generated ${#PATCH_FILES[@]} patch file(s) in $PATCH_DIR"

if $DRY_RUN; then
  echo
  echo "--- DRY RUN: patches that would be applied ---"
  for p in "${PATCH_FILES[@]}"; do
    basename "$p"
  done
  echo "(nothing applied — rerun without --dry-run to apply)"
  exit 0
fi

# ---------------------------------------------------------------------------
# APPLY TO PUBLIC REPO
# ---------------------------------------------------------------------------

cd "$PUB_DIR"

echo
echo "Applying patches to $PUB_DIR ..."
if git am --committer-date-is-author-date "$PATCH_DIR"/*.patch; then
  rm -rf "$PATCH_DIR"
  echo
  echo "✅ Synced successfully."
  echo "APP_PUB's new HEAD date will be the cutoff for the next run."
else
  echo
  echo "⚠️  git am stopped partway through (conflict or missing path)."
  echo "Resolve it manually, then either:"
  echo "  git am --continue   (after fixing/staging conflicts)"
  echo "  git am --skip       (to skip the offending patch)"
  echo "  git am --abort      (to bail out entirely)"
  echo
  echo "IMPORTANT: because the range is date-based (not marker-based), if you"
  echo "'--skip' a patch, that commit's changes will NOT be retried on the next"
  echo "run — the next run's cutoff is simply APP_PUB's new HEAD date, and any"
  echo "skipped commit's date will already be earlier than that. If you need"
  echo "it applied, resolve it manually now rather than skipping."
  exit 1
fi
