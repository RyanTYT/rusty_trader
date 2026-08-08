#!/usr/bin/env bash
#
# sync_to_pub.sh
#
# Syncs new commits from the local "APP" repo into the sibling public
# "APP_PUB" repo, using git format-patch + git am, applying the
# strategy-folder include/exclude rules automatically.
#
# Usage:
#   ./sync_to_pub.sh                # sync all new commits since last sync
#   ./sync_to_pub.sh --dry-run       # just show what would be patched
#   ./sync_to_pub.sh --reset-marker <sha>   # manually set the sync marker
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
MARKER_FILE="$PUB_DIR/.last-synced-commit"

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
RESET_MARKER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --reset-marker)
      RESET_MARKER="${2:-}"
      [[ -z "$RESET_MARKER" ]] && { echo "error: --reset-marker requires a commit SHA"; exit 1; }
      shift 2
      ;;
    *) echo "unknown option: $1"; exit 1 ;;
  esac
done

if [[ ! -d "$APP_DIR/.git" ]]; then
  echo "error: APP_DIR ($APP_DIR) is not a git repo"; exit 1
fi
if [[ ! -d "$PUB_DIR/.git" ]]; then
  echo "error: PUB_DIR ($PUB_DIR) is not a git repo"; exit 1
fi

if [[ -n "$RESET_MARKER" ]]; then
  mkdir -p "$PUB_DIR"
  echo "$RESET_MARKER" > "$MARKER_FILE"
  echo "Marker reset to $RESET_MARKER"
  exit 0
fi

# ---------------------------------------------------------------------------
# DETERMINE RANGE
# ---------------------------------------------------------------------------

cd "$APP_DIR"

if ! git show-ref --verify --quiet "refs/heads/$SYNC_BRANCH"; then
  echo "error: branch '$SYNC_BRANCH' not found in APP repo ($APP_DIR)"
  exit 1
fi

if [[ -f "$MARKER_FILE" ]]; then
  LAST_SYNCED="$(cat "$MARKER_FILE")"
  if ! git cat-file -e "${LAST_SYNCED}^{commit}" 2>/dev/null; then
    echo "error: marker commit $LAST_SYNCED not found in APP repo history."
    echo "Fix or reset it with: $0 --reset-marker <sha>"
    exit 1
  fi
else
  echo "No marker file found at $MARKER_FILE."
  echo "This looks like the first sync. Defaulting to the repo's root commit."
  echo "If that's wrong, abort and run: $0 --reset-marker <sha>"
  read -r -p "Continue with full history? [y/N] " CONFIRM
  [[ "$CONFIRM" =~ ^[Yy]$ ]] || exit 1
  LAST_SYNCED="$(git rev-list --max-parents=0 "$SYNC_BRANCH" | tail -1)"
fi

NEW_HEAD="$(git rev-parse "$SYNC_BRANCH")"

if [[ "$LAST_SYNCED" == "$NEW_HEAD" ]]; then
  echo "Nothing to sync — APP is already at the last-synced commit."
  exit 0
fi

COMMIT_COUNT="$(git rev-list --count "$LAST_SYNCED..$NEW_HEAD")"
echo "Found $COMMIT_COUNT new commit(s) to sync ($LAST_SYNCED..$NEW_HEAD)"

# ---------------------------------------------------------------------------
# BUILD EXCLUDE PATHSPECS FOR THE STRATEGY FOLDER
# ---------------------------------------------------------------------------

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

git format-patch "$LAST_SYNCED..$NEW_HEAD" -o "$PATCH_DIR" -- . "${EXCLUDE_PATHSPECS[@]}"

PATCH_FILES=("$PATCH_DIR"/*.patch)
if [[ ! -e "${PATCH_FILES[0]}" ]]; then
  echo "No patches generated (all changes may have been in excluded paths)."
  echo "Updating marker to $NEW_HEAD anyway."
  if ! $DRY_RUN; then
    echo "$NEW_HEAD" > "$MARKER_FILE"
  fi
  exit 0
fi

echo "Generated ${#PATCH_FILES[@]} patch file(s) in $PATCH_DIR"

if $DRY_RUN; then
  echo
  echo "--- DRY RUN: patches that would be applied ---"
  for p in "${PATCH_FILES[@]}"; do
    basename "$p"
  done
  echo "(marker NOT updated, nothing applied — rerun without --dry-run to apply)"
  exit 0
fi

# ---------------------------------------------------------------------------
# APPLY TO PUBLIC REPO
# ---------------------------------------------------------------------------

cd "$PUB_DIR"

echo
echo "Applying patches to $PUB_DIR ..."
if git am --committer-date-is-author-date "$PATCH_DIR"/*.patch; then
  echo "$NEW_HEAD" > "$MARKER_FILE"
  git add "$MARKER_FILE" 2>/dev/null || true
  if ! git diff --cached --quiet; then
    git commit -m "chore: update sync marker to $NEW_HEAD" >/dev/null
  fi
  rm -rf "$PATCH_DIR"
  echo
  echo "✅ Synced successfully. Marker updated to $NEW_HEAD."
else
  echo
  echo "⚠️  git am stopped partway through (conflict or missing path)."
  echo "Resolve it manually, then either:"
  echo "  git am --continue   (after fixing/staging conflicts)"
  echo "  git am --skip       (to skip the offending patch)"
  echo "  git am --abort      (to bail out entirely)"
  echo
  echo "Once 'git am' reports no session in progress, re-run this script"
  echo "with:  $0 --reset-marker $NEW_HEAD"
  echo "to mark everything up to $NEW_HEAD as synced (only do this once"
  echo "you've confirmed all patches were actually applied or intentionally skipped)."
  exit 1
fi
