#!/usr/bin/env bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http: //www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

# Default to patch if no argument
BUMP_TYPE="${1:-patch}"

if [[ ! "$BUMP_TYPE" =~ ^(major|minor|patch)$ ]]; then
    echo "Error: Argument must be 'major', 'minor', or 'patch'"
    exit 1
fi

# Ensure we are up to date with upstream
# If upstream doesn't exist, we might want to fallback or fail. 
# Assuming upstream exists as per previous script.
if git remote | grep -q upstream; then
    git fetch upstream main
    git checkout upstream/main
else
    git fetch origin main
    git checkout origin/main
fi

CURRENT_VERSION=$(jq -r '.version' version.json)
IFS='.' read -r -a parts <<< "$CURRENT_VERSION"

if [ ${#parts[@]} -ne 3 ]; then
    echo "Error: Version in version.json does not seem to be X.Y.Z format: $CURRENT_VERSION"
    exit 1
fi

MAJOR="${parts[0]}"
MINOR="${parts[1]}"
PATCH="${parts[2]}"

case "$BUMP_TYPE" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        ;;
    patch)
        PATCH=$((PATCH + 1))
        ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
echo "Bumping version: ${CURRENT_VERSION} -> ${NEW_VERSION}"

BRANCH_NAME="bump-version-${NEW_VERSION}"

# Create branch (delete if exists to start fresh)
if git show-ref --verify --quiet "refs/heads/${BRANCH_NAME}"; then
    git branch -D "${BRANCH_NAME}" || true
fi
git checkout -b "${BRANCH_NAME}"

# Update version.json
tmp=$(mktemp)
jq --arg ver "${NEW_VERSION}" '.version = $ver' version.json > "${tmp}" && mv "${tmp}" version.json

git add version.json
git commit -m "chore: Bump version to ${NEW_VERSION}"

echo "Ready to push and create PR for version ${NEW_VERSION}."
read -p "Do you want to proceed? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

# Push and create PR
git push -u origin "${BRANCH_NAME}" --force
gh pr create --fill --title "chore: Bump version to ${NEW_VERSION}" --body "Bumps version to ${NEW_VERSION}"
