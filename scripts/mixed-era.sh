#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

release=${GITHUB_REF_TYPE-}

record=$(mktemp)
trap 'rm -f "$record"' EXIT

status=0
MIXED_ERA_COVERAGE=$record ./scripts/test.sh \
	-race -count=1 -tags mixedera -v -timeout 900s ./ci/mixedera/... "$@" || status=$?

subject=$(awk -F'\t' '$1 == "subject" { print $2 }' "$record")
if [ -z "$subject" ]; then
	echo "mixed-era: the run left no coverage record, so no part of the matrix reported a verdict" >&2
	exit 1
fi

if [ "$release" != tag ]; then
	exit "$status"
fi

stub=$(awk -F'\t' -v s="$subject" '$1 == "peer" && $2 == s && $3 != s { print $3 }' "$record")
if [ -n "$stub" ]; then
	echo "mixed-era: this release drives a \"$stub\" peer; the $subject era's real transport is not under test" >&2
	echo "mixed-era: a peer the harness builds for the $subject era has to name that era in its own conformance, and this one declared \"$stub\" instead — a stand-in wearing the $subject label. No tag clears this gate while the two disagree." >&2
	status=1
fi

unproven=$(awk -F'\t' -v s="$subject" '$1 == "coverage" && $2 == s && $4 != "PROVEN" && $4 != "ABSENT" && $4 != "ENTAILED" { print "  " $3 " " $4 }' "$record")
if [ -n "$unproven" ]; then
	echo "mixed-era: docs/DESIGN.md §8.4 makes this gate non-waivable, and this release leaves the $subject era with:" >&2
	echo "$unproven" >&2
	status=1
fi

exit "$status"
