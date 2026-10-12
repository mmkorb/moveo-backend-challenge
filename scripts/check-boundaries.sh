#!/usr/bin/env bash
# Enforces the hexagonal import rules of ADR 0003 / ADR 0014 for each service module.
#
# Usage: scripts/check-boundaries.sh [module-dir ...]
# Without arguments it checks services/producer and services/consumer.
#
# Rules (per module):
#   1. internal/domain      imports only the standard library
#   2. internal/ports       imports only the standard library and internal/domain
#   3. internal/application imports neither adapters nor platform
#   4. no struct tags in internal/domain
#   5. adapters do not import other adapters
#   6. platform imports none of domain, application, adapters
#   7. a service module neither imports nor requires the other service module
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export GOWORK=off

violations=0

report() {
	printf 'VIOLATION [%s] %s\n' "$1" "$2" >&2
	violations=$((violations + 1))
}

# is_stdlib reports whether an import path belongs to the standard library:
# its first path element has no dot (e.g. "context", "net/http").
is_stdlib() {
	local first=${1%%/*}
	[[ $first != *.* ]]
}

check_module() {
	local dir=$1 mod name other
	if [[ ! -f $dir/go.mod ]]; then
		printf 'error: %s has no go.mod\n' "$dir" >&2
		return 2
	fi
	mod=$(cd "$dir" && go list -m)
	name=$(basename "$dir")
	printf '== %s (%s)\n' "$name" "$mod"

	# One line per package: "<import path> <import>..." (non-test imports).
	local listing line pkg imp rest layer
	listing=$(cd "$dir" && go list -f '{{.ImportPath}} {{join .Imports " "}}' ./...)

	while IFS= read -r line; do
		[[ -n $line ]] || continue
		pkg=${line%% *}
		rest=${line#"$pkg"}
		rest=${rest# }
		layer=${pkg#"$mod"/}
		for imp in $rest; do
			# Rule 7 applies to every package.
			case $imp in
			*/moveo-backend-challenge/services/*)
				if [[ $imp != "$mod" && $imp != "$mod"/* ]]; then
					report "cross-module" "$pkg imports $imp"
				fi
				;;
			esac
			case $layer in
			internal/domain | internal/domain/*)
				is_stdlib "$imp" || report "domain" "$pkg imports non-stdlib $imp"
				;;
			internal/ports | internal/ports/*)
				if ! is_stdlib "$imp" && [[ $imp != "$mod/internal/domain" && $imp != "$mod/internal/domain/"* ]]; then
					report "ports" "$pkg imports $imp (only stdlib and domain allowed)"
				fi
				;;
			internal/application | internal/application/*)
				case $imp in
				"$mod"/internal/adapters/* | "$mod"/internal/platform/*)
					report "application" "$pkg imports $imp"
					;;
				esac
				;;
			internal/adapters/*)
				# adapters/<name>[/...] must not import adapters/<other>
				local self=${layer#internal/adapters/}
				self=${self%%/*}
				case $imp in
				"$mod"/internal/adapters/*)
					local target=${imp#"$mod"/internal/adapters/}
					target=${target%%/*}
					[[ $target == "$self" ]] || report "adapters" "$pkg imports another adapter: $imp"
					;;
				esac
				;;
			internal/platform | internal/platform/*)
				case $imp in
				"$mod"/internal/domain* | "$mod"/internal/application* | "$mod"/internal/adapters*)
					report "platform" "$pkg imports $imp"
					;;
				esac
				;;
			esac
		done
	done <<<"$listing"

	# Rule 4: struct tags in domain (non-test Go files).
	if [[ -d $dir/internal/domain ]]; then
		local hit
		while IFS= read -r hit; do
			[[ -n $hit ]] || continue
			report "domain-tags" "struct tag in $hit"
		done < <(grep -rnE --include='*.go' --exclude='*_test.go' '`[A-Za-z_][A-Za-z0-9_]*:"' "$dir/internal/domain" || true)
	fi

	# Rule 7: go.mod must not mention the other service module.
	other=$(grep -E "moveo-backend-challenge/services/" "$dir/go.mod" | grep -v "$mod" || true)
	if [[ -n $other ]]; then
		report "cross-module" "$dir/go.mod references another service module: $other"
	fi
}

if [[ $# -eq 0 ]]; then
	set -- "$root/services/producer" "$root/services/consumer"
fi

for d in "$@"; do
	check_module "$d" || violations=$((violations + 1))
done

if ((violations > 0)); then
	printf 'check-boundaries: %d violation(s)\n' "$violations" >&2
	exit 1
fi
echo "check-boundaries: OK"
