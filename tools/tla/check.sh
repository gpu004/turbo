#!/usr/bin/env bash
# Model-checks spec/QueueBroker.tla with TLC. Needs Java 11 or later.
#
# spec/QueueBroker.cfg must pass. spec/QueueBrokerNoCAS.cfg turns off the R2
# compare-and-set and must fail, which shows the invariants still catch the
# bug they exist for.
set -euo pipefail

cd "$(dirname "$0")/../.."

version=1.8.0
sha256=fdffef247f563ef6edbfd44958d2a82db79e7564e07cb673262d896353fa6713
jar="${TLA2TOOLS_JAR:-.cache/tla2tools-$version.jar}"

if [[ ! -f "$jar" ]]; then
  mkdir -p "$(dirname "$jar")"
  curl -sSfL -o "$jar" "https://github.com/tlaplus/tlaplus/releases/download/v$version/tla2tools.jar"
fi

echo "$sha256  $jar" | shasum -a 256 -c - >/dev/null

meta="$(mktemp -d)"
trap 'rm -rf "$meta"' EXIT

tlc() {
  java -Xmx2g -cp "$jar" tlc2.TLC -workers auto -deadlock -noGenerateSpecTE \
    -metadir "$meta/$1" -config "spec/$1" spec/QueueBroker.tla
}

echo "QueueBroker.cfg: checking that the invariants hold"
tlc QueueBroker.cfg

echo "QueueBrokerNoCAS.cfg: checking that removing the compare-and-set breaks an invariant"
if output="$(tlc QueueBrokerNoCAS.cfg 2>&1)"; then
  echo "$output"
  echo "QueueBrokerNoCAS.cfg passed, so the invariants no longer catch a lost write." >&2
  exit 1
fi

if ! grep -q "is violated" <<<"$output"; then
  echo "$output"
  echo "QueueBrokerNoCAS.cfg failed for a reason other than an invariant violation." >&2
  exit 1
fi

grep "is violated" <<<"$output"
