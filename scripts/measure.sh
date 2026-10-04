#!/bin/bash
# Rerun every number in the README's Testing and Performance sections and keep
# the output in results/. About half an hour. Run it on a quiet machine: the
# benchmarks share the laptop's cores and SSD with whatever else is running.
#
#   scripts/measure.sh
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
./gradlew installDist --offline -q || exit 1

stamp() {
    echo "# $(sysctl -n machdep.cpu.brand_string), $(sw_vers -productName) $(sw_vers -productVersion), $(java -version 2>&1 | head -1)"
    echo "# $(pmset -g batt | head -1 | sed "s/Now drawing from //"), $(date '+%Y-%m-%d %H:%M')"
}

# The simulator. A search that finds a violation prints FAILED and exits 1,
# which is the expected outcome for the Figure 8 search, so failures are kept.
sim() {
    local name=$1
    shift
    { stamp; echo "# ./gradlew simulate --args=\"$*\""; ./gradlew simulate --offline -q --args="$*" 2>&1; } > "results/$name.txt"
    tail -2 "results/$name.txt"
}
sim sim_5nodes --seeds 10000
sim sim_3nodes --seeds 10000 --nodes 3
sim figure8_default_3nodes --seeds 2000 --nodes 3 --unsafe-commit-old-terms
sim figure8_default_5nodes --seeds 2000 --unsafe-commit-old-terms
sim figure8_tuned --seeds 2000 --nodes 3 --unsafe-commit-old-terms --max-entries-per-append 1 --crash-rate 0.03

{ stamp; python3 scripts/fullfsync.py; } > results/fullfsync.txt
cat results/fullfsync.txt

# Each bench run opens thousands of connections, and closed ones hold a local
# port in TIME_WAIT for 30 s; wait for them to clear before the next run.
quiet_ports() {
    while [ "$(netstat -an -p tcp | grep -c TIME_WAIT)" -gt 200 ]; do sleep 2; done
}
bench() {
    quiet_ports
    echo "## bench $*" >> results/bench.txt
    ./gradlew run --offline -q --args="bench $*" 2>&1 | grep -v "^cluster of" | tee -a results/bench.txt
}
stamp > results/bench.txt
for c in 1 8 32 128 512 2048 8192; do bench --nodes 3 --clients $c --seconds 10; done
for i in 2 3; do bench --nodes 3 --clients 8192 --seconds 10; done
for c in 1 128 2048 8192; do bench --nodes 3 --clients $c --seconds 10 --sync false; done
bench --nodes 1 --clients 1 --seconds 10
bench --nodes 5 --clients 2048 --seconds 10
bench --nodes 5 --clients 8192 --seconds 10
bench --nodes 3 --clients 2048 --seconds 10 --reads 1
bench --nodes 3 --clients 2048 --seconds 10 --reads 0.9

quiet_ports
{ stamp; ./gradlew run --offline -q --args="failover --nodes 3 --rounds 20" 2>&1 | grep -v "^cluster of"; } > results/failover.txt
tail -1 results/failover.txt
