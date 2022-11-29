#!/bin/bash

set -eu

cd $(git rev-parse --show-toplevel)

source bin/dev_queue_state.sh

BATCH_SIZE=14
ABQ_WORKER_OVERHEAD_THRESHOLD="10" # 10%

ABQ_LOG=abq=info

OUTDIR="bigtest/benchmark/out"

rm -rf $OUTDIR
mkdir -p $OUTDIR

ABQ_BENCHMARK_OUT="$OUTDIR/abq_benchmark_out.txt"
RSPEC_BENCHMARK_OUT="$OUTDIR/rspec_benchmark_out.txt"
EXPORTS="$OUTDIR/exports"

abq test \
  --reporter dot \
  --batch-size $BATCH_SIZE \
  --queue-addr $(cat $IP_FILE) \
  --token $(cat $USER_TOKEN_FILE) \
  --tls-cert $CERT_FILE \
  -n 1 \
  -- bundle exec rspec bigtest/benchmark/benchmark_spec.rb \
  | tee $ABQ_BENCHMARK_OUT

bundle exec rspec bigtest/benchmark/benchmark_spec.rb | tee $RSPEC_BENCHMARK_OUT

RE_FLOAT="[0-9]+.[0-9]+"
RE_WORKER_SUMMARY_LINE="Finished in $RE_FLOAT seconds \(files"

# TODO: we should check the `abq test` wall time here as well.
ABQ_WORKER_TIME="$(cat "$ABQ_BENCHMARK_OUT" | grep -Eo "$RE_WORKER_SUMMARY_LINE" | grep -Eo "$RE_FLOAT")"
RSPEC_TIME="$(cat "$RSPEC_BENCHMARK_OUT" | grep -Eo "$RE_WORKER_SUMMARY_LINE" | grep -Eo "$RE_FLOAT")"

# Round decimal percentage to 4 sig figs, then make 100%-scale percentage with 2 sig figs
ABQ_WORKER_OVERHEAD="$(echo "scale=4; del=($ABQ_WORKER_TIME - $RSPEC_TIME) / $RSPEC_TIME; scale=2; del * 100 / 1" | bc)"

echo "abq workers were $ABQ_WORKER_OVERHEAD% slower; threshold is $ABQ_WORKER_OVERHEAD_THRESHOLD%"

touch "$EXPORTS"
echo "ABQ_WORKER_TIME=$ABQ_WORKER_TIME" >> $EXPORTS
echo "RSPEC_TIME=$RSPEC_TIME" >> $EXPORTS
echo "ABQ_WORKER_OVERHEAD=$ABQ_WORKER_OVERHEAD" >> $EXPORTS

if (( $(echo "$ABQ_WORKER_OVERHEAD >= $ABQ_WORKER_OVERHEAD_THRESHOLD" | bc -l))); then
  exit 1
fi
