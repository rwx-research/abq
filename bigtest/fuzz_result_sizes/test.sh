#!/bin/bash

set -eu

cd $(git rev-parse --show-toplevel)

source bin/dev_queue_state.sh

export ABQ_LOG=abq=debug

EXIT=0
abq test \
    --reporter dot \
    --run-id "bigtest-$(uuidgen)" \
    --queue-addr "$(cat $IP_FILE)" \
    --token "$(cat $USER_TOKEN_FILE)" \
    --tls-cert "${CERT_FILE}" \
    -n 1 \
    -- bundle exec rspec bigtest/fuzz_result_sizes/fuzz_result_sizes_spec.rb > /dev/null \
    || EXIT=$?

# We should fail, but with no abq-internal errors
if [ "$EXIT" -ne "1" ]; then
  exit 1
fi
