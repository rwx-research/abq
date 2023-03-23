#!/usr/bin/env bash
set -euo pipefail

# set -u will fail this script if $RELEASE_ARCHIVE, $RELEASE_BUCKET, $RELEASE_S3_OBJECT, $RELEASE_S3_NIGHTLY_OBJECT, $RELEASE_DIR, $NEW_RELEASE_S3_OBJECT, or $NEW_RELEASE_S3_NIGHTLY_OBJECT are not set

# this script uploads abq to s3
#
# It's used by the `build_and_upload` github workflow.

ACCESS_KEY_ID=$1
SECRET_ACCESS_KEY=$2
PROFILE=$3

aws configure set aws_access_key_id "$ACCESS_KEY_ID" --profile "$PROFILE"
aws configure set aws_secret_access_key "$SECRET_ACCESS_KEY" --profile "$PROFILE"

# deprecated format, remove after we're fully migrated to rfc-20 style binaries
aws s3 cp "$RELEASE_ARCHIVE" "s3://$RELEASE_BUCKET/$RELEASE_S3_OBJECT" --profile "$PROFILE"
aws s3api put-object-tagging \
  --bucket "$RELEASE_BUCKET" \
  --key "$RELEASE_S3_OBJECT" \
  --tagging "$(jq -n --arg commit "$(git rev-parse HEAD)" '{"TagSet": [{"Key":"commit", "Value": $commit}]}')" \
  --profile "$PROFILE"

aws s3 cp \
  "s3://$RELEASE_BUCKET/$RELEASE_S3_OBJECT" \
  "s3://$RELEASE_BUCKET/$RELEASE_S3_NIGHTLY_OBJECT" \
  --profile "$PROFILE"

# rfc-20 style binaries
aws s3 cp "$RELEASE_DIR/abq" "s3://$RELEASE_BUCKET/$NEW_RELEASE_S3_OBJECT" --profile "$PROFILE"
aws s3api put-object-tagging \
  --bucket "$RELEASE_BUCKET" \
  --key "$NEW_RELEASE_S3_OBJECT" \
  --tagging "$(jq -n --arg commit "$(git rev-parse HEAD)" '{"TagSet": [{"Key":"commit", "Value": $commit}]}')" \
  --profile "$PROFILE"

aws s3 cp \
  "s3://$RELEASE_BUCKET/$NEW_RELEASE_S3_OBJECT" \
  "s3://$RELEASE_BUCKET/$NEW_RELEASE_S3_NIGHTLY_OBJECT" \
  --profile "$PROFILE"

# rfc-20 style binaries for the tester harness
aws s3 cp "$RELEASE_DIR/abq_tester_harness" "s3://$RELEASE_BUCKET/$NEW_RELEASE_S3_OBJECT_TESTER_HARNESS" --profile "$PROFILE"
aws s3api put-object-tagging \
  --bucket "$RELEASE_BUCKET" \
  --key "$NEW_RELEASE_S3_OBJECT_TESTER_HARNESS" \
  --tagging "$(jq -n --arg commit "$(git rev-parse HEAD)" '{"TagSet": [{"Key":"commit", "Value": $commit}]}')" \
  --profile "$PROFILE"

aws s3 cp \
  "s3://$RELEASE_BUCKET/$NEW_RELEASE_S3_OBJECT_TESTER_HARNESS" \
  "s3://$RELEASE_BUCKET/$NEW_RELEASE_S3_NIGHTLY_OBJECT_TESTER_HARNESS" \
  --profile "$PROFILE"

# TODO: also upload gzip and set content-encoding
