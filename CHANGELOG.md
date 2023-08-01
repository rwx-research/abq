## 1.6.2

ABQ 1.6.2 is a patch release.

An issue that could result in denial-of-service of an ABQ queue due to large
test results is fixed.

ABQ will now log a warning when a worker for a given run ID is using a test
command different from any other worker, either in the same run or during
retries. ABQ may not function properly if the test command for a run ID changes
between workers executing tests for that run ID.

## 1.6.1

ABQ 1.6.1 is a patch release fixing an issue that would not continue offloading
manifest and results files if one offload attempt failed.

## 1.6.0

ABQ 1.6.0 adds the `abq report list-tests` command, which lists the tests that were
run on an existing test run.

## 1.5.0

ABQ 1.5.0 adds new functionality for test distribution and the ability to replay
tests locally without modifying the remote queue.

By default, ABQ assigns the next available test to a worker that requests tests.
However, with ABQ 1.5.0, you have the option to distribute entire test files to
workers upon request. This feature can be beneficial when dealing with test
files that have expensive setup or teardown processes. By distributing the whole
file to a single worker responsible for that test file, you avoid running those
processes multiple times across multiple workers.

In version 1.5.0, ABQ now supports the usage of an RWX Personal Access token.
This token enables you to replay a remote run locally without modifying the
remote queue. Alongside the existing methods of setting the token through
environment variables and flags, you can now utilize the abq login command to
store your RWX access token locally for future use.

## 1.4.0

ABQ 1.4.0 adds support for loading test runs from other queue instances via
remote storage.

Until 1.4.0, a test run that executed on one instance of ABQ could not be loaded
and re-executed on another instance. Executing a test suite with the same run ID
on two different ABQ instances would run the test suite twice, with no sharing
of test results.

ABQ now writes `run_state` files to remote persistence locations, when remote
persistence is configured via the `ABQ_REMOTE_PERSISTENCE_STRATEGY` environment
variable, and its accompanying environment variables. If the same remote
persistence strategy is configured between multiple ABQ queue instances, queue
instances will have the capability to load test runs that initially executed on
previous queue instances.

At this time, loading test runs from other queue instances includes the
following restrictions:

- `run_state` files are schema-versioned, and no schema-version compatibility
    guarantees across versions of ABQ queues are provided at this time.
    `run_state` files are guaranteed to be compatible if shared between ABQ
    queues of the same version.
    If an ABQ queue loads a `run_state` file that it is incompatible with, the
    remote test run state will not be loaded. Executing a test suite whose
    run state file failed to be loaded will fall back on executing the test
    suite as a fresh run, similar to the pre-1.4.0 behavior.

- The same run ID may not be executed, in parallel, on two different ABQ queue
    instances sharing the same remote persistence. For a given run ID, an ABQ
    queue will **assume** exclusive ownership of the test suite run associated
    with that run ID.
    At this time, ABQ does **not** verify whether it indeed has exclusive
    ownership of a run ID. If you are self-hosting ABQ, you must ensure that
    run IDs are routed to a unique ABQ instance for the duration of a test run;
    however, once a test run is complete, retries of the test run may be routed
    to another ABQ instance, so long as the exclusive ownership constraint
    continues to apply for the duration of the retry.
    If you would like to avoid self-hosting, [RWX's managed hosting of ABQ][abq_pricing]
    supports routing test runs under these constraints.

See the [ABQ documentation][abq_docs_persistence] for more details on persistence.

## 1.3.5

ABQ 1.3.5 is a patch release that fixes a bug related to a race condition in how
test results are stored.

## 1.3.4

ABQ 1.3.4 is a patch release with feature preview of remote persisted storage.

A remote storage source is used to
- synchronize ABQ manifests and test results stored on local disk to the remote
  source
- offload manifests and test results at a configured frequency, synchronizing
  the data to the remote source and removing it from local disk
- download offloaded manifests and test results on-demand

When configured, files will be synced to the remote persistence target with
the following behavior:

- Manifests are synced to the remote persistence target after all entries in
  a test run's manifest have been assigned to a worker. The manifest is not
  modified again.
- Results are synced to the remote persitence target when there are no writes of
  results to the local persistence target in-flight. That is, writes to the
  remote target are batched and executed when the local state is stable.

The following environment configuration variables are available for configuring
remote storage on `abq start`:

- `ABQ_REMOTE_PERSISTENCE_STRATEGY`: What remote persistence strategy should be
  used. If unset, no remote persistence will be configured. The options for
  remote persistence are:
  - `s3`: manifests and results will be persisted to an AWS S3 bucket. This
    strategy requires the following additional environment variables to be
    set:

    - `ABQ_REMOTE_PERSISTENCE_S3_BUCKET`: The S3 bucket files should be written
      to.
    - `ABQ_REMOTE_PERSISTENCE_S3_KEY_PREFIX`: The prefix to use for keys written
      to the configured S3 bucket.

    AWS credentials and region information are read from the environment, using
    the [standard AWS environment variable support](https://docs.aws.amazon.com/sdkref/latest/guide/environment-variables.html).

  - `custom`: Files are remotely persisted by calling a provided executable.
    This strategy requires the following additional environment variables to be
    set:

    - `ABQ_REMOTE_PERSISTENCE_COMMAND`: A comma-delimited string of the executable
      and the head arguments that should be called to perform an operation on the
      remote persistence target. The executable will be called in the following form:

      ```
      <executable> <...arguments> <mode> <file-type> <run-id> <local-path>
      ```

      Where
        - `<mode>` is either "store" or "load", depending on whether the file should be stored
        into the remote location, or loaded from the remote location.
        - `<file-type>` is either "manifest" or "results".
        - `<run-id>` is the run ID of the test suite run.
        - `<local-path>` is the path to the file on the local filesystem. If the mode is "store",
        the content to upload should be read from this path. If the mode is "load", the
        downloaded content should be written to this path.

      The provided executable must be discoverable in the PATH that `abq start`
      is executed with.

      For example, if you have a Node.js script `abq-remote-persister.js` that
      performs remote persistence operations, the configured environment
      variable would be

      ```
      ABQ_REMOTE_PERSISTENCE_COMMAND="node,abq-remote-persister.js"
      ```

      Supposing that `node` is in the PATH.

- `ABQ_OFFLOAD_MANIFESTS_CRON`: If configured, the [cron schedule](https://docs.rs/cron/latest/cron/)
  by which manifest files stored locally should be offloaded to the remote
  storage location.
  A manifest file is only eligible for offload if it is stale; see
  `ABQ_OFFLOAD_STALE_FILE_THRESHOLD_HOURS` below.
  All time in the cron expression will be interpreted as UTC.

- `ABQ_OFFLOAD_RESULTS_CRON`: If configured, the [cron schedule](https://docs.rs/cron/latest/cron/)
  by which results files stored locally should be offloaded to the remote
  storage location.
  A results file is only eligible for offload if it is stale; see
  `ABQ_OFFLOAD_STALE_FILE_THRESHOLD_HOURS` below.
  All time in the cron expression will be interpreted as UTC.

- `ABQ_OFFLOAD_STALE_FILE_THRESHOLD_HOURS`: The threshold, in hours, since the
  last time a while was accessed before it is eligible for offloading to the
  configured remote persistence location, if any is configured.
  The default is 6 hours.

A future release of ABQ may support synchronizing manifests and test results
between ABQ queue instances, by way of a configured remote persistence target.

As this is a feature preview, the interface presented here may have a breaking
change in a future ABQ release.

## 1.3.3

ABQ 1.3.3 is a patch release with improvements to the stability of the queue
implementation.

- ABQ now uses the default musl libc memory allocator on x86 Linux targets,
  replacing use of the mimalloc memory allocator. For larger workloads, the
  default musl allocator is expected to provide better memory utilization
  characteristics. (#16)
- ABQ will now cancel active runs for which no progress has been made in an
  hour. (#12)

## 1.3.2

ABQ 1.3.2 includes bug fixes and improved output for `abq test` and `abq report`.

- Failure detail is provided in `abq report` output.
- Retried tests are listed in summary output.
- Runner number is included in failed/retried test lists during `abq test`.
- Worker number is included in failed/retried test lists during `abq report`.
- Fix reporting of native runner failures that are later retried.
- rwx-v1-json reporter counts native runner failures as `otherErrors`.
- Manifest generation output suppressed when it's empty.
- Document support for pytest.

## 1.3.1

ABQ 1.3.1 is a patch release with several bug fixes.

- ABQ now defaults to passing-through output from a test framework when `abq test` uses a single runner.
- Retry banners are output when a native test runner is re-launched for ABQ
  retries.
- ABQ's help menu is improved and usage patterns are clarified. References to
  the now-removed are elided.
- `abq report` now streams large test results messages as smaller chunks.
- The maximum-message-size restriction is lifted from intra-ABQ communication.

## 1.3.0

ABQ is a universal test runner for parallelizing your test suite. ABQ needs no
custom configuration to integrate with your test suite, making integration as
easy as installing an ABQ plugin and prepending `abq test -- ` to your existing
test command.

ABQ works locally and on all CI providers. It’s the best tool for splitting
test suites into parallel jobs in CI.

Get started with ABQ at [rwx.com/abq][abq_homepage].

Highlights of ABQ include:

- Optimal parallelization of your test suite using ABQ's message queue strategy
  of distributing tests.
  - ABQ's distribution strategy is designed to minimize
    network overhead, even if the message queue is far away.
- Parallelization of a test suite across any number of machines, with `abq test --worker <worker number>`.
- Running multiple parallel test processes in one invocation of `abq test` via
  the `-n` CLI flag.
- Automated retries of tests via the `--retries` flag to `abq test`.
- Manual retries of `abq test` nodes, that retry only the tests they ran the
  first time around.
- Test result reporting how you want it - ABQ won't interfere with your test
  framework's configured reporters.
  - ABQ also comes with standardized test result reporters, like `--reporter dot`, that can be used to enhance the reporters of your underlying test framework.
  - ABQ supports several structured reporters, like JUnit XML and RWX v1 JSON,
    so that you can export your test results in a machine-readable format
    without hassle.
- Aggregation of test results from all `abq test` nodes via `abq report`.
- ABQ is distributed as a standalone binary with all dependencies statically
  linked. All you need to run `abq test`, or self-host an ABQ queue, is to
  download the binary for your platform.
- Seamless integration with [Captain][captain_homepage].

This version of ABQ supports [ABQ native runner protocol 0.2][native_runner_protocol_0_2].
1.3.0 is the first public release of ABQ.

Learn more about using ABQ [at the docs][abq_docs].

[abq_docs]: https://www.rwx.com/docs/abq
[abq_docs]: https://www.rwx.com/docs/abq/persistence
[abq_homepage]: https://www.rwx.com/abq
[abq_pricing]: https://www.rwx.com/abq#abq-pricing
[captain_homepage]: https://www.rwx.com/captain
[native_runner_protocol_0_2]: https://rwx.notion.site/ABQ-Native-Runner-Protocol-0-2-70b3ec70b2f64b84aa3253a558eba16f
