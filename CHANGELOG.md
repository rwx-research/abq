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
[abq_homepage]: https://www.rwx.com/abq
[captain_homepage]: https://www.rwx.com/captain
[native_runner_protocol_0_2]: https://rwx.notion.site/ABQ-Native-Runner-Protocol-0-2-70b3ec70b2f64b84aa3253a558eba16f
