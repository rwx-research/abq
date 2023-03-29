# <img src="https://www.rwx.com/abq.svg" height="60" alt="abq">

:globe_with_meridians: [abq.build](https://abq.build) &ensp;
:bird: [@rwx_research](https://twitter.com/rwx_research) &ensp;
:speech_balloon: [discord](https://discord.gg/h4ha5Cue7j) &ensp;
:books: [documentation](https://www.rwx.com/docs/abq)

ABQ is a universal test runner that runs test suites in parallel.
It’s the best tool for splitting test suites into parallel jobs locally or on CI.
ABQ is implemented in Rust with bindings available for several test frameworks.

To use ABQ, check out the documentation on [getting started](https://www.rwx.com/docs/abq/getting-started).

## Demo

Here's a demo of running an RSpec test suite, and then using `abq` to run it in parallel.
ABQ invokes any test command passed to it, so you can continue using your native test framework CLI with any arguments it supports.

![abq-demo.svg](abq-demo.svg)

## Test Frameworks

:octocat: [rwx-research/rspec-abq](https://github.com/rwx-research/rspec-abq) &ensp;
:octocat: [rwx-research/jest-abq](https://github.com/rwx-research/jest-abq) &ensp;
:octocat: [rwx-research/playwright-abq](https://github.com/rwx-research/playwright-abq) &ensp;
:octocat: [rwx-research/pytest-abq](https://github.com/rwx-research/pytest-abq)

ABQ is currently compatible with

- [RSpec][rspec-abq-docs]
- [Jest][jest-abq-docs]
- [Playwright][playwright-abq-docs]
- [Pytest][playwright-abq-docs]

Open source sponsorship is available for anybody interested in implementing
bindings for other test frameworks. Get in touch on [discord](https://discord.gg/h4ha5Cue7j)
or by emailing oss@rwx.com

## Development

For notes on working on abq, see [development.md](development.md)

[rspec-abq-docs]: https://www.rwx.com/docs/abq/test-frameworks/rspec
[jest-abq-docs]: https://www.rwx.com/docs/abq/test-frameworks/jest
[playwright-abq-docs]: https://www.rwx.com/docs/abq/test-frameworks/playwright
[pytest-abq-docs]: https://www.rwx.com/docs/abq/test-frameworks/pytest
