const cp = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

function exec(shellCmd, options = {}) {
  return cp.execSync(shellCmd, { shell: "/bin/bash", ...options });
}

// ANSI escape codes due to the `chalk` project, licensed under MIT with source
// found at
//
//   https://github.com/chalk/ansi-regex/blob/02fa893d619d3da85411acc8fd4e2eea0e95a9d9/index.js#L1-L8
//
// MIT License
//
// Copyright (c) Sindre Sorhus <sindresorhus@gmail.com> (https://sindresorhus.com)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
const RE_ANSI = (() => {
  const pattern = [
    "[\\u001B\\u009B][[\\]()#;?]*(?:(?:(?:(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]+)*|[a-zA-Z\\d]+(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]*)*)?\\u0007)",
    "(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PR-TZcf-nq-uy=><~]))",
  ].join("|");

  return new RegExp(pattern, "g");
})();

const ROOT = exec("git rev-parse --show-toplevel").toString().trim();
const STATEDIR = `${ROOT}/.dev_queue_state`;
const INSTANCE_FILE = `${STATEDIR}/id`;
const CERT_FILE = `${STATEDIR}/cert.key`;
const HOST_FILE = `${STATEDIR}/host`;
const PORT_FILE = `${STATEDIR}/port`;
const IP_FILE = `${STATEDIR}/ip`;
const QUEUE_IP = fs.readFileSync(IP_FILE).toString().trim();
const USER_TOKEN_FILE = `${STATEDIR}/user.token`;
const USER_TOKEN = fs.readFileSync(USER_TOKEN_FILE).toString().trim();
const VERSION_FILE = `${STATEDIR}/version`;

const BATCH_SIZE = 14;
const EXPECTED_NUM_TESTS = 500;
const MAX_WORKER_OVERHEAD_PCT = 10; // 10%

function runBenchmark(
  {
    abqCmd,
    rawCmd,
    parseAbqRunTests,
    parseRawRunTests,
    parseAbqWorkTime,
    parseRawWorkTime,
    cwd,
  },
  debugReuse = false
) {
  debugReuse = debugReuse || !!process.env["BIGTEST_DEBUG"];
  abqOnly = !!process.env["ABQ_ONLY"];

  cwd = path.resolve(cwd);
  process.chdir(cwd);

  const outdir = `${cwd}/out`;

  if (!debugReuse) {
    fs.rmSync(outdir, { recursive: true, force: true });
    fs.mkdirSync(outdir);
  }

  const ABQ_BENCHMARK_OUT = `${outdir}/abq_benchmark_out.txt`;
  const RAW_BENCHMARK_OUT = `${outdir}/raw_benchmark_out.txt`;
  const EXPORTS = `${outdir}/exports`;
  const ABQENV = {
    ...process.env,
    ABQ_LOG: "abq=debug",
  };

  if (!debugReuse) {
    exec(
      `abq test \
          --worker 0 \
          --reporter dot \
          --run-id bigtest-${crypto.randomUUID()} \
          --batch-size ${BATCH_SIZE} \
          --queue-addr ${QUEUE_IP} \
          --token ${USER_TOKEN} \
          --tls-cert ${CERT_FILE} \
          -n 1 \
          -- ${abqCmd} \
        2>&1 | tee ${ABQ_BENCHMARK_OUT}`,
      { stdio: "inherit", env: ABQENV }
    );
    if (!abqOnly) {
      exec(`${rawCmd} 2>&1 | tee ${RAW_BENCHMARK_OUT}`, { stdio: "inherit" });
    }
  }

  const abq_out = fs
    .readFileSync(ABQ_BENCHMARK_OUT)
    .toString()
    .replace(RE_ANSI, "");

  if (abqOnly) {
    const abq_work_time = parseAbqWorkTime(abq_out);
    fs.writeFileSync(
      EXPORTS,
      `\
ABQ_WORKER_TIME="${abq_work_time}"
`
    );
    process.exit(0);
  }

  const raw_out = fs
    .readFileSync(RAW_BENCHMARK_OUT)
    .toString()
    .replace(RE_ANSI, "");

  {
    // Assert that the number of tests ABQ saw matches what the raw command saw.
    const abq_tests = parseAbqRunTests(abq_out);
    const raw_tests = parseRawRunTests(raw_out);
    let fail = false;
    if (abq_tests !== EXPECTED_NUM_TESTS) {
      console.error(
        `Expected ${EXPECTED_NUM_TESTS} tests run; ABQ recorded running ${abq_tests} tests`
      );
      fail = true;
    }
    if (raw_tests !== EXPECTED_NUM_TESTS) {
      console.error(
        `Expected ${EXPECTED_NUM_TESTS} tests run; raw command recorded running ${raw_tests} tests`
      );
      fail = true;
    }
    if (fail) {
      process.exit(1);
    }
  }

  {
    // Check the time delta
    // TODO: we should check the `abq test` wall time here as well.
    const abq_work_time = parseAbqWorkTime(abq_out);
    const raw_work_time = parseRawWorkTime(raw_out);
    console.assert(
      typeof abq_work_time === "number" && typeof raw_work_time === "number"
    );
    const abq_work_overhead_pct = (
      ((abq_work_time - raw_work_time) / raw_work_time) *
      100
    ).toFixed(2);

    console.log(
      `abq workers were ${abq_work_overhead_pct}% slower; threshold is ${MAX_WORKER_OVERHEAD_PCT}%`
    );

    fs.writeFileSync(
      EXPORTS,
      `\
ABQ_WORKER_TIME="${abq_work_time}"
RAW_TIME="${raw_work_time}"
ABQ_WORKER_OVERHEAD="${abq_work_overhead_pct}"
`
    );

    if (abq_work_overhead_pct > MAX_WORKER_OVERHEAD_PCT) {
      process.exit(1);
    }
  }
}

module.exports = { runBenchmark };
