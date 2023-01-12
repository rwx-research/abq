const { runBenchmark } = require("../benchmark_harness");
const cwd = __dirname;

runBenchmark({
  abqCmd: `npm test`,
  rawCmd: `npm test`,
  parseAbqRunTests: (s) => {
    return Number(/^(\d+) tests, 0 failures/gm.exec(s)[1]);
  },
  parseRawRunTests: (s) => {
    return Number(/^Tests:       500 passed, (\d+) total/gm.exec(s)[1]);
  },
  parseAbqWorkTime: (s) => {
    return Number(/^Time:        (\d+(\.\d+)?) s/gm.exec(s)[1]);
  },
  parseRawWorkTime: (s) => {
    return Number(/^Time:        (\d+(\.\d+)?) s/gm.exec(s)[1]);
  },
  cwd,
});
