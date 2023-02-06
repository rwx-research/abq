const { runBenchmark } = require("../benchmark_harness");
const cwd = __dirname;

runBenchmark({
  abqCmd: `bundle exec rspec ${cwd}/benchmark_spec.rb`,
  rawCmd: `bundle exec rspec ${cwd}/benchmark_spec.rb`,
  parseAbqRunTests: (s) => {
    return Number(/^(\d+) tests, 0 failures/gm.exec(s)[1]);
  },
  parseRawRunTests: (s) => {
    return Number(/^(\d+) examples, 0 failures/gm.exec(s)[1]);
  },
  parseAbqWorkTime: (s) => {
    return Number(/^Finished in (\d+(\.\d+)?) seconds \(.* seconds spent/gm.exec(s)[1]);
  },
  parseRawWorkTime: (s) => {
    return Number(/^Finished in (\d+(\.\d+)?) seconds \(files/gm.exec(s)[1]);
  },
  cwd,
});
