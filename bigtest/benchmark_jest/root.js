// NOTE keep in sync with rspec benchmarks
const TIMEOUT_MS = 35;

function sleepTest() {
  return new Promise(resolve => setTimeout(resolve, TIMEOUT_MS));
}

module.exports = { sleepTest };
