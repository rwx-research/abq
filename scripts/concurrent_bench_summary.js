const fs = require('fs');
const stdin = fs.readFileSync(0, 'utf-8');

const data = JSON.parse(stdin);

const rawTime = process.argv[2];

const overheads = data.map(rcd => Number((((rcd.abq - rawTime) / rawTime) * 100).toFixed(2)));
overheads.sort((a, b) => a - b);

function stddev(times) {
  const avg = times.reduce((a, b) => a + b) / times.length;
  const variances = times.map(x => (x - avg) ** 2);
  return Math.sqrt(variances.reduce((a, b) => a + b) / times.length)
}

console.log(`MAX_OVERHEAD="${overheads[overheads.length - 1]}"`);
console.log(`MIN_OVERHEAD="${overheads[0]}"`);
console.log(`STDDEV="${stddev(overheads).toFixed(2)}"`);
