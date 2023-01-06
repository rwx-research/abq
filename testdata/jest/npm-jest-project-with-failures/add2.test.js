const {add} = require('./add');

test('1 + 2', () => {
  console.log("hello from a second jest test");
  expect(add(1, 2)).toBe(4);
});
