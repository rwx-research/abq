const {names} = require('./names');

test('three names', () => {
  expect(names().length).toBe(3);
});
