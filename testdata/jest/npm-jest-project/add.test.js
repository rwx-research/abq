const {add} = require('./add');

test('mona + lisa', () => {
  expect(add('mona ', 'lisa')).toBe('mona lisa');
});
