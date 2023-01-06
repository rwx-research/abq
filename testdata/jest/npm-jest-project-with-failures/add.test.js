const {add} = require('./add');

test('mona + lisa', () => {
  console.log("hello from a first jest test");
  expect(add('mona ', 'lisa')).toBe('daVinci');
});
