require "bundler"
Bundler.require

RSpec.describe do
  5_000.times do |i|
    it do
      expect(false).to eq(true), "y" * rand(50_000)
    end
  end
end
