require "bundler"
Bundler.require

RSpec.describe do
  500.times do |i|
    it do
      sleep 0.035
    end
  end
end
