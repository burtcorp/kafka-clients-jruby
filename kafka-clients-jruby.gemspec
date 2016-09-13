# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'kafka/clients/version'

Gem::Specification.new do |s|
  s.name = 'kafka-clients-jruby'
  s.version = Kafka::Clients::VERSION
  s.author = 'Theo Hultberg'
  s.email = 'theo@burtcorp.com'
  s.summary = 'Kafka clients for JRuby'
  s.description = 'Native JRuby wrappers of the Kafka producer and consumer clients'
  s.platform = 'java'

  s.files = Dir['lib/**/*.rb', 'lib/**/*.jar', 'README.md', '.yardopts']
end
