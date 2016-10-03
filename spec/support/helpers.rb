# encoding: utf-8

module Helpers
  def to_producer_record(java_producer_record)
    Java::IoBurtKafkaClients::ProducerRecordWrapper.create(JRuby.runtime, java_producer_record)
  end
end

RSpec.configure do |c|
  c.include(Helpers)
end