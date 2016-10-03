# encoding: utf-8

module Kafka
  module Clients
    shared_context 'producer_consumer' do
      include_context 'config'

      let :consumer do
        Kafka::Clients::Consumer.new(config)
      end

      let :producer do
        Kafka::Clients::Producer.new(config)
      end
    end

    shared_context 'available_records' do
      include_context 'producer_consumer'

      let :rebalance_listener do
        listener = double(:rebalance_listener)
        assigned_partitions = []
        allow(listener).to receive(:assigned_partitions).and_return(assigned_partitions)
        allow(listener).to receive(:on_partitions_revoked)
        allow(listener).to receive(:on_partitions_assigned) do |partitions|
          assigned_partitions.concat(partitions)
        end
        listener
      end

      let :producer_records do
        Array.new(10) do |i|
          ProducerRecord.new(topic_names.first, sprintf('hello%d', i), sprintf('world%d', i))
        end
      end

      def send_records
        producer_records.each { |r| producer.send(r).get }
        producer.flush
      end

      before do
        send_records
        consumer.subscribe(topic_names, rebalance_listener)
        consumer.poll(0)
        if consumer.assignment.empty?
          raise 'No partitions assigned'
        end
      end

      after do
        producer.close rescue nil
        consumer.close rescue nil
      end
    end
  end
end
