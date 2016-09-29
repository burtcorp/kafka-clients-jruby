module Kafka
  module Clients
    shared_context 'producer_consumer' do
      let :consumer do
        described_class.new(config)
      end

      let :producer do
        Kafka::Clients::Producer.new(config)
      end

      let :consumer_id do
        (Time.now.to_f * 1000).to_i.to_s
      end

      let :config do
        {
          'bootstrap.servers' => 'localhost:19091',
          'group.id' => 'kafka-client-jruby-' << consumer_id,
        }
      end

      let :topic_names do
        %w[topic0 topic1].map { |t| t << '_' << consumer_id }
      end
    end

    shared_context 'available_records' do
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
    end
  end
end
