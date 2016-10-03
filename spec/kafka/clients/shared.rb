# encoding: utf-8

module Kafka
  module Clients
    shared_context 'config' do
      let :consumer_id do
        (Time.now.to_f * 1000).to_i.to_s
      end

      let :config do
        {
          'bootstrap.servers' => 'localhost:19091',
          'group.id' => 'kafka-client-jruby-' << consumer_id,
          'max.block.ms' => 5000,
        }
      end

      let :topic_names do
        %w[topic0 topic1].map { |t| t << '_' << consumer_id }
      end
    end
  end
end
