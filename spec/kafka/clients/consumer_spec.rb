# encoding: utf-8

require 'set'

module Kafka
  module Clients
    describe Consumer do
      let :consumer do
        Java::IoBurtKafkaClients::ConsumerWrapper.create(JRuby.runtime, mock_consumer)
      end

      let :mock_consumer do
        Java::OrgApacheKafkaClientsConsumer::MockConsumer.new(Java::OrgApacheKafkaClientsConsumer::OffsetResetStrategy::NONE)
      end

      describe '#close' do
        let :mock_consumer do
          double(:mock_consumer, close: nil)
        end

        it 'closes the consumer' do
          consumer.close
          expect(mock_consumer).to have_received(:close)
        end
      end
    end
  end
end
