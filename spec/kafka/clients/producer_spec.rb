# encoding: utf-8

module Kafka
  module Clients
    describe Producer do
      let :producer do
        described_class.new(config)
      end

      let :config do
        {
          'bootstrap.servers' => 'localhost:19091',
          'max.block.ms' => '5000',
        }
      end

      after do
        producer.close rescue nil
      end

      describe '#initialize' do
        it 'creates and configures the producer' do
          producer
        end

        context 'when created with an empty config' do
          let :config do
            {}
          end

          it 'raises ConfigError' do
            expect { producer }.to raise_error(Kafka::Clients::ConfigError)
          end
        end
      end

      describe '#close' do
        it 'closes the producer' do
          producer.close(timeout: 5)
        end
      end

      describe '#send' do
        it 'sends a message to Kafka' do
          future = producer.send('topic', 'hello', 'world')
          future.get(timeout: 5)
        end
      end

      describe '#flush' do
        it 'flushes' do
          producer.flush
        end
      end
    end
  end
end
