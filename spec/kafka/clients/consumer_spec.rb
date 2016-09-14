# encoding: utf-8

module Kafka
  module Clients
    describe Consumer do
      let :consumer do
        described_class.new(config)
      end

      let :config do
        {
          'bootstrap.servers' => 'localhost:19091',
        }
      end

      after do
        producer.close rescue nil
      end

      describe '#initialize' do
        it 'creates and configures the consumer' do
          consumer
        end

        context 'when given an empty config' do
          let :config do
            {}
          end

          it 'raises ConfigError' do
            expect { consumer }.to raise_error(Kafka::Clients::ConfigError)
          end
        end

        context 'when given a config with symbolic keys' do
          it 'understands that :bootstrap_servers is an alias for bootstrap.servers' do
            config[:bootstrap_servers] = config.delete('bootstrap.servers')
            consumer
          end

          it 'allows :bootstrap_servers to be an array' do
            config[:bootstrap_servers] = [config.delete('bootstrap.servers')] * 3
            consumer
          end
        end
      end

      describe '#close' do
        it 'closes the consumer' do
          consumer.close
        end
      end
    end
  end
end
