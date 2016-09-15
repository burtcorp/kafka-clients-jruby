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
          'group.id' => sprintf('kafka-client-jruby-%d', (Time.now.to_f * 1000).to_i),
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

      describe '#subscribe' do
        it 'subscribes the consumer to the specified topics' do
          consumer.subscribe(%w[foo bar baz])
        end

        it 'subscribes the consumer to all topics matching a pattern' do
          consumer.subscribe('foo\..*')
        end

        it 'registers a rebalance listener', pending: 'there is currently no way to trigger the callbacks' do
          listener = double(:listener)
          allow(listener).to receive(:on_partitions_assigned)
          allow(listener).to receive(:on_partitions_revoked)
          consumer.subscribe('foo\..*', listener)
          expect(listener).to have_received(:on_partitions_assigned)
          expect(listener).to have_received(:on_partitions_revoked)
        end
      end

      describe '#unsubscribe' do
        it 'unsubscribes the consumer' do
          consumer.subscribe(%w[foo bar baz])
          consumer.unsubscribe
        end
      end

      describe '#poll' do
        it 'retrieves a batch of messages' do
          consumer.subscribe(%w[foo bar])
          records = consumer.poll(0)
          expect(records).to_not be_nil
        end

        context 'when not given a group ID' do
          let :config do
            c = super()
            c.delete('group.id')
            c
          end

          it 'raises InvalidGroupIdError' do
            consumer.subscribe(%w[foo bar])
            expect { consumer.poll(0) }.to raise_error(InvalidGroupIdError)
          end
        end
      end

      describe '#commit_sync' do
        it 'synchronously commits the offsets received from the last call to #poll' do
          consumer.commit_sync
        end

        it 'synchronously commits specific offsets' do
          offsets = {
            TopicPartition.new('foo', 2) => OffsetAndMetadata.new(12, 'hello world'),
            TopicPartition.new('bar', 0) => OffsetAndMetadata.new(24, 'hello world'),
          }
          consumer.commit_sync(offsets)
        end
      end
    end
  end
end
