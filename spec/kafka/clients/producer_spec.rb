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
          future = producer.send('topictopic', 'hello', 'world')
          future.get(timeout: 5)
        end

        it 'returns a future that resolves to a record metadata object describing the saved record' do
          metadata = producer.send('topictopic', 'hello', 'world').get(timeout: 5)
          aggregate_failures do
            expect(metadata.offset).to be_a(Fixnum)
            expect(metadata.partition).to be_a(Fixnum)
            expect(metadata.timestamp).to be_within(5).of(Time.now)
            expect(metadata.topic).to eq('topictopic')
            expect(metadata.checksum).to be_a(Fixnum)
            expect(metadata.serialized_key_size).to be_a(Fixnum)
            expect(metadata.serialized_value_size).to be_a(Fixnum)
          end
        end
      end

      describe '#flush' do
        it 'flushes' do
          producer.flush
        end
      end

      describe '#partitions_for' do
        it 'returns partitions for a topic' do
          partitions = producer.partitions_for('topictopic')
          partition = partitions.first
          aggregate_failures do
            expect(partition.topic).to eq('topictopic')
            expect(partition.partition).to be_a(Fixnum)
            expect(partition.leader.host).to be_a(String)
            expect(partition.leader.port).to be_a(Fixnum)
            expect(partition.leader.id).to be_a(Fixnum)
            expect(partition.leader).to_not have_rack
            expect(partition.leader.rack).to be_nil
            expect(partition.leader).to_not be_empty
            expect(partition.replicas).to include(partition.leader)
            expect(partition.in_sync_replicas).to include(partition.leader)
          end
        end
      end
    end
  end
end
