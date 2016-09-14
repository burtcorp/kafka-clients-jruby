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
          'max.block.ms' => 5000,
        }
      end

      after do
        producer.close rescue nil
      end

      describe '#initialize' do
        it 'creates and configures the producer' do
          producer
        end

        context 'when given an empty config' do
          let :config do
            {}
          end

          it 'raises ConfigError' do
            expect { producer }.to raise_error(Kafka::Clients::ConfigError)
          end
        end

        context 'when given a config with symbolic keys' do
          it 'understands that :bootstrap_servers is an alias for bootstrap.servers' do
            config[:bootstrap_servers] = config.delete('bootstrap.servers')
            expect(producer.partitions_for('topictopic')).to_not be_empty
          end

          it 'allows :bootstrap_servers to be an array' do
            config[:bootstrap_servers] = [config.delete('bootstrap.servers')] * 3
            expect(producer.partitions_for('topictopic')).to_not be_empty
          end
        end
      end

      describe '#close' do
        it 'closes the producer' do
          producer.close(timeout: 5)
        end

        context 'when a custom partitioner is used' do
          let :config do
            super().merge(partitioner: partitioner)
          end

          let :partitioner do
            double(:partitioner)
          end

          before do
            allow(partitioner).to receive(:close)
          end

          it 'calls #close on the partitioner', pending: 'Partitioner#close doesn\'t seem to be called by KafkaProducer' do
            producer.close
            expect(partitioner).to have_received(:close)
          end
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

        it 'accepts a block that will be called when the record has been saved' do
          block_called = false
          future = producer.send('topictopic', 'hello', 'world') do
            block_called = true
          end
          future.get(timeout: 5)
          expect(block_called).to be_truthy
        end

        it 'passes the metadata to the block' do
          metadata = nil
          future = producer.send('topictopic', 'hello', 'world') do |md|
            metadata = md
          end
          future.get(timeout: 5)
          expect(metadata.topic).to eq('topictopic')
        end

        context 'when an error occurs during sending' do
          it 'returns a future that raises an error when resolved' do
            future = producer.send('topictopic', 'hello', '!' * (1024 * 1024 + 1))
            expect { future.get(timeout: 5) }.to raise_error(Kafka::Clients::RecordTooLargeError)
          end

          it 'yields the error to the block' do
            yielded_error = nil
            future = producer.send('topictopic', 'hello', '!' * (1024 * 1024 + 1)) do |_, error|
              yielded_error = error
            end
            future.get(timeout: 5) rescue nil
            expect(yielded_error).to be_a(Kafka::Clients::RecordTooLargeError)
          end
        end

        context 'when given a custom partitioner' do
          let :config do
            super().merge(partitioner: partitioner)
          end

          let :partitioner do
            double(:partitioner)
          end

          before do
            allow(partitioner).to receive(:partition).and_return(0)
            allow(partitioner).to receive(:close)
          end

          it 'uses the partitioner when sending records' do
            future = producer.send('topictopic', 'hello', 'world')
            future.get(timeout: 5)
            expect(partitioner).to have_received(:partition).with('topictopic', 'hello', 'world', instance_of(Cluster))
          end

          it 'passes a cluster object to the partitioner' do
            cluster = nil
            allow(partitioner).to receive(:partition) do |_, _, _, c|
              cluster = c
              0
            end
            future = producer.send('topictopic', 'hello', 'world')
            future.get(timeout: 5)
            partitions = producer.partitions_for('topictopic')
            partition = partitions.first
            aggregate_failures do
              expect(cluster.nodes).to_not be_empty
              expect(cluster.topics).to_not be_empty
              expect(cluster.unauthorized_topics).to be_an(Array)
              expect(cluster.bootstrap_configured?).to_not be_nil
              expect(cluster.node_by_id(partition.leader.id)).to eq(partition.leader)
              expect(cluster.leader_for(partition.topic, partition.partition)).to eq(partition.leader)
              expect(cluster.partition(partition.topic, partition.partition)).to eq(partition)
              expect(cluster.partitions_for_topic(partition.topic)).to eq(partitions)
              expect(cluster.available_partitions_for_topic(partition.topic)).to be_an(Array)
              expect(cluster.partitions_for_node(partition.leader.id)).to include(partition)
              expect(cluster.partition_count_for_topic(partition.topic)).to be_a(Fixnum)
            end
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
