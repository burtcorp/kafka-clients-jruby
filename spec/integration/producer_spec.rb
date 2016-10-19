# encoding: utf-8

module Kafka
  module Clients
    describe Producer do
      include_context 'producer_consumer'

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
            expect(producer.partitions_for(topic_names.first)).to_not be_empty
          end

          it 'allows :bootstrap_servers to be an array' do
            config[:bootstrap_servers] = [config.delete('bootstrap.servers')] * 3
            expect(producer.partitions_for(topic_names.first)).to_not be_empty
          end
        end

        context 'when given a key serializer that does not implement #serialize or #call' do
          let :config do
            super().merge(key_serializer: double(:bad_serializer))
          end

          it 'raises an error' do
            expect { producer }.to raise_error(Kafka::Clients::KafkaError)
          end
        end

        context 'when given a value serializer that does not implement #serialize or #call' do
          let :config do
            super().merge(value_serializer: double(:bad_serializer))
          end

          it 'raises an error' do
            expect { producer }.to raise_error(Kafka::Clients::KafkaError)
          end
        end

        context 'when given a partitioner that does not implement #partition or #call' do
          let :config do
            super().merge(partitioner: double(:bad_partitioner))
          end

          it 'raises an error' do
            expect { producer }.to raise_error(Kafka::Clients::KafkaError)
          end
        end
      end

      describe '#close' do
        context 'with a custom key serializer' do
          let :config do
            super().merge(key_serializer: serializer)
          end

          let :serializer do
            double(:serializer, close: nil, serialize: nil)
          end

          it 'calls #close on the serializer' do
            producer.close
            expect(serializer).to have_received(:close)
          end

          context 'but the serializer does not implement #close' do
            let :serializer do
              double(:serializer, serialize: nil)
            end

            it 'does not call #close on the serializer' do
              expect { producer.close }.to_not raise_error
            end
          end
        end

        context 'with a custom value serializer' do
          let :config do
            super().merge(value_serializer: serializer)
          end

          let :serializer do
            double(:serializer, close: nil, serialize: nil)
          end

          it 'calls #close on the serializer' do
            producer.close
            expect(serializer).to have_received(:close)
          end

          context 'but the serializer does not implement #close' do
            let :serializer do
              double(:serializer, serialize: nil)
            end

            it 'does not call #close on the serializer' do
              expect { producer.close }.to_not raise_error
            end
          end
        end

        context 'with a custom partitioner' do
          let :config do
            super().merge(partitioner: partitioner)
          end

          let :partitioner do
            double(:partitioner, close: nil, partition: nil)
          end

          it 'calls #close on the partitioner', pending: 'KafkaProducer never calls #close on partitioners' do
            producer.close
            expect(partitioner).to have_received(:close)
          end

          context 'but the partitioner does not implement #close' do
            let :partitioner do
              double(:partitioner, partition: nil)
            end

            it 'does not call #close on the partitioner' do
              expect { producer.close }.to_not raise_error
            end
          end
        end
      end

      describe '#send' do
        def consume_records(topic_name_or_partition)
          case topic_name_or_partition
          when TopicPartition
            consumer.assign([topic_name_or_partition])
          else
            consumer.subscribe([topic_name_or_partition])
            consumer.poll(0)
          end
          consumer.seek_to_beginning(consumer.assignment)
          consumer.poll(1)
        end

        it 'sends a message to Kafka' do
          future = producer.send(topic_names.first, 'hello world')
          future.get(timeout: 5)
          consumer_records = consume_records(topic_names.first)
          aggregate_failures do
            expect(consumer_records.count).to eq(1)
            expect(consumer_records.first.value).to eq('hello world')
          end
        end

        it 'sends a message with both a key and a value' do
          future = producer.send(topic_names.first, 'hello', 'world')
          future.get(timeout: 5)
          consumer_records = consume_records(topic_names.first)
          aggregate_failures do
            expect(consumer_records.count).to eq(1)
            expect(consumer_records.first.key).to eq('hello')
            expect(consumer_records.first.value).to eq('world')
          end
        end

        it 'sends a pre-partitioned message' do
          future = producer.send(topic_names.first, 0, 'hello', 'world')
          future.get(timeout: 5)
          consumer_records = consume_records(TopicPartition.new(topic_names.first, 0))
          aggregate_failures do
            expect(consumer_records.count).to eq(1)
            expect(consumer_records.first.key).to eq('hello')
            expect(consumer_records.first.value).to eq('world')
          end
        end

        it 'sends a message with a specified timestamp (as a Time)' do
          t = Time.now
          future = producer.send(topic_names.first, 0, t, 'hello', 'world')
          future.get(timeout: 5)
          consumer_records = consume_records(topic_names.first)
          aggregate_failures do
            expect(consumer_records.count).to eq(1)
            expect(consumer_records.first.timestamp).to eq(t)
            expect(consumer_records.first.timestamp_type).to eq(:create_time)
          end
        end

        it 'sends a message with a specified timestamp (as float)' do
          t = Time.now
          future = producer.send(topic_names.first, 0, t.to_f, 'hello', 'world')
          future.get(timeout: 5)
          consumer_records = consume_records(topic_names.first)
          aggregate_failures do
            expect(consumer_records.count).to eq(1)
            expect(consumer_records.first.timestamp).to eq(t)
            expect(consumer_records.first.timestamp_type).to eq(:create_time)
          end
        end

        it 'sends a message with a specified timestamp (as an integer)' do
          t = Time.now
          future = producer.send(topic_names.first, 0, t.to_i, 'hello', 'world')
          future.get(timeout: 5)
          consumer_records = consume_records(topic_names.first)
          aggregate_failures do
            expect(consumer_records.count).to eq(1)
            expect(consumer_records.first.timestamp.to_i).to eq(t.to_i)
            expect(consumer_records.first.timestamp_type).to eq(:create_time)
          end
        end

        it 'sends a message with a pre-created ProducerRecord' do
          t = Time.now
          future = producer.send(ProducerRecord.new(topic_names.first, 0, t, 'hello', 'world'))
          future.get(timeout: 5)
          consumer_records = consume_records(TopicPartition.new(topic_names.first, 0))
          aggregate_failures do
            expect(consumer_records.count).to eq(1)
            expect(consumer_records.first.topic).to eq(topic_names.first)
            expect(consumer_records.first.partition).to eq(0)
            expect(consumer_records.first.key).to eq('hello')
            expect(consumer_records.first.value).to eq('world')
            expect(consumer_records.first.timestamp).to eq(t)
            expect(consumer_records.first.timestamp_type).to eq(:create_time)
          end
        end

        context 'when the key is nil' do
          it 'serializes and deserializes it correctly' do
            future = producer.send(topic_names.first, nil, 'world')
            future.get(timeout: 5)
            consumer_records = consume_records(topic_names.first)
            aggregate_failures do
              expect(consumer_records.first.key).to be_nil
            end
          end
        end

        context 'when the value is nil' do
          it 'serializes and deserializes it correctly' do
            future = producer.send(topic_names.first, 'hello', nil)
            future.get(timeout: 5)
            consumer_records = consume_records(topic_names.first)
            aggregate_failures do
              expect(consumer_records.first.value).to be_nil
            end
          end
        end

        it 'returns a future that resolves to a record metadata object describing the saved record' do
          metadata = producer.send(topic_names.first, 'hello', 'world').get(timeout: 5)
          aggregate_failures do
            expect(metadata.offset).to be_a(Fixnum)
            expect(metadata.partition).to be_a(Fixnum)
            expect(metadata.timestamp).to be_within(5).of(Time.now)
            expect(metadata.topic).to eq(topic_names.first)
            expect(metadata.checksum).to be_a(Fixnum)
            expect(metadata.serialized_key_size).to be_a(Fixnum)
            expect(metadata.serialized_value_size).to be_a(Fixnum)
          end
        end

        it 'accepts a block that will be called when the record has been saved' do
          block_called = false
          future = producer.send(topic_names.first, 'hello', 'world') do
            block_called = true
          end
          future.get(timeout: 5)
          expect(block_called).to be_truthy
        end

        it 'passes the metadata to the block' do
          metadata = nil
          future = producer.send(topic_names.first, 'hello', 'world') do |md|
            metadata = md
          end
          future.get(timeout: 5)
          expect(metadata.topic).to eq(topic_names.first)
        end

        context 'when specifying a partition that does not exist' do
          it 'raises ArgumentError' do
            expect { producer.send(topic_names.first, 99, 'hello', 'world') }.to raise_error(ArgumentError, /99 is not in the range/)
          end
        end

        context 'when an error occurs during sending' do
          it 'returns a future that raises an error when resolved' do
            future = producer.send(topic_names.first, 'hello', '!' * (1024 * 1024 + 1))
            expect { future.get(timeout: 5) }.to raise_error(Kafka::Clients::RecordTooLargeError)
          end

          it 'yields the error to the block' do
            yielded_error = nil
            future = producer.send(topic_names.first, 'hello', '!' * (1024 * 1024 + 1)) do |_, error|
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

          shared_examples 'partitioner' do
            let :partitioner do
              double(:partitioner)
            end

            before do
              allow(partitioner).to receive(partition_method_name).and_return(0)
            end

            it 'uses the partitioner when sending records' do
              future = producer.send(topic_names.first, 'hello', 'world')
              future.get(timeout: 5)
              expect(partitioner).to have_received(partition_method_name).with(topic_names.first, 'hello', 'world', instance_of(Cluster))
            end

            it 'does not use the partitioner when sending a pre-partitioned record' do
              future = producer.send(topic_names.first, 1, 'hello', 'world')
              future.get(timeout: 5)
              expect(partitioner).to_not have_received(partition_method_name)
            end

            it 'passes a cluster object to the partitioner' do
              cluster = nil
              allow(partitioner).to receive(partition_method_name) do |_, _, _, c|
                cluster = c
                0
              end
              future = producer.send(topic_names.first, 'hello', 'world')
              future.get(timeout: 5)
              partitions = producer.partitions_for(topic_names.first)
              partition = partitions.first
              aggregate_failures do
                expect(cluster.nodes).to_not be_empty
                expect(cluster.topics).to_not be_empty
                expect(cluster.unauthorized_topics).to be_an(Array)
                expect(cluster.bootstrap_configured?).to_not be_nil
                expect(cluster.node_by_id(partition.leader.id)).to eq(partition.leader)
                expect(cluster.leader_for(partition.topic, partition.partition)).to eq(partition.leader)
                expect(cluster.leader_for(TopicPartition.new(partition.topic, partition.partition))).to eq(partition.leader)
                expect(cluster.partition(TopicPartition.new(partition.topic, partition.partition))).to eq(partition)
                expect(cluster.partitions_for_topic(partition.topic)).to eq(partitions)
                expect(cluster.available_partitions_for_topic(partition.topic)).to be_an(Array)
                expect(cluster.partitions_for_node(partition.leader.id)).to include(partition)
                expect(cluster.partition_count_for_topic(partition.topic)).to be_a(Fixnum)
              end
            end
          end

          context 'that implements #partition' do
            include_examples 'partitioner' do
              let :partition_method_name do
                :partition
              end
            end
          end

          context 'that implements #call' do
            include_examples 'partitioner' do
              let :partition_method_name do
                :call
              end
            end
          end
        end

        context 'when given a custom key serializer' do
          let :config do
            super().merge(key_serializer: serializer)
          end

          context 'that implements #serialize' do
            let :serializer do
              double(:serializer)
            end

            before do
              allow(serializer).to receive(:serialize) { |s| s.reverse }
            end

            it 'uses the serializer to serialize keys' do
              future = producer.send(topic_names.first, 'hello', 'world')
              future.get(timeout: 5)
              consumer_records = consume_records(topic_names.first)
              aggregate_failures do
                expect(consumer_records.first.key).to eq('olleh')
              end
            end
          end

          context 'that implements #call' do
            let :serializer do
              lambda { |s| s.reverse }
            end

            it 'uses the serializer to serialize keys' do
              future = producer.send(topic_names.first, 'hello', 'world')
              future.get(timeout: 5)
              consumer_records = consume_records(topic_names.first)
              aggregate_failures do
                expect(consumer_records.first.key).to eq('olleh')
              end
            end
          end
        end

        context 'when given a custom value serializer' do
          let :config do
            super().merge(value_serializer: serializer)
          end

          context 'that implements #serialize' do
            let :serializer do
              double(:serializer)
            end

            before do
              allow(serializer).to receive(:serialize) { |s| s.reverse }
            end

            it 'uses the serializer to serialize values' do
              future = producer.send(topic_names.first, 'hello', 'world')
              future.get(timeout: 5)
              consumer_records = consume_records(topic_names.first)
              aggregate_failures do
                expect(consumer_records.first.value).to eq('dlrow')
              end
            end
          end

          context 'that implements #call' do
            let :serializer do
              proc { |s| s.reverse }
            end

            it 'uses the serializer to serialize values' do
              future = producer.send(topic_names.first, 'hello', 'world')
              future.get(timeout: 5)
              consumer_records = consume_records(topic_names.first)
              aggregate_failures do
                expect(consumer_records.first.value).to eq('dlrow')
              end
            end
          end
        end
      end

      describe '#partitions_for' do
        it 'returns partitions for a topic' do
          partitions = producer.partitions_for(topic_names.first)
          partition = partitions.first
          aggregate_failures do
            expect(partition.topic).to eq(topic_names.first)
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
