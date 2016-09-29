# encoding: utf-8

require 'set'

module Kafka
  module Clients
    describe Consumer do
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

      let :producer_records do
        Array.new(10) do |i|
          ProducerRecord.new(topic_names.first, sprintf('hello%d', i), sprintf('world%d', i))
        end
      end

      def send_records
        producer_records.each { |r| producer.send(r).get }
        producer.flush
      end

      after do
        producer.close rescue nil
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

        before do
          send_records
          consumer.subscribe(topic_names, rebalance_listener)
          consumer.poll(0)
          if consumer.assignment.empty?
            raise 'No partitions assigned'
          end
        end
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
          consumer.subscribe(topic_names)
        end

        it 'subscribes the consumer to the topics in an Enumerable' do
          consumer.subscribe(Set.new(topic_names))
        end

        it 'subscribes the consumer to all topics matching a pattern' do
          consumer.subscribe('foo\\..*')
        end

        it 'registers a rebalance listener' do
          listener = double(:listener)
          allow(listener).to receive(:on_partitions_assigned)
          allow(listener).to receive(:on_partitions_revoked)
          consumer.subscribe('foo\\..*', listener)
          consumer.poll(0)
          expect(listener).to have_received(:on_partitions_assigned)
          expect(listener).to have_received(:on_partitions_revoked)
        end

        context 'when given something that is not a String or Enumerable' do
          it 'raises TypeError' do
            expect { consumer.subscribe(/foo|bar/) }.to raise_error(TypeError, /Enumerable of topics or topic pattern/)
          end
        end
      end

      describe '#unsubscribe' do
        it 'unsubscribes the consumer' do
          consumer.subscribe(topic_names)
          consumer.unsubscribe
        end
      end

      describe '#poll' do
        context 'when there are records available' do
          include_context 'available_records'

          it 'returns an enumerable of records' do
            consumer.seek_to_beginning(consumer.assignment)
            consumer_records = consumer.poll(1)
            aggregate_failures do
              expect(consumer_records.count).to eq(10)
              expect(consumer_records).to be_an(Enumerable)
              expect(consumer_records.each).to be_an(Enumerable)
              expect(consumer_records.each.first).to be_a(ConsumerRecord)
              expect(consumer_records.first).to be_a(ConsumerRecord)
            end
          end
        end

        context 'when there are no records available' do
          it 'returns an empty enumerable' do
            consumer_records = consumer.poll(0)
            expect(consumer_records).to be_empty
          end
        end

        context 'when not given a group ID' do
          let :config do
            c = super()
            c.delete('group.id')
            c
          end

          it 'raises InvalidGroupIdError' do
            consumer.subscribe(topic_names)
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
            TopicPartition.new(topic_names[0], 2) => OffsetAndMetadata.new(12, 'hello world'),
            TopicPartition.new(topic_names[1], 0) => OffsetAndMetadata.new(24, 'hello world'),
          }
          consumer.commit_sync(offsets)
        end
      end

      describe '#position' do
        before do
          send_records
          consumer.subscribe(topic_names.take(1))
          consumer.poll(0)
          if consumer.assignment.empty?
            raise 'No partitions assigned'
          end
        end

        context 'when given a topic name and partition' do
          it 'returns the offset of the next record offset for a topic and partition' do
            topic_partition = consumer.assignment.first
            offset = consumer.position(topic_partition.topic, topic_partition.partition)
            expect(offset).to be_a(Fixnum)
          end
        end

        context 'when given a TopicPartition' do
          it 'returns the offset of the next record offset for a topic and partition' do
            offset = consumer.position(consumer.assignment.first)
            expect(offset).to be_a(Fixnum)
          end
        end

        context 'when given a partition not assigned to this consumer' do
          it 'raises ArgumentError' do
            tp = TopicPartition.new(consumer.assignment.first.topic, 99)
            expect { consumer.position(tp) }.to raise_error(ArgumentError, /can only check the position for partitions assigned to this consumer/)
          end
        end
      end

      describe '#seek_to_beginning' do
        include_context 'available_records'

        it 'sets the offset to the earliest available' do
          consumer.seek_to_beginning(consumer.assignment)
          positions = consumer.assignment.map { |tp| consumer.position(tp) }
          expect(positions).to all(be_zero)
        end

        it 'makes #poll return the first available records' do
          consumer.seek_to_beginning(consumer.assignment)
          consumer_records = consumer.poll(1)
          expect(consumer_records.map(&:key)).to include('hello0')
        end
      end

      describe '#seek_to_end' do
        include_context 'available_records'

        it 'sets the offset to the latest available' do
          consumer.seek_to_end(consumer.assignment)
          positions = consumer.assignment.map { |tp| consumer.position(tp) }
          expect(positions.reduce(:+)).to eq(10)
        end

        it 'makes #poll not return any previous records' do
          consumer.seek_to_end(consumer.assignment)
          consumer_records = consumer.poll(1)
          expect(consumer_records).to be_empty
        end
      end

      describe '#seek' do
        include_context 'available_records'

        context 'when given a TopicPartition' do
          it 'seeks to the specified offset' do
            positions = consumer.assignment.map { |tp| consumer.position(tp) }
            max_offset = positions.max
            max_partition = consumer.assignment[positions.index(max_offset)]
            consumer.seek(max_partition, 1)
            consumer_records = consumer.poll(1)
            expect(consumer_records.count).to eq(max_offset - 1)
          end
        end

        context 'when given a topic and a partition' do
          it 'seeks to the specified offset' do
            positions = consumer.assignment.map { |tp| consumer.position(tp) }
            max_offset = positions.max
            max_partition = consumer.assignment[positions.index(max_offset)]
            consumer.seek(max_partition.topic, max_partition.partition, 1)
            consumer_records = consumer.poll(1)
            expect(consumer_records.count).to eq(max_offset - 1)
          end
        end
      end

      describe '#assignment' do
        context 'before assignment' do
          it 'returns an empty enumerable' do
            assignment = consumer.assignment
            aggregate_failures do
              expect(assignment).to be_empty
              expect(assignment.count).to eq(0)
              expect(assignment).to be_a(Enumerable)
            end
          end
        end

        context 'after subscription' do
          include_context 'available_records'

          it 'returns an enumerable of TopicPartitions' do
            assignment = consumer.assignment
            aggregate_failures do
              expect(assignment).not_to be_empty
              expect(assignment.count).to be > 0
              expect(assignment).to be_a(Enumerable)
              expect(assignment.first).to be_a(TopicPartition)
            end
          end

          it 'returns the same partitions as is given to the rebalance listener' do
            expect(consumer.assignment).to contain_exactly(*rebalance_listener.assigned_partitions)
          end
        end

        context 'after manual assignment' do
          it 'returns an enumerable of TopicPartitions' do
            topic_partitions = topic_names.map { |name| TopicPartition.new(name, 0) }
            consumer.assign(topic_partitions)
            expect(consumer.assignment.to_a).to contain_exactly(*topic_partitions)
          end
        end
      end

      describe '#assign' do
        it 'explicitly assigns partitions to the consumer' do
          topic_partitions = topic_names.map { |name| TopicPartition.new(name, 0) }
          consumer.assign(topic_partitions)
          expect(consumer.assignment.to_a).to contain_exactly(*topic_partitions)
        end
      end

      describe '#list_topics' do
        include_context 'available_records'

        it 'returns information on the available topics and their partitions' do
          topics = consumer.list_topics
          partitions = topics[topic_names.first]
          expect(partitions[0]).to be_a(PartitionInfo)
        end
      end
    end
  end
end
