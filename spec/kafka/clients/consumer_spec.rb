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
        let :assigned_partitions do
          []
        end

        before do
          listener = double(:listener)
          allow(listener).to receive(:on_partitions_revoked)
          allow(listener).to receive(:on_partitions_assigned) do |partitions|
            assigned_partitions.concat(partitions)
          end
          send_records
          consumer.subscribe(topic_names, listener)
          consumer.poll(0)
          if assigned_partitions.empty?
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
            consumer.seek_to_beginning(assigned_partitions)
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
        let :assigned_partitions do
          []
        end

        before do
          listener = double(:listener)
          allow(listener).to receive(:on_partitions_assigned) do |topic_partitions|
            assigned_partitions.concat(topic_partitions)
          end
          send_records
          consumer.subscribe(topic_names.take(1), listener)
          consumer.poll(0)
          if assigned_partitions.empty?
            raise 'No partitions assigned'
          end
        end

        context 'when given a topic name and partition' do
          it 'returns the offset of the next record offset for a topic and partition' do
            offset = consumer.position(assigned_partitions.first.topic, assigned_partitions.first.partition)
            expect(offset).to be_a(Fixnum)
          end
        end

        context 'when given a TopicPartition' do
          it 'returns the offset of the next record offset for a topic and partition' do
            offset = consumer.position(assigned_partitions.first)
            expect(offset).to be_a(Fixnum)
          end
        end

        context 'when given a partition not assigned to this consumer' do
          it 'raises ArgumentError' do
            tp = TopicPartition.new(assigned_partitions.first.topic, 99)
            expect { consumer.position(tp) }.to raise_error(ArgumentError, /can only check the position for partitions assigned to this consumer/)
          end
        end
      end

      describe '#seek_to_beginning' do
        include_context 'available_records'

        it 'sets the offset to the earliest available' do
          consumer.seek_to_beginning(assigned_partitions)
          positions = assigned_partitions.map { |tp| consumer.position(tp) }
          expect(positions).to all(be_zero)
        end

        it 'makes #poll return the first available records' do
          consumer.seek_to_beginning(assigned_partitions)
          consumer_records = consumer.poll(1)
          expect(consumer_records.map(&:key)).to include('hello0')
        end
      end
    end
  end
end
