# encoding: utf-8

require 'set'

module Kafka
  module Clients
    describe Consumer do
      include_context 'producer_consumer'

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

      describe '#subscription' do
        context 'before the consumer subscribes' do
          it 'returns an empty enumerable' do
            subscription = consumer.subscription
            expect(subscription).to be_empty
          end
        end

        context 'after the consumer has subscribed' do
          context 'to explicit of topics' do
            it 'returns the topics that the consumer is subscribed to' do
              consumer.subscribe(topic_names)
              subscription = consumer.subscription
              expect(subscription).to contain_exactly(*topic_names)
            end
          end

          context 'to a topic pattern' do
            it 'returns an empty enumerable' do
              consumer.subscribe("#{topic_names.first[0..-2]}.*")
              subscription = consumer.subscription
              expect(subscription).to be_empty
            end
          end
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
              expect(consumer_records.first.topic).to eq(topic_names.first)
              expect(consumer_records.first.partition).to be_a(Fixnum)
              expect(consumer_records.first.offset).to be_a(Fixnum)
              expect(consumer_records.first.checksum).to be_a(Fixnum)
              expect(consumer_records.first.key).to match(/\Ahello\d+\Z/)
              expect(consumer_records.first.value).to match(/\Aworld\d+\Z/)
              expect(consumer_records.first.timestamp).to be_a(Time)
              expect(consumer_records.first.timestamp_type).to eq(:create_time)
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
        include_context 'available_records'

        context 'without arguments' do
          it 'synchronously commits the offsets received from the last call to #poll' do
            consumer.poll(0)
            consumer.commit_sync
          end
        end

        context 'with arguments' do
          it 'synchronously commits specific offsets' do
            records = consumer.poll(0)
            offsets = {}
            records.each do |record|
              tp = TopicPartition.new(record.topic, record.partition)
              om = offsets[tp] ||= OffsetAndMetadata.new(0, '')
              if record.offset > om.offset
                offsets[tp] = OffsetAndMetadata.new(om.offset, '')
              end
            end
            consumer.commit_sync(offsets)
          end
        end
      end

      describe '#commit_async' do
        context 'without arguments' do
          it 'asynchronously commits the offsets received from the last call to #poll' do
            consumer.commit_async
          end

          context 'with a callback' do
            it 'asynchronously commits the offsets received from the last call to #poll and then calls the callback' do
              committed_offsets = nil
              consumer.commit_async do |offsets, _|
                committed_offsets = offsets
              end
              expect(committed_offsets).to_not be_nil
            end
          end
        end

        context 'with arguments' do
          let :offsets do
            records = consumer.poll(0)
            offsets = {}
            records.each do |record|
              tp = TopicPartition.new(record.topic, record.partition)
              om = offsets[tp] ||= OffsetAndMetadata.new(0, '')
              if record.offset > om.offset
                offsets[tp] = OffsetAndMetadata.new(om.offset, '')
              end
            end
            offsets
          end

          it 'asynchronously commits specific offsets' do
            consumer.commit_async(offsets)
          end

          context 'with a callback' do
            it 'asynchronously commits the offsets and then calls the callback' do
              rendez_vous = Java::JavaUtilConcurrent::SynchronousQueue.new
              committed_offsets = nil
              consumer.commit_async(offsets) do |offsets, _|
                committed_offsets = offsets
              end
              expect(committed_offsets).to_not be_nil
            end

            context 'and there is an error' do
              it 'passes the error as the second argument to the callback' do
                rendez_vous = Java::JavaUtilConcurrent::SynchronousQueue.new
                error = nil
                consumer.commit_async({}) do |_, e|
                  error = e
                end
                expect(error).to be_a(Kafka::Clients::ApiError)
              end
            end
          end
        end
      end

      describe '#position' do
        include_context 'available_records'

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

      describe '#pause' do
        include_context 'available_records'

        it 'no longer fetches records for the specified partitions' do
          consumer.pause(consumer.assignment)
          consumer_records = consumer.poll(1)
          expect(consumer_records).to be_empty
        end
      end

      describe '#paused' do
        context 'when the consumer has not been paused' do
          it 'returns an empty enumerable' do
            paused = consumer.paused
            expect(paused).to be_empty
          end
        end

        context 'when the consumer has been paused' do
          include_context 'available_records'

          it 'returns the paused partitions' do
            assignment = consumer.assignment
            consumer.pause(assignment.dup)
            paused = consumer.paused
            expect(paused).to contain_exactly(*assignment)
          end
        end
      end

      describe '#resume' do
        include_context 'available_records'

        it 'unpauses paused partitions' do
          assignment = consumer.assignment
          consumer.pause(assignment.dup)
          consumer.resume(assignment.dup)
          paused = consumer.paused
          expect(paused).to be_empty
        end
      end

      describe '#wakeup' do
        it 'makes #poll throw a WakeupError' do
          poller_thread = Thread.start do
            consumer.subscribe('foo.*')
            consumer.poll(5)
          end
          consumer.wakeup
          expect { poller_thread.value }.to raise_error(WakeupError)
        end
      end
    end
  end
end
