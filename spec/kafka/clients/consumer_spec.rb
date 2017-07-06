# encoding: utf-8

require 'set'

module Kafka
  module Clients
    describe Consumer do
      let :consumer do
        Java::IoBurtKafkaClients::ConsumerWrapper.create(JRuby.runtime, kafka_consumer)
      end

      let :kafka_consumer do
        Java::OrgApacheKafkaClientsConsumer::MockConsumer.new(Java::OrgApacheKafkaClientsConsumer::OffsetResetStrategy::EARLIEST)
      end

      shared_context 'topics' do
        let :kafka_partition_infos do
          nodes = [
            Java::OrgApacheKafkaCommon::Node.new(123, 'lolcathost', 1234, 'a'),
            Java::OrgApacheKafkaCommon::Node.new(234, 'lolcathost', 2345, 'b'),
            Java::OrgApacheKafkaCommon::Node.new(345, 'lolcathost', 3456, 'c'),
          ]
          {
            'toptopic' => [
              Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 0, nodes[0], nodes, [nodes[0], nodes[1]]),
              Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 1, nodes[1], nodes, [nodes[1], nodes[2]]),
              Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 2, nodes[2], nodes, [nodes[0], nodes[2]]),
            ],
            'tiptopic' => [
              Java::OrgApacheKafkaCommon::PartitionInfo.new('tiptopic', 0, nodes[0], nodes, [nodes[0], nodes[1]]),
              Java::OrgApacheKafkaCommon::PartitionInfo.new('tiptopic', 1, nodes[1], nodes, [nodes[1], nodes[2]]),
              Java::OrgApacheKafkaCommon::PartitionInfo.new('tiptopic', 2, nodes[2], nodes, [nodes[0], nodes[2]]),
            ]
          }
        end

        before do
          kafka_partition_infos.each do |topic_name, partition_infos|
            kafka_consumer.update_partitions(topic_name, partition_infos)
          end
        end
      end

      shared_context 'records' do
        let :kafka_records do
          Array.new(10) do |i|
            k = sprintf('hello%d', i).to_java(Java::OrgJrubyRuntimeBuiltin::IRubyObject)
            v = sprintf('world%d', i).to_java(Java::OrgJrubyRuntimeBuiltin::IRubyObject)
            Java::OrgApacheKafkaClientsConsumer::ConsumerRecord.new('toptopic', i % 3, i + 9, k, v)
          end
        end

        let :kafka_partitions do
          kafka_records.map { |r| Java::OrgApacheKafkaCommon::TopicPartition.new(r.topic, r.partition) }.uniq
        end

        let :partitions do
          kafka_partitions.map { |tp| TopicPartition.new(tp.topic, tp.partition) }
        end

        let :auto_init_subscription do
          true
        end

        let :auto_add_records do
          true
        end

        def init_subscription(options={})
          if (topics = options[:subscribe])
            consumer.subscribe(topics)
            kafka_consumer.rebalance(kafka_partitions)
          elsif (assignment = options[:assign])
            consumer.assign(assignment)
          end
          kafka_consumer.update_beginning_offsets(Hash[kafka_partitions.map { |tp| [tp, 0] }])
          if (assignment = options[:assign])
            consumer.seek_to_beginning(assignment)
          else
            consumer.seek_to_beginning(partitions)
          end
        end

        def add_records
          end_offsets = Hash.new(0)
          kafka_records.each do |record|
            if !block_given? || yield(record)
              kafka_consumer.add_record(record)
              tp = Java::OrgApacheKafkaCommon::TopicPartition.new(record.topic, record.partition)
              next_offset = record.offset + 1
              end_offsets[tp] = next_offset if end_offsets[tp] < next_offset
            end
          end
          kafka_consumer.update_end_offsets(end_offsets)
        end

        before do
          if auto_init_subscription
            init_subscription(subscribe: partitions.map(&:topic).uniq)
          end
          if auto_add_records
            add_records
          end
        end
      end

      describe '#close' do
        it 'closes the consumer' do
          consumer.close
          expect(kafka_consumer.closed).to be_truthy
        end
      end

      describe '#partitions_for' do
        include_context 'topics'

        it 'returns partitions for a topic' do
          partitions = consumer.partitions_for('toptopic')
          partition = partitions.find { |pi| pi.partition == 1 }
          aggregate_failures do
            expect(partition.topic).to eq('toptopic')
            expect(partition.partition).to eq(1)
            expect(partition.leader.host).to eq('lolcathost')
            expect(partition.leader.port).to eq(2345)
            expect(partition.leader.id).to eq(234)
            expect(partition.leader).to have_rack
            expect(partition.leader.rack).to eq('b')
            expect(partition.leader).to_not be_empty
            expect(partition.replicas.map(&:id)).to contain_exactly(123, 234, 345)
            expect(partition.in_sync_replicas.map(&:id)).to contain_exactly(234, 345)
          end
        end

        context 'when there are no partitions' do
          let :kafka_partition_infos do
            {}
          end

          it 'returns an empty enumerable' do
            expect(consumer.partitions_for('toptopic')).to be_empty
          end
        end
      end

      describe '#list_topics' do
        include_context 'topics'

        context 'returns a hash that' do
          it 'has the available topics\' names as keys' do
            expect(consumer.list_topics.keys).to contain_exactly('toptopic', 'tiptopic')
          end

          it 'has info about the topics\' partitions as values' do
            partitions = consumer.list_topics['tiptopic']
            partition = partitions.find { |pi| pi.partition == 1 }
            aggregate_failures do
              expect(partitions.size).to eq(3)
              expect(partition.topic).to eq('tiptopic')
              expect(partition.partition).to eq(1)
              expect(partition.leader.host).to eq('lolcathost')
              expect(partition.leader.port).to eq(2345)
              expect(partition.leader.id).to eq(234)
              expect(partition.leader).to have_rack
              expect(partition.leader.rack).to eq('b')
              expect(partition.leader).to_not be_empty
              expect(partition.replicas.map(&:id)).to contain_exactly(123, 234, 345)
              expect(partition.in_sync_replicas.map(&:id)).to contain_exactly(234, 345)
            end
          end
        end

        context 'when there are no topics' do
          let :kafka_partition_infos do
            {}
          end

          it 'returns an empty hash' do
            expect(consumer.list_topics).to be_empty
          end
        end
      end

      describe '#subscribe' do
        context 'when given a list of topic names' do
          it 'subscribes to the specified topics' do
            consumer.subscribe(%w[toptopic cipotpot])
            expect(kafka_consumer.subscription).to contain_exactly('toptopic', 'cipotpot')
          end
        end

        context 'when given a single string' do
          include_context 'topics'

          it 'subscribes to the topics matching the pattern' do
            consumer.subscribe('t.ptopic')
            expect(consumer.subscription).to contain_exactly('toptopic', 'tiptopic')
          end

          it 'raises ArgumentError when the string is not a valid regex' do
            expect { consumer.subscribe(')$') }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#unsubscribe' do
        it 'unsubscribes from all topics' do
          consumer.subscribe(%w[toptopic cipotpot])
          consumer.unsubscribe
          expect(kafka_consumer.subscription).to be_empty
        end
      end

      describe '#poll' do
        include_context 'records'

        context 'when there are records available' do
          it 'returns an enumerable of records' do
            consumer_records = consumer.poll(0)
            record = consumer_records.find { |r| r.offset == 12 }
            aggregate_failures do
              expect(consumer_records.size).to eq(10)
              expect(record.topic).to eq('toptopic')
              expect(record.offset).to eq(12)
              expect(record.checksum).to be_a(Fixnum)
              expect(record.key).to eq('hello3')
              expect(record.value).to eq('world3')
              expect(record.timestamp).to be_a(Time)
              expect(record.timestamp_type).to eq(:no_timestamp_type)
            end
          end
        end

        context 'when there are no records available' do
          let :kafka_records do
            []
          end

          it 'returns an empty enumerable' do
            expect(consumer.poll(0)).to be_empty
          end
        end
      end

      describe '#pause' do
        let :kafka_partitions do
          [
            Java::OrgApacheKafkaCommon::TopicPartition.new('toptopic', 0),
            Java::OrgApacheKafkaCommon::TopicPartition.new('toptopic', 1),
            Java::OrgApacheKafkaCommon::TopicPartition.new('toptopic', 2),
          ]
        end

        let :partitions do
          kafka_partitions.map { |tp| TopicPartition.new(tp.topic, tp.partition) }
        end

        it 'stops fetching records for the specified partitions' do
          consumer.assign(partitions)
          consumer.pause(partitions.drop(1))
          expect(kafka_consumer.paused).to contain_exactly(*kafka_partitions.drop(1))
        end

        context 'with partitions that are not assigned to the consumer' do

          it 'raises an ArgumentError' do
            consumer.assign(partitions.drop(1))
            expect { consumer.pause(partitions) }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#paused' do
        let :partitions do
          [
            TopicPartition.new('toptopic', 0),
            TopicPartition.new('toptopic', 1),
            TopicPartition.new('toptopic', 2),
          ]
        end

        it 'returns the paused partitions' do
          consumer.assign(partitions)
          consumer.pause(partitions.drop(2))
          expect(consumer.paused).to contain_exactly(*partitions.drop(2))
        end

        it 'returns an empty enumerable when there are no paused partitions' do
          expect(consumer.paused).to be_empty
        end
      end

      describe '#resume' do
        let :kafka_partitions do
          [
            Java::OrgApacheKafkaCommon::TopicPartition.new('toptopic', 0),
            Java::OrgApacheKafkaCommon::TopicPartition.new('toptopic', 1),
            Java::OrgApacheKafkaCommon::TopicPartition.new('toptopic', 2),
          ]
        end

        let :partitions do
          kafka_partitions.map { |tp| TopicPartition.new(tp.topic, tp.partition) }
        end

        it 'starts fetching records for the specified partitions' do
          consumer.assign(partitions)
          consumer.pause(partitions.drop(1))
          consumer.resume(partitions.drop(2))
          expect(kafka_consumer.paused).to contain_exactly(kafka_partitions[1])
        end

        context 'with partitions that are not assigned to the consumer' do
          it 'raises an ArgumentError' do
            consumer.assign(partitions.drop(1))
            consumer.pause(partitions.drop(1))
            expect { consumer.resume(partitions) }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#commit_sync' do
        include_context 'records'

        context 'without arguments' do
          it 'synchronously commits the offsets received from the last call to #poll' do
            consumer_records = consumer.poll(0)
            offsets = Hash[consumer_records.group_by(&:partition).map { |p, ps| [p, ps.map(&:offset).max] }]
            consumer.commit_sync
            aggregate_failures do
              expect(kafka_consumer.committed(kafka_partitions[0]).offset).to eq(offsets[0] + 1)
              expect(kafka_consumer.committed(kafka_partitions[1]).offset).to eq(offsets[1] + 1)
              expect(kafka_consumer.committed(kafka_partitions[2]).offset).to eq(offsets[2] + 1)
            end
          end
        end

        context 'with arguments' do
          it 'synchronously commits specific offsets' do
            consumer_records = consumer.poll(0)
            offsets = {
              TopicPartition.new('toptopic', 0) => OffsetAndMetadata.new(0),
              TopicPartition.new('toptopic', 1) => OffsetAndMetadata.new(1),
              TopicPartition.new('toptopic', 2) => OffsetAndMetadata.new(2**33),
            }
            consumer.commit_sync(offsets)
            aggregate_failures do
              expect(kafka_consumer.committed(kafka_partitions[0]).offset).to eq(0)
              expect(kafka_consumer.committed(kafka_partitions[1]).offset).to eq(1)
              expect(kafka_consumer.committed(kafka_partitions[2]).offset).to eq(2**33)
            end
          end

          context 'and the offsets as integers' do
            it 'synchronously commits specific offsets' do
              consumer_records = consumer.poll(0)
              offsets = {
                TopicPartition.new('toptopic', 0) => 0,
                TopicPartition.new('toptopic', 1) => 1,
                TopicPartition.new('toptopic', 2) => 4,
              }
              consumer.commit_sync(offsets)
              aggregate_failures do
                expect(kafka_consumer.committed(kafka_partitions[0]).offset).to eq(0)
                expect(kafka_consumer.committed(kafka_partitions[1]).offset).to eq(1)
                expect(kafka_consumer.committed(kafka_partitions[2]).offset).to eq(4)
              end
            end
          end
        end
      end

      describe '#commit_async' do
        include_context 'records'

        context 'without arguments' do
          it 'synchronously commits the offsets received from the last call to #poll' do
            consumer_records = consumer.poll(0)
            offsets = Hash[consumer_records.group_by(&:partition).map { |p, ps| [p, ps.map(&:offset).max] }]
            consumer.commit_async
            aggregate_failures do
              expect(kafka_consumer.committed(kafka_partitions[0]).offset).to eq(offsets[0] + 1)
              expect(kafka_consumer.committed(kafka_partitions[1]).offset).to eq(offsets[1] + 1)
              expect(kafka_consumer.committed(kafka_partitions[2]).offset).to eq(offsets[2] + 1)
            end
          end

          context 'when given a block' do
            it 'yields when the offsets have successfully been committed' do
              called = false
              consumer.commit_async do
                called = true
              end
              expect(called).to be_truthy
            end
          end
        end

        context 'with arguments' do
          let :offsets do
            {
              TopicPartition.new('toptopic', 0) => OffsetAndMetadata.new(0),
              TopicPartition.new('toptopic', 1) => OffsetAndMetadata.new(1),
              TopicPartition.new('toptopic', 2) => OffsetAndMetadata.new(4),
            }
          end

          it 'synchronously commits specific offsets' do
            consumer.commit_async(offsets)
            aggregate_failures do
              expect(kafka_consumer.committed(kafka_partitions[0]).offset).to eq(0)
              expect(kafka_consumer.committed(kafka_partitions[1]).offset).to eq(1)
              expect(kafka_consumer.committed(kafka_partitions[2]).offset).to eq(4)
            end
          end

          context 'when given a block' do
            it 'yields the committed offsets to the block when they have successfully been committed' do
              committed_offsets = nil
              consumer.commit_async(offsets) do |offsets|
                committed_offsets = offsets
              end
              expect(committed_offsets).to contain_exactly(*offsets)
            end
          end

          context 'and the offsets as integers' do
            let :offsets do
              {
                TopicPartition.new('toptopic', 0) => 0,
                TopicPartition.new('toptopic', 1) => 1,
                TopicPartition.new('toptopic', 2) => 4,
              }
            end

            it 'synchronously commits specific offsets' do
              consumer.commit_async(offsets)
              aggregate_failures do
                expect(kafka_consumer.committed(kafka_partitions[0]).offset).to eq(0)
                expect(kafka_consumer.committed(kafka_partitions[1]).offset).to eq(1)
                expect(kafka_consumer.committed(kafka_partitions[2]).offset).to eq(4)
              end
            end

            context 'when given a block' do
              it 'yields the committed offsets to the block when they have successfully been committed' do
                committed_offsets = nil
                consumer.commit_async(offsets) do |offsets|
                  committed_offsets = offsets
                end
                expect(committed_offsets.values.map(&:offset)).to contain_exactly(*offsets.values)
              end
            end
          end
        end
      end

      describe '#committed' do
        include_context 'records'

        before do
          consumer.poll(0)
          consumer.commit_sync
        end

        it 'returns the committed offsets' do
          expect(consumer.committed(TopicPartition.new('toptopic', 0)).offset).to eq(19)
          expect(consumer.committed(TopicPartition.new('toptopic', 1)).offset).to eq(17)
          expect(consumer.committed(TopicPartition.new('toptopic', 2)).offset).to eq(18)
        end
      end

      describe '#position' do
        include_context 'records'

        before do
          consumer.poll(0)
          consumer.commit_sync
        end

        context 'when given a topic name and partition' do
          it 'returns the offset of the next record for that topic and partition' do
            offset = consumer.position('toptopic', 1)
            expect(offset).to eq(17)
          end
        end

        context 'when given a TopicPartition' do
          it 'returns the offset of the next record for that topic and partition' do
            offset = consumer.position(TopicPartition.new('toptopic', 1))
            expect(offset).to eq(17)
          end
        end

        context 'when given a partition not assigned to this consumer' do
          it 'raises ArgumentError' do
            partition = TopicPartition.new('toptopic', 99)
            expect { consumer.position(partition) }.to raise_error(ArgumentError, /can only check the position for partitions assigned to this consumer/)
          end
        end
      end

      describe '#seek_to_beginning' do
        include_context 'records'

        context 'when given no arguments', pending: 'This is not supported by MockConsumer' do
          it 'seeks to the first available offset for all assigned partitions' do
            consumer.poll(0)
            consumer.commit_sync
            consumer.seek_to_beginning
            aggregate_failures do
              expect(consumer.position('toptopic', 0)).to eq(0)
              expect(consumer.position('toptopic', 1)).to eq(0)
              expect(consumer.position('toptopic', 2)).to eq(0)
            end
          end
        end

        context 'when given a list of partitions' do
          it 'seeks to the first available offset for each partition' do
            consumer.poll(0)
            consumer.commit_sync
            consumer.seek_to_beginning([TopicPartition.new('toptopic', 0), TopicPartition.new('toptopic', 2)])
            aggregate_failures do
              expect(consumer.position('toptopic', 0)).to eq(0)
              expect(consumer.position('toptopic', 1)).to eq(17)
              expect(consumer.position('toptopic', 2)).to eq(0)
            end
          end
        end

        context 'when seeking with a partition not assigned to the consumer' do
          it 'raises ArgumentError' do
            expect { consumer.seek_to_beginning([TopicPartition.new('toptopic', 99)]) }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#seek_to_end' do
        include_context 'records'

        context 'when given no arguments', pending: 'This is not supported by MockConsumer' do
          it 'seeks to the last offset for all assigned partitions' do
            consumer.seek_to_end
            consumer_records = consumer.poll(0)
            aggregate_failures do
              expect(consumer_records).to be_empty
              expect(consumer.position('toptopic', 0)).to eq(19)
              expect(consumer.position('toptopic', 1)).to eq(17)
              expect(consumer.position('toptopic', 2)).to eq(18)
            end
          end
        end

        context 'when given a list of partitions' do
          it 'seeks to the last offset for each partition' do
            consumer.seek_to_end([TopicPartition.new('toptopic', 0), TopicPartition.new('toptopic', 2)])
            aggregate_failures do
              expect(consumer.position('toptopic', 0)).to eq(19)
              expect(consumer.position('toptopic', 1)).to eq(0)
              expect(consumer.position('toptopic', 2)).to eq(18)
            end
          end
        end

        context 'when seeking with a partition not assigned to the consumer' do
          it 'raises ArgumentError' do
            expect { consumer.seek_to_end([TopicPartition.new('toptopic', 99)]) }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#seek' do
        context 'when given a TopicPartition' do
          it 'seeks to the specified offset' do
            partition = TopicPartition.new('toptopic', 1)
            consumer.assign([partition])
            consumer.seek(partition, 14)
            expect(consumer.position(partition)).to eq(14)
          end
        end

        context 'when given a topic and a partition' do
          it 'seeks to the specified offset' do
            partition = TopicPartition.new('toptopic', 1)
            consumer.assign([partition])
            consumer.seek(partition.topic, partition.partition, 14)
            expect(consumer.position(partition)).to eq(14)
          end
        end

        context 'when seeking with a partition not assigned to the consumer' do
          it 'raises ArgumentError' do
            expect { consumer.seek('toptopic', 99, 14) }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#assign' do
        include_context 'records'

        let :auto_init_subscription do
          false
        end

        let :auto_add_records do
          false
        end

        it 'tells the consumer which partitions to fetch' do
          assignment = partitions.take(2)
          init_subscription(assign: assignment)
          partitions = assignment.map(&:partition)
          add_records { |r| partitions.include?(r.partition) }
          consumer_records = consumer.poll(0)
          aggregate_failures do
            expect(consumer_records.size).to eq(4 + 3)
            expect(consumer_records).to all(satisfy { |r| r.partition == 0 || r.partition == 1 })
          end
        end

        context 'when calling #assign after #subscribe' do
          it 'raises ArgumentError' do
            consumer.subscribe(%w[toptopic])
            expect { consumer.assign(partitions.take(2)) }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#assignment' do
        include_context 'records'

        context 'when subscribed' do
          it 'returns the partitions assigned to the consumer' do
            expect(consumer.assignment).to contain_exactly(*partitions)
          end
        end

        context 'when manually assigning partitions' do
          let :auto_init_subscription do
            false
          end

          let :auto_add_records do
            false
          end

          it 'returns the partitions assigned to the consumer' do
            assignment = partitions.take(2)
            init_subscription(assign: assignment)
            expect(consumer.assignment).to contain_exactly(*assignment)
          end
        end
      end

      describe '#wakeup' do
        it 'will raise WakeupError in the thread calling #poll' do
          consumer.wakeup
          expect { consumer.poll(0) }.to raise_error(WakeupError)
        end
      end
    end
  end
end
