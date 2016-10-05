# encoding: utf-8

require 'set'

module Kafka
  module Clients
    describe Consumer do
      let :consumer do
        Java::IoBurtKafkaClients::ConsumerWrapper.create(JRuby.runtime, kafka_consumer)
      end

      let :kafka_consumer do
        double(:kafka_consumer)
      end

      shared_context 'kafka_mock_consumer' do
        let :kafka_consumer do
          Java::OrgApacheKafkaClientsConsumer::MockConsumer.new(Java::OrgApacheKafkaClientsConsumer::OffsetResetStrategy::NONE)
        end
      end

      describe '#close' do
        before do
          allow(kafka_consumer).to receive(:close)
        end

        it 'closes the consumer' do
          consumer.close
          expect(kafka_consumer).to have_received(:close)
        end
      end

      describe '#partitions_for' do
        include_context 'kafka_mock_consumer'

        let :kafka_partitions do
          nodes = [
            Java::OrgApacheKafkaCommon::Node.new(123, 'lolcathost', 1234, 'a'),
            Java::OrgApacheKafkaCommon::Node.new(234, 'lolcathost', 2345, 'b'),
            Java::OrgApacheKafkaCommon::Node.new(345, 'lolcathost', 3456, 'c'),
          ]
          [
            Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 0, nodes[0], nodes, [nodes[0], nodes[1]]),
            Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 1, nodes[1], nodes, [nodes[1], nodes[2]]),
            Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 2, nodes[2], nodes, [nodes[0], nodes[2]]),
          ]
        end

        before do
          kafka_consumer.update_partitions('toptopic', kafka_partitions)
        end

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
          let :kafka_partitions do
            []
          end

          it 'returns an empty enumerable' do
            expect(consumer.partitions_for('toptopic')).to be_empty
          end
        end
      end

      describe '#subscribe' do
        before do
          allow(kafka_consumer).to receive(:subscribe)
        end

        context 'when given a list of topic names' do
          it 'subscribes to the specified topics' do
            consumer.subscribe(%w[toptopic cipotpot])
            expect(kafka_consumer).to have_received(:subscribe).with(contain_exactly('toptopic', 'cipotpot'), anything)
          end
        end

        context 'when given a single string' do
          it 'subscribes to the topics matching the pattern' do
            pattern = nil
            allow(kafka_consumer).to receive(:subscribe) do |p, _|
              pattern = p
            end
            consumer.subscribe('toptopics.*')
            aggregate_failures do
              expect(pattern).to be_a(Java::JavaUtilRegex::Pattern)
              expect(pattern.to_s).to eq('toptopics.*')
            end
          end

          it 'raises ArgumentError when the string is not a valid regex' do
            expect { consumer.subscribe(')$') }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#unsubscribe' do
        before do
          allow(kafka_consumer).to receive(:unsubscribe)
        end

        it 'unsubscribes from all topics' do
          consumer.unsubscribe
          expect(kafka_consumer).to have_received(:unsubscribe)
        end
      end

      describe '#poll' do
        include_context 'kafka_mock_consumer'

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

        before do
          consumer.subscribe(%w[toptopic])
          kafka_consumer.rebalance(kafka_partitions)
          kafka_consumer.update_beginning_offsets(Hash[kafka_partitions.map { |tp| [tp, 0] }])
          consumer.seek_to_beginning(partitions)
        end

        context 'when there are records available' do
          before do
            10.times do |i|
              k = sprintf('hello%d', i).to_java(Java::OrgJrubyRuntimeBuiltin::IRubyObject)
              v = sprintf('world%d', i).to_java(Java::OrgJrubyRuntimeBuiltin::IRubyObject)
              r = Java::OrgApacheKafkaClientsConsumer::ConsumerRecord.new('toptopic', i % 3, i + 9, k, v)
              kafka_consumer.add_record(r)
            end
          end

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
          it 'returns an empty enumerable' do
            expect(consumer.poll(0)).to be_empty
          end
        end
      end

      describe '#pause' do
        let :kafka_partitions do
          [
            Java::OrgApacheKafkaCommon::TopicPartition.new('toptopic', 0),
            Java::OrgApacheKafkaCommon::TopicPartition.new('toptopic', 2),
          ]
        end

        let :partitions do
          kafka_partitions.map { |tp| TopicPartition.new(tp.topic, tp.partition) }
        end

        before do
          allow(kafka_consumer).to receive(:pause)
        end

        it 'stops fetching records for the specified partitions' do
          consumer.pause(partitions)
          expect(kafka_consumer).to have_received(:pause).with(contain_exactly(*kafka_partitions))
        end

        context 'with partitions that are not assigned to the consumer' do
          let :kafka_consumer do
            Java::OrgApacheKafkaClientsConsumer::MockConsumer.new(Java::OrgApacheKafkaClientsConsumer::OffsetResetStrategy::NONE)
          end

          it 'raises an ArgumentError' do
            consumer.assign(partitions.drop(1))
            expect { consumer.pause(partitions) }.to raise_error(ArgumentError)
          end
        end
      end

      describe '#paused' do
        include_context 'kafka_mock_consumer'

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
    end
  end
end
