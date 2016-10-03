# encoding: utf-8

module Kafka
  module Clients
    describe Producer do
      let :producer do
        Java::IoBurtKafkaClients::ProducerWrapper.create(JRuby.runtime, mock_producer)
      end

      let :mock_producer do
        partitioner = Java::OrgApacheKafkaClientsProducerInternals::DefaultPartitioner.new
        serializer = Java::IoBurtKafkaClients::RubyStringSerializer.new
        Java::OrgApacheKafkaClientsProducer::MockProducer.new(cluster, complete_sends_immediately, partitioner, serializer, serializer)
      end

      let :cluster do
        Java::OrgApacheKafkaCommon::Cluster.empty
      end

      let :complete_sends_immediately do
        true
      end

      describe '#send' do
        let :time do
          Time.utc(2016, 10, 3, 10, 27)
        end

        it 'sends a message to Kafka' do
          producer.send('toptopic', 'hello world').get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          aggregate_failures do
            expect(producer_records.size).to eq(1)
            expect(producer_records.first.value).to eq('hello world')
          end
        end

        it 'sends a message with both a key and a value' do
          future = producer.send('toptopic', 'hello', 'world').get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          aggregate_failures do
            expect(producer_records.size).to eq(1)
            expect(producer_records.first.key).to eq('hello')
            expect(producer_records.first.value).to eq('world')
          end
        end

        it 'sends a message a pre-partitioned message' do
          future = producer.send('toptopic', 0, 'hello', 'world').get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          aggregate_failures do
            expect(producer_records.size).to eq(1)
            expect(producer_records.first.key).to eq('hello')
            expect(producer_records.first.value).to eq('world')
          end
        end

        it 'sends a message with a specified timestamp (as a Time)' do
          future = producer.send('toptopic', 0, time, 'hello', 'world').get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          aggregate_failures do
            expect(producer_records.size).to eq(1)
            expect(producer_records.first.timestamp).to eq(time)
          end
        end

        it 'sends a message with a specified timestamp (as float)' do
          future = producer.send('toptopic', 0, time.to_f, 'hello', 'world').get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          aggregate_failures do
            expect(producer_records.size).to eq(1)
            expect(producer_records.first.timestamp).to eq(time)
          end
        end

        it 'sends a message with a specified timestamp (as an integer)' do
          future = producer.send('toptopic', 0, time.to_i, 'hello', 'world').get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          aggregate_failures do
            expect(producer_records.size).to eq(1)
            expect(producer_records.first.timestamp.to_i).to eq(time.to_i)
          end
        end

        it 'sends a message with a pre-created ProducerRecord (with the timestamp as a Time)' do
          producer_record = ProducerRecord.new('toptopic', 0, time, 'hello', 'world')
          future = producer.send(producer_record).get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          expect(producer_records).to contain_exactly(producer_record)
        end

        it 'sends a message with a pre-created ProducerRecord (with the timestamp as a float)' do
          producer_record = ProducerRecord.new('toptopic', 0, time.to_f, 'hello', 'world')
          future = producer.send(producer_record).get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          expect(producer_records).to contain_exactly(producer_record)
        end

        it 'sends a message with a pre-created ProducerRecord (with the timestamp as an integer)' do
          producer_record = ProducerRecord.new('toptopic', 0, time.to_i, 'hello', 'world')
          future = producer.send(producer_record).get
          producer_records = mock_producer.history.map { |pr| to_producer_record(pr) }
          expect(producer_records).to contain_exactly(producer_record)
        end

        context 'returns a future that' do
          let :complete_sends_immediately do
            false
          end

          it 'resolves when the record has been saved' do
            future = producer.send('toptopic', 0, time, 'hello', 'world')
            expect(future).to_not be_done
            mock_producer.complete_next
            expect(future).to be_done
          end

          it 'resolves to a record metadata object describing the saved record' do
            future = producer.send('toptopic', 0, time, 'hello', 'world')
            mock_producer.complete_next
            metadata = future.get
            aggregate_failures do
              expect(metadata.offset).to be_a(Fixnum)
              expect(metadata.partition).to eq(0)
              expect(metadata.timestamp).to be_a(Time)
              expect(metadata.topic).to eq('toptopic')
              expect(metadata.checksum).to be_a(Fixnum)
              expect(metadata.serialized_key_size).to be_a(Fixnum)
              expect(metadata.serialized_value_size).to be_a(Fixnum)
            end
          end
        end

        it 'accepts a block that will be called when the record has been saved' do
          block_called = false
          future = producer.send('toptopic', 'hello', 'world') do
            block_called = true
          end
          future.get
          expect(block_called).to be_truthy
        end

        it 'passes a record metadata object describing the saved record to the block' do
          metadata = nil
          future = producer.send('toptopic', 'hello', 'world') do |md|
            metadata = md
          end
          future.get
          expect(metadata.topic).to eq('toptopic')
        end
      end

      describe '#flush' do
        let :complete_sends_immediately do
          false
        end

        it 'waits until all sends have completed' do
          f1 = producer.send('toptopic', 'hello')
          f2 = producer.send('toptopic', 'world')
          aggregate_failures do
            expect(f1).to_not be_done
            expect(f2).to_not be_done
          end
          producer.flush
          aggregate_failures do
            expect(f1).to be_done
            expect(f2).to be_done
          end
        end
      end

      describe '#partitions_for' do
        let :cluster do
          nodes = [
            Java::OrgApacheKafkaCommon::Node.new(123, 'lolcathost', 1234, 'a'),
            Java::OrgApacheKafkaCommon::Node.new(234, 'lolcathost', 2345, 'b'),
            Java::OrgApacheKafkaCommon::Node.new(345, 'lolcathost', 3456, 'c'),
          ]
          partitions = [
            Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 0, nodes[0], nodes, [nodes[0], nodes[1]]),
            Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 1, nodes[1], nodes, [nodes[1], nodes[2]]),
            Java::OrgApacheKafkaCommon::PartitionInfo.new('toptopic', 2, nodes[2], nodes, [nodes[0], nodes[2]]),
          ]
          Java::OrgApacheKafkaCommon::Cluster.new(nodes, partitions, [])
        end

        it 'returns partitions for a topic' do
          partitions = producer.partitions_for('toptopic')
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

        context 'when the cluster is empty' do
          let :cluster do
            Java::OrgApacheKafkaCommon::Cluster.empty
          end

          it 'returns an empty enumerable' do
            expect(producer.partitions_for('toptopic')).to be_empty
          end
        end
      end
    end
  end
end
