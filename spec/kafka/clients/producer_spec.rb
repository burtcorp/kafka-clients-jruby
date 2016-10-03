# encoding: utf-8

module Kafka
  module Clients
    describe Producer do
      let :producer do
        Java::IoBurtKafkaClients::ProducerWrapper.create(JRuby.runtime, mock_producer)
      end

      let :mock_producer do
        serializer = Java::IoBurtKafkaClients::RubyStringSerializer.new
        Java::OrgApacheKafkaClientsProducer::MockProducer.new(complete_sends_immediately, serializer, serializer)
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
    end
  end
end
