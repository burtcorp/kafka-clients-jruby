# encoding: utf-8

module Kafka
  describe Clients do
    it 'dynamically generates error classes matching those in Kafka' do
      aggregate_failures do
        expect(described_class::AuthorizationError.ancestors).to include(described_class::KafkaError)
        expect(described_class::BufferExhaustedError.ancestors).to include(described_class::KafkaError)
        expect(described_class::CommitFailedError.ancestors).to include(described_class::KafkaError)
        expect(described_class::ConfigError.ancestors).to include(described_class::KafkaError)
        expect(described_class::InvalidOffsetError.ancestors).to include(described_class::KafkaError)
      end
    end

    it 'makes ApiExceptions subclasses of ApiError' do
      aggregate_failures do
        expect(described_class::AuthorizationError.ancestors).to include(described_class::ApiError)
        expect(described_class::UnsupportedVersionError.ancestors).to include(described_class::ApiError)
      end
    end

    it 'makes RetriableExceptions subclasses of RetriableError' do
      aggregate_failures do
        expect(described_class::DisconnectError.ancestors).to include(described_class::RetriableError)
        expect(described_class::InvalidMetadataError.ancestors).to include(described_class::RetriableError)
      end
    end

    it 'does not generate an error class when there is no corresponding error in Kafka' do
      expect { described_class::FuzzBazzError }.to raise_error(NameError)
    end

    context 'when converting configuration' do
      let :kafka_config do
        Hash[Java::IoBurtKafkaClients::KafkaClientsLibrary.to_kafka_configuration(config)]
      end

      let :config do
        {}
      end

      context 'and :bootstrap_servers is used' do
        let :config do
          {:bootstrap_servers => 'lolcathost:4444'}
        end

        it 'changes the property to "bootstrap.servers"' do
          expect(kafka_config).to include('bootstrap.servers' => 'lolcathost:4444')
        end

        context 'and the value is an array' do
          let :config do
            {:bootstrap_servers => %w[lolcathost:4444 example.com:5555]}
          end

          it 'joins the array with comma' do
            expect(kafka_config).to include('bootstrap.servers' => 'lolcathost:4444,example.com:5555')
          end
        end
      end

      context 'and bootstrap.servers is used' do
        context 'and the value is an array' do
          let :config do
            {'bootstrap.servers' => %w[lolcathost:4444 example.com:5555]}
          end

          it 'joins the array with comma' do
            expect(kafka_config).to include('bootstrap.servers' => 'lolcathost:4444,example.com:5555')
          end
        end
      end

      context 'and :group_id is used' do
        let :config do
          {:group_id => 'kafkatron'}
        end

        it 'changes the property to "group.id"' do
          expect(kafka_config).to include('group.id' => 'kafkatron')
        end
      end

      context 'and :acks is used' do
        let :config do
          {:acks => 4}
        end

        it 'changes the property to "acks" and the value to a string' do
          expect(kafka_config).to include('acks' => '4')
        end
      end

      context 'and :retries is used' do
        let :config do
          {:retries => 3}
        end

        it 'changes the property to "retries"' do
          expect(kafka_config).to include('retries' => 3)
        end
      end

      context 'and :compression_type is used' do
        let :config do
          {:compression_type => 'lz4'}
        end

        it 'changes the property to "compression.type"' do
          expect(kafka_config).to include('compression.type' => 'lz4')
        end
      end

      context 'and :batch_size is used' do
        let :config do
          {:batch_size => 123}
        end

        it 'changes the property to "batch.size"' do
          expect(kafka_config).to include('batch.size' => 123)
        end
      end

      context 'and :client_id is used' do
        let :config do
          {:client_id => 'me'}
        end

        it 'changes the property to "client.id"' do
          expect(kafka_config).to include('client.id' => 'me')
        end

        context 'when the value is not a string' do
          let :config do
            {:client_id => 123}
          end

          it 'converts it to a string' do
            expect(kafka_config).to include('client.id' => '123')
          end
        end
      end

      context 'and :linger is used' do
        let :config do
          {:linger => 12.3}
        end

        it 'changes the property to "linger.ms" and the value to milliseconds' do
          expect(kafka_config).to include('linger.ms' => 12_300)
        end
      end

      context 'and :max_block is used' do
        let :config do
          {:max_block => 12.3}
        end

        it 'changes the property to "max.block.ms" and the value to milliseconds' do
          expect(kafka_config).to include('max.block.ms' => 12_300)
        end
      end

      context 'and :max_request_size is used' do
        let :config do
          {:max_request_size => 123}
        end

        it 'changes the property to "max_request_size"' do
          expect(kafka_config).to include('max.request.size' => 123)
        end
      end

      context 'and :max_poll_records is used' do
        let :config do
          {:max_poll_records => 123}
        end

        it 'changes the property to "max.poll.records"' do
          expect(kafka_config).to include('max.poll.records' => 123)
        end
      end

      context 'and :request_timeout is used' do
        let :config do
          {:request_timeout => 12.3}
        end

        it 'changes the property to "request.timeout.ms" and the value to milliseconds' do
          expect(kafka_config).to include('request.timeout.ms' => 12_300)
        end
      end

      context 'and :auto_commit is used' do
        let :config do
          {:auto_commit => false}
        end

        it 'changes the property to "enable.auto.commit"' do
          expect(kafka_config).to include('enable.auto.commit' => false)
        end
      end

      context 'and :auto_commit_interval is used' do
        let :config do
          {:auto_commit_interval => 12.3}
        end

        it 'changes the property to "auto.commit.interval.ms" and the value to milliseconds' do
          expect(kafka_config).to include('auto.commit.interval.ms' => 12_300)
        end
      end

      context 'and :auto_offset_reset is used' do
        let :config do
          {:auto_offset_reset => 'smallest'}
        end

        it 'changes the property to "auto.offset.reset"' do
          expect(kafka_config).to include('auto.offset.reset' => 'smallest')
        end
      end
    end
  end

  module Clients
    describe OffsetAndMetadata do
      let :offset_and_metadata do
        OffsetAndMetadata.new(234253434, 'hello world')
      end

      describe '#to_s' do
        context 'returns a string that' do
          it 'includes the class name' do
            expect(offset_and_metadata.to_s).to include(described_class.name)
          end

          it 'includes the offset' do
            expect(offset_and_metadata.to_s).to include('@offset=234253434')
          end

          it 'includes the metadata' do
            expect(offset_and_metadata.to_s).to include('@metadata="hello world"')
          end

          context 'when no metadata is specified' do
            let :offset_and_metadata do
              OffsetAndMetadata.new(234253434)
            end

            it 'includes the metadata (as nil)' do
              expect(offset_and_metadata.to_s).to include('@metadata=nil')
            end
          end
        end
      end
    end

    describe PartitionInfo do
      include_context 'config'

      let :partition_info do
        offset_strategy = Java::OrgApacheKafkaClientsConsumer::OffsetResetStrategy::NONE
        mock_consumer = Java::OrgApacheKafkaClientsConsumer::MockConsumer.new(offset_strategy)
        consumer = Java::IoBurtKafkaClients::ConsumerWrapper.create(JRuby.runtime, mock_consumer)
        leader = Java::OrgApacheKafkaCommon::Node.new(123, 'lolcathost', 1234)
        partition_info = Java::OrgApacheKafkaCommon::PartitionInfo.new(topic_names.first, 1, leader, [leader], [leader])
        mock_consumer.update_partitions(topic_names.first, [partition_info])
        consumer.partitions_for(topic_names.first).first
      end

      describe '#to_s' do
        context 'returns a string that' do
          it 'includes the class name' do
            expect(partition_info.to_s).to include(described_class.name)
          end

          it 'includes the topic' do
            expect(partition_info.to_s).to include(%|@topic="#{topic_names.first}"|)
          end

          it 'includes the partition' do
            expect(partition_info.to_s).to include("@partition=#{partition_info.partition}")
          end

          it 'includes the leader' do
            expect(partition_info.to_s).to include("@leader=#{partition_info.leader}")
          end

          it 'includes the replicas' do
            expect(partition_info.to_s).to include("@replicas=#{partition_info.replicas}")
          end

          it 'includes the in sync replicas' do
            expect(partition_info.to_s).to include("@in_sync_replicas=#{partition_info.in_sync_replicas}")
          end
        end
      end
    end

    describe Node do
      include_context 'config'

      let :node do
        offset_strategy = Java::OrgApacheKafkaClientsConsumer::OffsetResetStrategy::NONE
        mock_consumer = Java::OrgApacheKafkaClientsConsumer::MockConsumer.new(offset_strategy)
        consumer = Java::IoBurtKafkaClients::ConsumerWrapper.create(JRuby.runtime, mock_consumer)
        leader = Java::OrgApacheKafkaCommon::Node.new(123, 'lolcathost', 1234)
        partition_info = Java::OrgApacheKafkaCommon::PartitionInfo.new(topic_names.first, 1, leader, [leader], [leader])
        mock_consumer.update_partitions(topic_names.first, [partition_info])
        consumer.partitions_for(topic_names.first).first.leader
      end

      describe '#to_s' do
        context 'returns a string that' do
          it 'includes the class name' do
            expect(node.to_s).to include(described_class.name)
          end

          it 'includes the host' do
            expect(node.to_s).to include(%|@host="#{node.host}"|)
          end

          it 'includes the port' do
            expect(node.to_s).to include("@port=#{node.port}")
          end

          it 'includes the node ID' do
            expect(node.to_s).to include("@id=#{node.id}")
          end
        end
      end
    end

    describe RecordMetadata do
      include_context 'config'

      let :record_metadata do
        serializer = Java::IoBurtKafkaClients::RubyStringSerializer.new
        mock_producer = Java::OrgApacheKafkaClientsProducer::MockProducer.new(true, serializer, serializer)
        producer = Java::IoBurtKafkaClients::ProducerWrapper.create(JRuby.runtime, mock_producer)
        producer.send(topic_names.first, 'hello', 'world').get
      end

      describe '#to_s' do
        context 'returns a string that' do
          it 'includes the class name' do
            expect(record_metadata.to_s).to include(described_class.name)
          end

          it 'includes the topic' do
            expect(record_metadata.to_s).to include(%|@topic="#{record_metadata.topic}"|)
          end

          it 'includes the partition' do
            expect(record_metadata.to_s).to include("@partition=#{record_metadata.partition}")
          end

          it 'includes the offset' do
            expect(record_metadata.to_s).to include("@offset=#{record_metadata.offset}")
          end

          it 'includes the timestamp' do
            expect(record_metadata.to_s).to include("@timestamp=#{record_metadata.timestamp}")
          end

          it 'includes the checksum' do
            expect(record_metadata.to_s).to include("@checksum=#{record_metadata.checksum}")
          end

          it 'includes the serialized key size' do
            expect(record_metadata.to_s).to include("@serialized_key_size=#{record_metadata.serialized_key_size}")
          end

          it 'includes the serialized value size' do
            expect(record_metadata.to_s).to include("@serialized_value_size=#{record_metadata.serialized_value_size}")
          end
        end
      end
    end

    describe TopicPartition do
      let :topic_partition do
        TopicPartition.new('tropic_topic', 3)
      end

      describe '#to_s' do
        context 'returns a string that' do
          it 'includes the class name' do
            expect(topic_partition.to_s).to include(described_class.name)
          end

          it 'includes the topic' do
            expect(topic_partition.to_s).to include('@topic="tropic_topic"')
          end

          it 'includes the partition' do
            expect(topic_partition.to_s).to include('@partition=3')
          end
        end
      end
    end
  end
end
