module Kafka
  module Clients
    class Producer
      # @!method initialize(configuration)
      #   @param configuration [Hash]
      #   @return [self]

      # @!method close(options=nil)
      #   @param options [Hash]
      #   @option options [Float] :timeout
      #   @return [self]

      # @!method send(record)
      #   @yieldparam metadata [Kafka::Clients::RecordMetadata]
      #   @yieldparam error [Kafka::Clients::KafkaError]
      #   @return [Kafka::Clients::Future<Kafka::Clients::RecordMetadata>]
      #   @overload send(record)
      #     @yieldparam metadata [Kafka::Clients::RecordMetadata]
      #     @yieldparam error [Kafka::Clients::KafkaError]
      #     @param record [Kafka::Clients::ProducerRecord]
      #     @return [Kafka::Clients::Future<Kafka::Clients::RecordMetadata>]
      #   @overload send(topic, value)
      #     @yieldparam metadata [Kafka::Clients::RecordMetadata]
      #     @yieldparam error [Kafka::Clients::KafkaError]
      #     @param topic [String]
      #     @param value [String]
      #     @return [Kafka::Clients::Future<Kafka::Clients::RecordMetadata>]
      #   @overload send(topic, key, value)
      #     @yieldparam metadata [Kafka::Clients::RecordMetadata]
      #     @yieldparam error [Kafka::Clients::KafkaError]
      #     @param topic [String]
      #     @param key [String]
      #     @param value [String]
      #     @return [Kafka::Clients::Future<Kafka::Clients::RecordMetadata>]
      #   @overload send(topic, partition, key, value)
      #     @yieldparam metadata [Kafka::Clients::RecordMetadata]
      #     @yieldparam error [Kafka::Clients::KafkaError]
      #     @param topic [String]
      #     @param partition [Integer]
      #     @param key [String]
      #     @param value [String]
      #     @return [Kafka::Clients::Future<Kafka::Clients::RecordMetadata>]
      #   @overload send(topic, partition, timestamp, key, value)
      #     @yieldparam metadata [Kafka::Clients::RecordMetadata]
      #     @yieldparam error [Kafka::Clients::KafkaError]
      #     @param topic [String]
      #     @param partition [Integer]
      #     @param timestamp [Time, Float]
      #     @param key [String]
      #     @param value [String]
      #     @return [Kafka::Clients::Future<Kafka::Clients::RecordMetadata>]

      # @!method flush
      #   @return [nil]

      # @!method partitions_for(topic_name)
      #   @param topic_name [String]
      #   @return [Enumerable<Kafka::Clients::PartitionInfo>]
    end
  end
end
