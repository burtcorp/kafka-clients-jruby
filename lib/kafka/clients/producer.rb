module Kafka
  module Clients
    class Producer
      # @!method initialize(configuration)
      #   @param configuration [Hash] A set of key-value pairs to use as
      #     configuration for the producer. Common config parameters have symbol
      #     aliases for convenience (see below), but the native string properties
      #     can also be used.
      #   @option configuration [String, Array<String>] :bootstrap_servers
      #     Alias for `bootstrap.servers`, but in addition to a comma separated
      #     list of servers it accepts an array.
      #   @option configuration [String] :acks Alias for `acks`
      #   @option configuration [String] :compression_type Alias for `compression.type`
      #   @option configuration [Integer] :retries Alias for `retries`
      #   @option configuration [Integer] :batch_size Alias for `batch.size`
      #   @option configuration [String] :client_id Alias for `client.id`
      #   @option configuration [Float] :linger Alias for `linger.ms`,
      #     but in seconds, _not milliseconds_
      #   @option configuration [Float] :max_block Alias for `max.block.ms`,
      #     but in seconds, _not milliseconds_
      #   @option configuration [Integer] :max_request_size Alias for `max.request.size`
      #   @option configuration [Float] :request_timeout Alias for `request.timeout.ms`,
      #     but in seconds, _not milliseconds_
      #   @option configuration [#partition] :partitioner An object that will
      #     receive `#partition` with the topic name, partition, key, value and
      #     cluster metadata and that will return the partition that the record
      #     should be sent to
      #   @option configuration [#serialize] :key_serializer An object that
      #     will receive `#serialize` with the key and that will return the
      #     string serialization of that key
      #   @option configuration [#deserialize] :value_serializer An object that
      #     will receive `#serialize` with the value and that will return the
      #     string serialization of that value
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
