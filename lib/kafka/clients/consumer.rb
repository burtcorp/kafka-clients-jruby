module Kafka
  module Clients
    class Consumer
      # @!method initialize(configuration)
      #   @param configuration [Hash] A set of key-value pairs to use as
      #     configuration for the consumer. Common config parameters have symbol
      #     aliases for convenience (see below), but the native string properties
      #     can also be used.
      #   @option configuration [String, Array<String>] :bootstrap_servers
      #     Alias for `bootstrap.servers`, but in addition to a comma separated
      #     list of servers it accepts an array.
      #   @option configuration [String] :group_id Alias for `group.id`
      #   @option configuration [String] :client_id Alias for `client.id`
      #   @option configuration [Integer] :max_poll_records Alias for `max.poll.records`
      #   @option configuration [true, false] :auto_commit alias for `enable.auto.commit`
      #   @option configuration [Integer] :auto_commit_interval Alias for `auto.commit.interval.ms`,
      #     but in seconds, _not milliseconds_
      #   @option configuration [true, false] :auto_offset_reset Alias for `auto.offset.reset`
      #   @option configuration [Float] :request_timeout Alias for `request.timeout.ms`,
      #     but in seconds, _not milliseconds_
      #   @option configuration [#deserialize] :key_deserializer An object that
      #     will receive `#deserialize` with the string serialization of the key
      #     and that will return the deserialized version of that key. The input
      #     to the deserializer is affected by the `:encoding` configuration.
      #     The default deserializer interprets the key as a string.
      #   @option configuration [#deserialize] :value_deserializer An object that
      #     will receive `#deserialize` with the string serialization of the value
      #     and that will return the deserialized version of that value. The input
      #     to the deserializer is affected by the `:encoding` configuration.
      #     The default deserializer returns strings.
      #     The default deserializer interprets the value as a string.
      #   @option configuration [Encoding] :encoding The encoding to use for
      #     deserialized keys and values (default to `Encoding.default_external`).
      #     This encoding is applied (using something like `#force_encoding`) to
      #     strings before any custom deserialization happens, so if you're
      #     setting `:key_deserializer` and/or `:value_deserializer` you might
      #     want to set this configuration too.
      #   @return [self]

      # @!method close
      #   @return [nil]

      # @!method partitions_for(topic_name)
      #   @param topic_name [String]
      #   @return [Enumerable<Kafka::Clients::PartitionInfo>]

      # @!method subscribe(topic_names_or_pattern)
      #   @param topic_names_or_pattern [Enumerable<String>, String]
      #   @return [nil]

      # @!method subscription
      #   @return [Enumerable<String>]

      # @!method usubscribe
      #   @return [nil]

      # @!method poll(timeout)
      #   @param timeout [Float] the number of seconds (not milliseconds) to
      #     wait for records
      #   @return [Kafka::Clients::ConsumerRecords]

      # @!method commit_sync(offsets=nil)
      #   @param offsets [Hash<Kafka::Clients::TopicPartition, (Kafka::Clients::OffsetAndMetadata, Integer)>]
      #   @return [nil]

      # @!method commit_async(offsets=nil)
      #   @param offsets [Hash<Kafka::Clients::TopicPartition, (Kafka::Clients::OffsetAndMetadata, Integer)>]
      #   @yieldparam offsets [Hash<Kafka::Clients::TopicPartition, Kafka::Clients::OffsetAndMetadata>]
      #   @yieldparam error [Kafka::Clients::KafkaError]
      #   @return [nil]

      # @!method committed(topic_partition)
      #   @return [OffsetAndMetadata]
      #   @overload committed(topic_partition)
      #     @param topic_partition [TopicPartition]
      #     @return [OffsetAndMetadata]
      #   @overload committed(topic, partition)
      #     @param topic [String]
      #     @param partition [OffsetAndMetadata]
      #     @return [OffsetAndMetadata]

      # @!method position(topic_partition)
      #   @return [Integer]
      #   @overload position(topic_partition)
      #     @param topic_partition [TopicPartition]
      #     @return [Integer]
      #   @overload position(topic, partition)
      #     @param topic [String]
      #     @param partition [Integer]
      #     @return [Integer]

      # @!method seek_to_beginning(topic_partitions=nil)
      #   @param topic_partitions [Enumerable<Kafka::Clients::TopicPartition>]
      #   @return [nil]

      # @!method seek_to_end(topic_partitions=nil)
      #   @param topic_partitions [Enumerable<Kafka::Clients::TopicPartition>]
      #   @return [nil]

      # @!method seek(topic_partition, offset)
      #   @return [nil]
      #   @overload seek(topic_partition, offset)
      #     @param topic_partition [Kafka::Clients::TopicPartition]
      #     @param offset [Integer]
      #     @return [nil]
      #   @overload seek(topic, partition, offset)
      #     @param topic [String]
      #     @param partition [Integer]
      #     @param offset [Integer]
      #     @return [nil]

      # @!method assign(topic_partitions)
      #   @param topic_partitions [Enumerable<Kafka::Clients::TopicPartition>]
      #   @return [nil]

      # @!method assignment
      #   @return [Enumerable<TopicPartition>]

      # @!method wakeup
      #   @return [nil]

      # @!method list_topics
      #   @return [Hash<String, Enumerable<Kafka::Clients::PartitionInfo>>]

      # @!method pause(topic_partitions)
      #   @param topic_partitions [Enumerable<Kafka::Clients::TopicPartition>]
      #   @return [nil]

      # @!method resume(topic_partitions)
      #   @param topic_partitions [Enumerable<Kafka::Clients::TopicPartition>]
      #   @return [nil]

      # @!method paused
      #   @return [Enumerable<Kafka::Clients::TopicPartition>]
    end
  end
end
