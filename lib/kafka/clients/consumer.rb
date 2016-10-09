module Kafka
  module Clients
    class Consumer
      # @!method initialize(configuration)
      #   @param configuration [Hash]
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