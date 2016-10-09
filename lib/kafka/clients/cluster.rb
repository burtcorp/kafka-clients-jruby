module Kafka
  module Clients
    class Cluster
      # @!method nodes
      #   @return [Enumerable<Kafka::Clients::Node>]

      # @!method node_by_id(node_id)
      #   @param node_id [Integer]
      #   @return [Kafka::Clients::Node]

      # @!method leader_for(topic_partition)
      #   @return [Kafka::Clients::Node]
      #   @overload leader_for(topic_partition)
      #     @param topic_partition [Kafka::Clients::TopicPartition]
      #     @return [Kafka::Clients::Node]
      #   @overload leader_for(topic, partition)
      #     @param topic [String]
      #     @param partition [Integer]
      #     @return [Kafka::Clients::Node]

      # @!method partition(topic_partition)
      #   @return [Kafka::Clients::PartitionInfo]
      #   @overload partition(topic_partition)
      #     @param topic_partition [Kafka::Clients::TopicPartition]
      #     @return [Kafka::Clients::PartitionInfo]
      #   @overload partition(topic, partition)
      #     @param topic [String]
      #     @param partition [Integer]
      #     @return [Kafka::Clients::PartitionInfo]

      # @!method partitions_for_topic(topic_name)
      #   @param topic_name [String]
      #   @return [Enumerable<Kafka::Clients::PartitionInfo>]

      # @!method available_partitions_for_topic(topic_name)
      #   @param topic_name [String]
      #   @return [Enumerable<Kafka::Clients::PartitionInfo>]

      # @!method partitions_for_node(node_id)
      #   @param node_id [Integer]
      #   @return [Enumerable<Kafka::Clients::PartitionInfo>]

      # @!method partition_count_for_topic(topic_name)
      #   @param topic_name [String]
      #   @return [Integer, nil]

      # @!method topics
      #   @return [Enumerable<String>]
    end
  end
end
