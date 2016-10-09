module Kafka
  module Clients
    class PartitionInfo
      # @!method topic
      #   @return [String]

      # @!method partition
      #   @return [Integer]

      # @!method leader
      #   @return [Kafka::Clients::Node]

      # @!method replicas
      #   @return [Enumerable<Kafka::Clients::Node>]

      # @!method in_sync_replicas
      #   @return [Enumerable<Kafka::Clients::Node>]
    end
  end
end
