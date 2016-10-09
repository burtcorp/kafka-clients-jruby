module Kafka
  module Clients
    class ProducerRecord
      # @!method initialize(topic, partition, timestamp, key, value)
      #   @return [self]
      #   @overload send(topic, value)
      #     @param topic [String]
      #     @param value [String]
      #     @return [self]
      #   @overload send(topic, key, value)
      #     @param topic [String]
      #     @param key [String]
      #     @param value [String]
      #     @return [self]
      #   @overload send(topic, partition, key, value)
      #     @param topic [String]
      #     @param partition [Integer]
      #     @param key [String]
      #     @param value [String]
      #     @return [self]
      #   @overload send(topic, partition, timestamp, key, value)
      #     @param topic [String]
      #     @param partition [Integer]
      #     @param timestamp [Time, Float]
      #     @param key [String]
      #     @param value [String]
      #     @return [self]

      # @!method topic
      #   @return [String]

      # @!method partition
      #   @return [Integer]

      # @!method timestamp
      #   @return [Time]

      # @!method key
      #   @return [String]

      # @!method value
      #   @return [String]
    end
  end
end
