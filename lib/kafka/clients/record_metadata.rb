module Kafka
  module Clients
    class RecordMetadata
      # @!method topic
      #   @return [String]

      # @!method partition
      #   @return [Integer]

      # @!method timestamp
      #   @return [Time]

      # @!method offset
      #   @return [Integer]

      # @!method checksum
      #   @return [Integer]

      # @!method serialized_key_size
      #   @return [Integer]

      # @!method serialized_value_size
      #   @return [Integer]
    end
  end
end
