module Kafka
  module Clients
    class ConsumerRecord
      # @!method topic
      #   @return [String]

      # @!method partition
      #   @return [Integer]

      # @!method offset
      #   @return [Integer]

      # @!method checksum
      #   @return [Integer]

      # @!method key
      #   @return [String]

      # @!method value
      #   @return [String]

      # @!method timestamp
      #   @return [Timestamp]

      # @!method timestamp_type
      #   @return [Symbol]
    end
  end
end
