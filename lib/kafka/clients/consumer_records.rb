module Kafka
  module Clients
    class ConsumerRecords
      include Enumerable

      # @!method count
      #   @return [Integer]

      alias_method :size, :count

      # @!method each
      #   @yieldparam record [Kafka::Clients::ConsumerRecord]
      #   @return [Enumerable<Kafka::Clients::ConsumerRecord>]

      # @!method empty?
      #   @return [true, false]
    end
  end
end
