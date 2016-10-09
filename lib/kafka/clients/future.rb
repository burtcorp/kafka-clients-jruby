module Kafka
  module Clients
    class Future
      # @!method done?
      #   @return [true, false]

      # @!method get(options=nil)
      #   @param options [Hash]
      #   @option options [Float] :timeout
      #   @return [Object]
    end
  end
end
