module Kafka
  class Batch
    attr_accessor :messages

    def initialize
      self.messages = []
    end

    def << (message)
      self.messages << message
    end
  end
end