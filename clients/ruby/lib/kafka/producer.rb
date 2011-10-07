module Kafka
  class Producer

    include Kafka::IO

    PRODUCE_REQUEST_ID = Kafka::RequestType::PRODUCE

    attr_accessor :topic, :partition

    def initialize(options = {})
      self.topic     = options[:topic]      || "test"
      self.partition = options[:partition]  || 0
      self.host      = options[:host]       || "localhost"
      self.port      = options[:port]       || 9092
      self.connect(self.host, self.port)
    end

    def encode(message)
      [message.magic].pack("C") + [message.calculate_checksum].pack("N") + message.payload.to_s
    end

    def encode_request(topic, partition, messages)
      message_set = Array(messages).collect { |message|
        encoded_message = self.encode(message)
        [encoded_message.length].pack("N") + encoded_message
      }.join("")

      request   = [PRODUCE_REQUEST_ID].pack("n")
      topic     = [topic.length].pack("n") + topic
      partition = [partition].pack("N")
      messages  = [message_set.length].pack("N") + message_set

      data = request + topic + partition + messages

      return [data.length].pack("N") + data
    end

    def send(messages)
      self.write(self.encode_request(self.topic, self.partition, messages))
    end

    def batch(&block)
      batch = Kafka::Batch.new
      block.call( batch )
      self.send(batch.messages)
      batch.messages.clear
    end
  end
end
