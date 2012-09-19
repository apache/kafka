# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
module Kafka
  class Consumer

    include Kafka::IO

    CONSUME_REQUEST_TYPE = Kafka::RequestType::FETCH
    MAX_SIZE = 1048576 # 1 MB
    DEFAULT_POLLING_INTERVAL = 2 # 2 seconds
    MAX_OFFSETS = 100

    attr_accessor :topic, :partition, :offset, :max_size, :request_type, :polling

    def initialize(options = {})
      self.topic        = options[:topic]        || "test"
      self.partition    = options[:partition]    || 0
      self.host         = options[:host]         || "localhost"
      self.port         = options[:port]         || 9092
      self.offset       = options[:offset]       || -2
      self.max_size     = options[:max_size]     || MAX_SIZE
      self.request_type = options[:request_type] || CONSUME_REQUEST_TYPE
      self.polling      = options[:polling]      || DEFAULT_POLLING_INTERVAL
      self.connect(self.host, self.port)

      if @offset < 0
         send_offsets_request
         offsets = read_offsets_response
         raise Exception, "No offsets for #@topic-#@partition" if offsets.empty?
         @offset = offsets[0]
      end
    end

    # REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
    def request_size
      2 + 2 + topic.length + 4 + 8 + 4
    end

    def encode_request_size
      [self.request_size].pack("N")
    end

    def encode_request(request_type, topic, partition, offset, max_size)
      request_type = [request_type].pack("n")
      topic        = [topic.length].pack('n') + topic
      partition    = [partition].pack("N")
      offset       = [offset].pack("Q").reverse # DIY 64bit big endian integer
      max_size     = [max_size].pack("N")

      request_type + topic + partition + offset + max_size
    end

    def offsets_request_size
       2 + 2 + topic.length + 4 + 8 +4
    end

    def encode_offsets_request_size
       [offsets_request_size].pack('N')
    end

    # Query the server for the offsets
    def encode_offsets_request(topic, partition, time, max_offsets)
       req         = [Kafka::RequestType::OFFSETS].pack('n')
       topic       = [topic.length].pack('n') + topic
       partition   = [partition].pack('N')
       time        = [time].pack("q").reverse # DIY 64bit big endian integer
       max_offsets = [max_offsets].pack('N')

       req + topic + partition + time + max_offsets
    end

    def consume
      self.send_consume_request         # request data
      data = self.read_data_response    # read data response
      self.parse_message_set_from(data) # parse message set
    end

    def loop(&block)
      messages = []
      while(true) do
        messages = self.consume
        block.call(messages) if messages && !messages.empty?
        sleep(self.polling)
      end
    end

    def read_data_response
      data_length = self.socket.read(4).unpack("N").shift # read length
      data = self.socket.read(data_length)                # read message set
      data[2, data.length]                                # we start with a 2 byte offset
    end

    def send_consume_request
      self.write(self.encode_request_size) # write request_size
      self.write(self.encode_request(self.request_type, self.topic, self.partition, self.offset, self.max_size)) # write request
    end

    def send_offsets_request
      self.write(self.encode_offsets_request_size) # write request_size
      self.write(self.encode_offsets_request(@topic, @partition, -2, MAX_OFFSETS)) # write request
    end

    def read_offsets_response
      data_length = self.socket.read(4).unpack('N').shift # read length
      data = self.socket.read(data_length)                # read message

      pos = 0
      error_code = data[pos,2].unpack('n')[0]
      raise Exception, Kafka::ErrorCodes::to_s(error_code) if error_code != Kafka::ErrorCodes::NO_ERROR

      pos += 2
      count = data[pos,4].unpack('N')[0]
      pos += 4

      res = []
      while pos != data.size
         res << data[pos,8].reverse.unpack('q')[0]
         pos += 8
      end

      res
    end

    def parse_message_set_from(data)
      messages = []
      processed = 0
      length = data.length - 4
      while(processed <= length) do
        message_size = data[processed, 4].unpack("N").shift
        messages << Kafka::Message.parse_from(data[processed, message_size + 4])
        processed += 4 + message_size
      end
      self.offset += processed
      messages
    end
  end
end
