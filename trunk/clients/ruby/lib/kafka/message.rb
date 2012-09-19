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

  # A message. The format of an N byte message is the following:
  # 1 byte "magic" identifier to allow format changes
  # 4 byte CRC32 of the payload
  # N - 5 byte payload
  class Message

    MAGIC_IDENTIFIER_DEFAULT = 0

    attr_accessor :magic, :checksum, :payload

    def initialize(payload = nil, magic = MAGIC_IDENTIFIER_DEFAULT, checksum = nil)
      self.magic    = magic
      self.payload  = payload
      self.checksum = checksum || self.calculate_checksum
    end

    def calculate_checksum
      Zlib.crc32(self.payload)
    end

    def valid?
      self.checksum == Zlib.crc32(self.payload)
    end

    def self.parse_from(binary)
      size     = binary[0, 4].unpack("N").shift.to_i
      magic    = binary[4, 1].unpack("C").shift
      checksum = binary[5, 4].unpack("N").shift
      payload  = binary[9, size] # 5 = 1 + 4 is Magic + Checksum
      return Kafka::Message.new(payload, magic, checksum)
    end
  end
end
