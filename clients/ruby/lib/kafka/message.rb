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
