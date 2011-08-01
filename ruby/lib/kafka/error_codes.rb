module Kafka
  module ErrorCodes
    NO_ERROR                = 0
    OFFSET_OUT_OF_RANGE     = 1
    INVALID_MESSAGE_CODE    = 2
    WRONG_PARTITION_CODE    = 3
    INVALID_RETCH_SIZE_CODE = 4

    STRINGS = {
      0 => 'No error',
      1 => 'Offset out of range',
      2 => 'Invalid message code',
      3 => 'Wrong partition code',
      4 => 'Invalid retch size code',
    }

    def self.to_s(code)
      STRINGS[code] || 'Unknown error'
    end
  end
end
