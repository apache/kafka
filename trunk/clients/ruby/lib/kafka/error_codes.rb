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
