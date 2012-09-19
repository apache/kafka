#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import socket
import struct
import binascii
import sys

PRODUCE_REQUEST_ID = 0

def encode_message(message):
    # <MAGIC_BYTE: char> <COMPRESSION_ALGO: char> <CRC32: int> <PAYLOAD: bytes>
    return struct.pack('>B', 1) + \
           struct.pack('>B', 0) + \
           struct.pack('>i', binascii.crc32(message)) + \
           message

def encode_produce_request(topic, partition, messages):
    # encode messages as <LEN: int><MESSAGE_BYTES>
    encoded = [encode_message(message) for message in messages]
    message_set = ''.join([struct.pack('>i', len(m)) + m for m in encoded])
    
    # create the request as <REQUEST_SIZE: int> <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER_SIZE: int> <BUFFER: bytes>
    data = struct.pack('>H', PRODUCE_REQUEST_ID) + \
           struct.pack('>H', len(topic)) + topic + \
           struct.pack('>i', partition) + \
           struct.pack('>i', len(message_set)) + message_set
    return struct.pack('>i', len(data)) + data


class KafkaProducer:
    def __init__(self, host, port):
        self.REQUEST_KEY = 0
        self.connection = socket.socket()
        self.connection.connect((host, port))

    def close(self):
        self.connection.close()

    def send(self, messages, topic, partition = 0):
        self.connection.sendall(encode_produce_request(topic, partition, messages))
    
if __name__ == '__main__':
    if len(sys.argv) < 4:
        print >> sys.stderr, 'USAGE: python', sys.argv[0], 'host port topic'
        sys.exit(1)
    host = sys.argv[1]
    port = int(sys.argv[2])
    topic = sys.argv[3]

    producer = KafkaProducer(host, port)

    while True:
        print 'Enter comma seperated messages: ',
        line = sys.stdin.readline()
        messages = line.split(',')
        producer.send(messages, topic)
        print 'Sent', len(messages), 'messages successfully'
