/*
 *  Copyright (c) 2011 NeuStar, Inc.
 *  All rights reserved.  
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at 
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 *  NeuStar, the Neustar logo and related names and logos are registered
 *  trademarks, service marks or tradenames of NeuStar, Inc. All other 
 *  product names, company names, marks, logos and symbols may be trademarks
 *  of their respective owners.
 */

package kafka

import (
  "hash/crc32"
  "encoding/binary"
  "bytes"
  "log"
)

const (
  // Compression Support uses '1' - https://cwiki.apache.org/confluence/display/KAFKA/Compression
  MAGIC_DEFAULT = 1
  // magic + compression + chksum
  NO_LEN_HEADER_SIZE = 1 + 1 + 4
)

type Message struct {
  magic       byte
  compression byte
  checksum    [4]byte
  payload     []byte
  offset      uint64 // only used after decoding
  totalLength uint32 // total length of the raw message (from decoding)

}

func (m *Message) Offset() uint64 {
  return m.offset
}

func (m *Message) Payload() []byte {
  return m.payload
}

func (m *Message) PayloadString() string {
  return string(m.payload)
}

func NewMessageWithCodec(payload []byte, codec PayloadCodec) *Message {
  message := &Message{}
  message.magic = byte(MAGIC_DEFAULT)
  message.compression = codec.Id()
  message.payload = codec.Encode(payload)
  binary.BigEndian.PutUint32(message.checksum[0:], crc32.ChecksumIEEE(message.payload))
  return message
}

// Default is is create a message with no compression
func NewMessage(payload []byte) *Message {
  return NewMessageWithCodec(payload, DefaultCodecsMap[NO_COMPRESSION_ID])
}

// Create a Message using the default compression method (gzip)
func NewCompressedMessage(payload []byte) *Message {
  return NewCompressedMessages(NewMessage(payload))
}

func NewCompressedMessages(messages ...*Message) *Message {
  buf := bytes.NewBuffer([]byte{})
  for _, message := range messages {
    buf.Write(message.Encode())
  }
  return NewMessageWithCodec(buf.Bytes(), DefaultCodecsMap[GZIP_COMPRESSION_ID])
}

// MESSAGE SET: <MESSAGE LENGTH: uint32><MAGIC: 1 byte><COMPRESSION: 1 byte><CHECKSUM: uint32><MESSAGE PAYLOAD: bytes>
func (m *Message) Encode() []byte {
  msgLen := NO_LEN_HEADER_SIZE + len(m.payload)
  msg := make([]byte, 4+msgLen)
  binary.BigEndian.PutUint32(msg[0:], uint32(msgLen))
  msg[4] = m.magic
  msg[5] = m.compression

  copy(msg[6:], m.checksum[0:])
  copy(msg[10:], m.payload)

  return msg
}

func DecodeWithDefaultCodecs(packet []byte) (uint32, []Message) {
  return Decode(packet, DefaultCodecsMap)
}

func Decode(packet []byte, payloadCodecsMap map[byte]PayloadCodec) (uint32, []Message) {
  messages := []Message{}

  length, message := decodeMessage(packet, payloadCodecsMap)

  if length > 0 && message != nil {
    if message.compression != NO_COMPRESSION_ID {
      // wonky special case for compressed messages having embedded messages
      payloadLen := uint32(len(message.payload))
      messageLenLeft := payloadLen
      for messageLenLeft > 0 {
        start := payloadLen - messageLenLeft
        innerLen, innerMsg := decodeMessage(message.payload[start:], payloadCodecsMap)
        messageLenLeft = messageLenLeft - innerLen - 4 // message length uint32
        messages = append(messages, *innerMsg)
      }
    } else {
      messages = append(messages, *message)
    }
  }

  return length, messages
}

func decodeMessage(packet []byte, payloadCodecsMap map[byte]PayloadCodec) (uint32, *Message) {
  length := binary.BigEndian.Uint32(packet[0:])
  if length > uint32(len(packet[4:])) {
    log.Printf("length mismatch, expected at least: %X, was: %X\n", length, len(packet[4:]))
    return 0, nil
  }
  msg := Message{}
  msg.totalLength = length
  msg.magic = packet[4]

  rawPayload := []byte{}
  if msg.magic == 0 {
    msg.compression = byte(0)
    copy(msg.checksum[:], packet[5:9])
    payloadLength := length - 1 - 4
    rawPayload = packet[9 : 9+payloadLength]
  } else if msg.magic == MAGIC_DEFAULT {
    msg.compression = packet[5]
    copy(msg.checksum[:], packet[6:10])
    payloadLength := length - NO_LEN_HEADER_SIZE
    rawPayload = packet[10 : 10+payloadLength]
  } else {
    log.Printf("incorrect magic, expected: %X was: %X\n", MAGIC_DEFAULT, msg.magic)
    return 0, nil
  }

  payloadChecksum := make([]byte, 4)
  binary.BigEndian.PutUint32(payloadChecksum, crc32.ChecksumIEEE(rawPayload))
  if !bytes.Equal(payloadChecksum, msg.checksum[:]) {
    msg.Print()
    log.Printf("checksum mismatch, expected: % X was: % X\n", payloadChecksum, msg.checksum[:])
    return 0, nil
  }
  msg.payload = payloadCodecsMap[msg.compression].Decode(rawPayload)

  return length, &msg
}

func (msg *Message) Print() {
  log.Println("----- Begin Message ------")
  log.Printf("magic: %X\n", msg.magic)
  log.Printf("compression: %X\n", msg.compression)
  log.Printf("checksum: %X\n", msg.checksum)
  if len(msg.payload) < 1048576 { // 1 MB 
    log.Printf("payload: % X\n", msg.payload)
    log.Printf("payload(string): %s\n", msg.PayloadString())
  } else {
    log.Printf("long payload, length: %d\n", len(msg.payload))
  }
  log.Printf("length: %d\n", msg.totalLength)
  log.Printf("offset: %d\n", msg.offset)
  log.Println("----- End Message ------")
}
