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
  "bytes"
  "encoding/binary"
)

type RequestType uint16

// Request Types
const (
  REQUEST_PRODUCE      RequestType = 0
  REQUEST_FETCH                    = 1
  REQUEST_MULTIFETCH               = 2
  REQUEST_MULTIPRODUCE             = 3
  REQUEST_OFFSETS                  = 4
)

// Request Header: <REQUEST_SIZE: uint32><REQUEST_TYPE: uint16><TOPIC SIZE: uint16><TOPIC: bytes><PARTITION: uint32>
func (b *Broker) EncodeRequestHeader(requestType RequestType) *bytes.Buffer {
  request := bytes.NewBuffer([]byte{})
  request.Write(uint32bytes(0)) // placeholder for request size
  request.Write(uint16bytes(int(requestType)))
  request.Write(uint16bytes(len(b.topic)))
  request.WriteString(b.topic)
  request.Write(uint32bytes(b.partition))

  return request
}

// after writing to the buffer is complete, encode the size of the request in the request.
func encodeRequestSize(request *bytes.Buffer) {
  binary.BigEndian.PutUint32(request.Bytes()[0:], uint32(request.Len()-4))
}

// <Request Header><TIME: uint64><MAX NUMBER of OFFSETS: uint32>
func (b *Broker) EncodeOffsetRequest(time int64, maxNumOffsets uint32) []byte {
  request := b.EncodeRequestHeader(REQUEST_OFFSETS)
  // specific to offset request
  request.Write(uint64ToUint64bytes(uint64(time)))
  request.Write(uint32toUint32bytes(maxNumOffsets))

  encodeRequestSize(request)

  return request.Bytes()
}

// <Request Header><OFFSET: uint64><MAX SIZE: uint32>
func (b *Broker) EncodeConsumeRequest(offset uint64, maxSize uint32) []byte {
  request := b.EncodeRequestHeader(REQUEST_FETCH)
  // specific to consume request
  request.Write(uint64ToUint64bytes(offset))
  request.Write(uint32toUint32bytes(maxSize))

  encodeRequestSize(request)

  return request.Bytes()
}

// <Request Header><MESSAGE SET SIZE: uint32><MESSAGE SETS>
func (b *Broker) EncodePublishRequest(messages ...*Message) []byte {
  // 4 + 2 + 2 + topicLength + 4 + 4
  request := b.EncodeRequestHeader(REQUEST_PRODUCE)

  messageSetSizePos := request.Len()
  request.Write(uint32bytes(0)) // placeholder message len

  written := 0
  for _, message := range messages {
    wrote, _ := request.Write(message.Encode())
    written += wrote
  }

  // now add the accumulated size of that the message set was
  binary.BigEndian.PutUint32(request.Bytes()[messageSetSizePos:], uint32(written))
  // now add the size of the whole to the first uint32
  encodeRequestSize(request)
  return request.Bytes()
}
