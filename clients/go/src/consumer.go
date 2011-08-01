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
  "log"
  "os"
  "net"
  "time"
  "encoding/binary"
)

type BrokerConsumer struct {
  broker  *Broker
  offset  uint64
  maxSize uint32
}

// Create a new broker consumer
// hostname - host and optionally port, delimited by ':'
// topic to consume
// partition to consume from
// offset to start consuming from
// maxSize (in bytes) of the message to consume (this should be at least as big as the biggest message to be published)
func NewBrokerConsumer(hostname string, topic string, partition int, offset uint64, maxSize uint32) *BrokerConsumer {
  return &BrokerConsumer{broker: newBroker(hostname, topic, partition),
    offset:  offset,
    maxSize: maxSize}
}

// Simplified consumer that defaults the offset and maxSize to 0.
// hostname - host and optionally port, delimited by ':'
// topic to consume
// partition to consume from
func NewBrokerOffsetConsumer(hostname string, topic string, partition int) *BrokerConsumer {
  return &BrokerConsumer{broker: newBroker(hostname, topic, partition),
    offset:  0,
    maxSize: 0}
}


func (consumer *BrokerConsumer) ConsumeOnChannel(msgChan chan *Message, pollTimeoutMs int64, quit chan bool) (int, os.Error) {
  conn, err := consumer.broker.connect()
  if err != nil {
    return -1, err
  }

  num := 0
  done := make(chan bool, 1)
  go func() {
    for {
      _, err := consumer.consumeWithConn(conn, func(msg *Message) {
        msgChan <- msg
        num += 1
      })

      if err != nil {
        if err != os.EOF {
          log.Println("Fatal Error: ", err)
        }
        break
      }
      time.Sleep(pollTimeoutMs * 1000000)
    }
    done <- true
  }()

  // wait to be told to stop..
  <-quit
  conn.Close()
  close(msgChan)
  <-done
  return num, err
}

type MessageHandlerFunc func(msg *Message)

func (consumer *BrokerConsumer) Consume(handlerFunc MessageHandlerFunc) (int, os.Error) {
  conn, err := consumer.broker.connect()
  if err != nil {
    return -1, err
  }
  defer conn.Close()

  num, err := consumer.consumeWithConn(conn, handlerFunc)

  if err != nil {
    log.Println("Fatal Error: ", err)
  }

  return num, err
}


func (consumer *BrokerConsumer) consumeWithConn(conn *net.TCPConn, handlerFunc MessageHandlerFunc) (int, os.Error) {
  _, err := conn.Write(consumer.broker.EncodeConsumeRequest(consumer.offset, consumer.maxSize))
  if err != nil {
    return -1, err
  }

  length, payload, err := consumer.broker.readResponse(conn)

  if err != nil {
    return -1, err
  }

  num := 0
  if length > 2 {
    // parse out the messages
    var currentOffset uint64 = 0
    for currentOffset <= uint64(length-4) {
      msg := Decode(payload[currentOffset:])
      if msg == nil {
        return num, os.NewError("Error Decoding Message")
      }
      msg.offset = consumer.offset + currentOffset
      currentOffset += uint64(4 + msg.totalLength)
      handlerFunc(msg)
      num += 1
    }
    // update the broker's offset for next consumption
    consumer.offset += currentOffset
  }

  return num, err
}


// Get a list of valid offsets (up to maxNumOffsets) before the given time, where 
// time is in milliseconds (-1, from the latest offset available, -2 from the smallest offset available)
// The result is a list of offsets, in descending order.
func (consumer *BrokerConsumer) GetOffsets(time int64, maxNumOffsets uint32) ([]uint64, os.Error) {
  offsets := make([]uint64, 0)

  conn, err := consumer.broker.connect()
  if err != nil {
    return offsets, err
  }

  defer conn.Close()

  _, err = conn.Write(consumer.broker.EncodeOffsetRequest(time, maxNumOffsets))
  if err != nil {
    return offsets, err
  }

  length, payload, err := consumer.broker.readResponse(conn)
  if err != nil {
    return offsets, err
  }

  if length > 4 {
    // get the number of offsets
    numOffsets := binary.BigEndian.Uint32(payload[0:])
    var currentOffset uint64 = 4
    for currentOffset < uint64(length-4) && uint32(len(offsets)) < numOffsets {
      offset := binary.BigEndian.Uint64(payload[currentOffset:])
      offsets = append(offsets, offset)
      currentOffset += 8 // offset size
    }
  }

  return offsets, err
}
