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
  "net"
  "os"
  "fmt"
  "encoding/binary"
  "io"
  "bufio"
)

const (
  NETWORK = "tcp"
)

type Broker struct {
  topic     string
  partition int
  hostname  string
}

func newBroker(hostname string, topic string, partition int) *Broker {
  return &Broker{topic: topic,
    partition: partition,
    hostname:  hostname}
}

func (b *Broker) connect() (conn *net.TCPConn, error os.Error) {
  raddr, err := net.ResolveTCPAddr(NETWORK, b.hostname)
  if err != nil {
    log.Println("Fatal Error: ", err)
    return nil, err
  }
  conn, err = net.DialTCP(NETWORK, nil, raddr)
  if err != nil {
    log.Println("Fatal Error: ", err)
    return nil, err
  }
  return conn, error
}

// returns length of response & payload & err
func (b *Broker) readResponse(conn *net.TCPConn) (uint32, []byte, os.Error) {
  reader := bufio.NewReader(conn)
  length := make([]byte, 4)
  lenRead, err := io.ReadFull(reader, length)
  if err != nil {
    return 0, []byte{}, err
  }
  if lenRead != 4 || lenRead < 0 {
    return 0, []byte{}, os.NewError("invalid length of the packet length field")
  }

  expectedLength := binary.BigEndian.Uint32(length)
  messages := make([]byte, expectedLength)
  lenRead, err = io.ReadFull(reader, messages)
  if err != nil {
    return 0, []byte{}, err
  }

  if uint32(lenRead) != expectedLength {
    return 0, []byte{}, os.NewError(fmt.Sprintf("Fatal Error: Unexpected Length: %d  expected:  %d", lenRead, expectedLength))
  }

  errorCode := binary.BigEndian.Uint16(messages[0:2])
  if errorCode != 0 {
    log.Println("errorCode: ", errorCode)
    return 0, []byte{}, os.NewError(
      fmt.Sprintf("Broker Response Error: %d", errorCode))
  }
  return expectedLength, messages[2:], nil
}
