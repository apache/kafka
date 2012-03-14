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

package main

import (
  "flag"
  "fmt"
  "os"
  "os/signal"
  "strconv"
  kafka "svn.apache.org/repos/asf/incubator/kafka.svn/trunk/clients/go/src"
  "syscall"
)

var hostname string
var topic string
var partition int
var offset uint64
var maxSize uint
var writePayloadsTo string
var consumerForever bool
var printmessage bool

func init() {
  flag.StringVar(&hostname, "hostname", "localhost:9092", "host:port string for the kafka server")
  flag.StringVar(&topic, "topic", "test", "topic to publish to")
  flag.IntVar(&partition, "partition", 0, "partition to publish to")
  flag.Uint64Var(&offset, "offset", 0, "offset to start consuming from")
  flag.UintVar(&maxSize, "maxsize", 1048576, "max size in bytes of message set to request")
  flag.StringVar(&writePayloadsTo, "writeto", "", "write payloads to this file")
  flag.BoolVar(&consumerForever, "consumeforever", false, "loop forever consuming")
  flag.BoolVar(&printmessage, "printmessage", true, "print the message details to stdout")
}

func main() {
  flag.Parse()
  fmt.Println("Consuming Messages :")
  fmt.Printf("From: %s, topic: %s, partition: %d\n", hostname, topic, partition)
  fmt.Println(" ---------------------- ")
  broker := kafka.NewBrokerConsumer(hostname, topic, partition, offset, uint32(maxSize))

  var payloadFile *os.File = nil
  if len(writePayloadsTo) > 0 {
    var err error
    payloadFile, err = os.Create(writePayloadsTo)
    if err != nil {
      fmt.Println("Error opening file: ", err)
      payloadFile = nil
    }
  }

  consumerCallback := func(msg *kafka.Message) {
    if printmessage {
      msg.Print()
    }
    if payloadFile != nil {
      payloadFile.Write([]byte("Message at: " + strconv.FormatUint(msg.Offset(), 10) + "\n"))
      payloadFile.Write(msg.Payload())
      payloadFile.Write([]byte("\n-------------------------------\n"))
    }
  }

  if consumerForever {
    quit := make(chan bool, 1)
    go func() {
      sigIn := make(chan os.Signal)
      signal.Notify(sigIn)
      for {

        select {
        case sig := <-sigIn:
          if sig.(os.Signal) == syscall.SIGINT {
            quit <- true
          } else {
            fmt.Println(sig)
          }
        }
      }
    }()

    msgChan := make(chan *kafka.Message)
    go broker.ConsumeOnChannel(msgChan, 10, quit)
    for msg := range msgChan {
      if msg != nil {
        consumerCallback(msg)
      } else {
        break
      }
    }
  } else {
    broker.Consume(consumerCallback)
  }

  if payloadFile != nil {
    payloadFile.Close()
  }

}
