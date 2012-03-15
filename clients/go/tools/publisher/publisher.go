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
  "kafka"
  "flag"
  "fmt"
  "os"
)

var hostname string
var topic string
var partition int
var message string
var messageFile string
var compress bool

func init() {
  flag.StringVar(&hostname, "hostname", "localhost:9092", "host:port string for the kafka server")
  flag.StringVar(&topic, "topic", "test", "topic to publish to")
  flag.IntVar(&partition, "partition", 0, "partition to publish to")
  flag.StringVar(&message, "message", "", "message to publish")
  flag.StringVar(&messageFile, "messagefile", "", "read message from this file")
  flag.BoolVar(&compress, "compress", false, "compress the messages published")
}

func main() {
  flag.Parse()
  fmt.Println("Publishing :", message)
  fmt.Printf("To: %s, topic: %s, partition: %d\n", hostname, topic, partition)
  fmt.Println(" ---------------------- ")
  broker := kafka.NewBrokerPublisher(hostname, topic, partition)

  if len(message) == 0 && len(messageFile) != 0 {
    file, err := os.Open(messageFile)
    if err != nil {
      fmt.Println("Error: ", err)
      return
    }
    stat, err := file.Stat()
    if err != nil {
      fmt.Println("Error: ", err)
      return
    }
    payload := make([]byte, stat.Size)
    file.Read(payload)
    timing := kafka.StartTiming("Sending")

    if compress {
      broker.Publish(kafka.NewCompressedMessage(payload))
    } else {
      broker.Publish(kafka.NewMessage(payload))
    }

    timing.Print()
    file.Close()
  } else {
    timing := kafka.StartTiming("Sending")

    if compress {
      broker.Publish(kafka.NewCompressedMessage([]byte(message)))
    } else {
      broker.Publish(kafka.NewMessage([]byte(message)))
    }

    timing.Print()
  }
}
