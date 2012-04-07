/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import consumer._
import message.Message
import utils.Utils
import java.util.concurrent.CountDownLatch

object TestZKConsumerOffsets {
  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      println("USAGE: " + TestZKConsumerOffsets.getClass.getName + " consumer.properties topic latest")
      System.exit(1)
    }
    println("Starting consumer...")
    val topic = args(1)
    val autoOffsetReset = args(2)    
    val props = Utils.loadProps(args(0))
    props.put("autooffset.reset", "largest")
    
    val config = new ConsumerConfig(props)
    val consumerConnector: ConsumerConnector = Consumer.create(config)
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(topic -> 1))
    var threadList = List[ConsumerThread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (stream <- streamList)
        threadList ::= new ConsumerThread(stream)

    for (thread <- threadList)
      thread.start

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        consumerConnector.shutdown
        threadList.foreach(_.shutdown)
        println("consumer threads shutted down")
      }
    })
  }
}

private class ConsumerThread(stream: KafkaStream[Message]) extends Thread {
  val shutdownLatch = new CountDownLatch(1)

  override def run() {
    println("Starting consumer thread..")
    for (messageAndMetadata <- stream) {
      println("consumed: " + Utils.toString(messageAndMetadata.message.payload, "UTF-8"))
    }
    shutdownLatch.countDown
    println("thread shutdown !" )
  }

  def shutdown() {
    shutdownLatch.await
  }
}
