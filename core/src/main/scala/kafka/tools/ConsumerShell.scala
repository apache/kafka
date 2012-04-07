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

package kafka.tools

import joptsimple._
import kafka.utils.{Utils, Logging}
import java.util.concurrent.CountDownLatch
import kafka.consumer._
import kafka.serializer.StringDecoder

/**
 * Program to read using the rich consumer and dump the results to standard out
 */
object ConsumerShell {
  def main(args: Array[String]): Unit = {
    
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val consumerPropsOpt = parser.accepts("props", "REQUIRED: Properties file with the consumer properties.")
                           .withRequiredArg
                           .describedAs("properties")
                           .ofType(classOf[String])
    val partitionsOpt = parser.accepts("partitions", "Number of partitions to consume from.")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
    
    val options = parser.parse(args : _*)
    
    for(arg <- List(topicOpt, consumerPropsOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    
    val partitions = options.valueOf(partitionsOpt).intValue
    val propsFile = options.valueOf(consumerPropsOpt)
    val topic = options.valueOf(topicOpt)
    
    println("Starting consumer...")

    val consumerConfig = new ConsumerConfig(Utils.loadProps(propsFile))
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(topic -> partitions), new StringDecoder)
    var threadList = List[ZKConsumerThread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (stream <- streamList)
        threadList ::= new ZKConsumerThread(stream)

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

class ZKConsumerThread(stream: KafkaStream[String]) extends Thread with Logging {
  val shutdownLatch = new CountDownLatch(1)

  override def run() {
    println("Starting consumer thread..")
    var count: Int = 0
    try {
      for (messageAndMetadata <- stream) {
        println("consumed: " + messageAndMetadata.message)
        count += 1
      }
    }catch {
      case e:ConsumerTimeoutException => // this is ok
      case oe: Exception => error("error in ZKConsumerThread", oe)
    }
    shutdownLatch.countDown
    println("Received " + count + " messages")
    println("thread shutdown !" )
  }

  def shutdown() {
    shutdownLatch.await
  }          
}
