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

import java.net.URI
import joptsimple._
import kafka.api.FetchRequest
import kafka.utils._
import kafka.consumer._

/**
 * Command line program to dump out messages to standard out using the simple consumer
 */
object SimpleConsumerShell extends Logging {

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser
    val urlOpt = parser.accepts("server", "REQUIRED: The hostname of the server to connect to.")
                           .withRequiredArg
                           .describedAs("kafka://hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val partitionOpt = parser.accepts("partition", "The partition to consume from.")
                           .withRequiredArg
                           .describedAs("partition")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(0)
    val offsetOpt = parser.accepts("offset", "The offset to start consuming from.")
                           .withRequiredArg
                           .describedAs("offset")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(0L)
    val fetchsizeOpt = parser.accepts("fetchsize", "The fetch size of each request.")
                           .withRequiredArg
                           .describedAs("fetchsize")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1000000)
    val printOffsetOpt = parser.accepts("print-offsets", "Print the offsets returned by the iterator")
                           .withOptionalArg
                           .describedAs("print offsets")
                           .ofType(classOf[java.lang.Boolean])
                           .defaultsTo(false)
    val printMessageOpt = parser.accepts("print-messages", "Print the messages returned by the iterator")
                           .withOptionalArg
                           .describedAs("print messages")
                           .ofType(classOf[java.lang.Boolean])
                           .defaultsTo(false)

    val options = parser.parse(args : _*)
    
    for(arg <- List(urlOpt, topicOpt)) {
      if(!options.has(arg)) {
        error("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val url = new URI(options.valueOf(urlOpt))
    val topic = options.valueOf(topicOpt)
    val partition = options.valueOf(partitionOpt).intValue
    val startingOffset = options.valueOf(offsetOpt).longValue
    val fetchsize = options.valueOf(fetchsizeOpt).intValue
    val printOffsets = if(options.has(printOffsetOpt)) true else false
    val printMessages = if(options.has(printMessageOpt)) true else false

    info("Starting consumer...")
    val consumer = new SimpleConsumer(url.getHost, url.getPort, 10000, 64*1024)
    val thread = Utils.newThread("kafka-consumer", new Runnable() {
      def run() {
        var offset = startingOffset
        while(true) {
          val fetchRequest = new FetchRequest(topic, partition, offset, fetchsize)
          val messageSets = consumer.multifetch(fetchRequest)
          for (messages <- messageSets) {
            debug("multi fetched " + messages.sizeInBytes + " bytes from offset " + offset)
            var consumed = 0
            for(messageAndOffset <- messages) {
              if(printMessages)
                info("consumed: " + Utils.toString(messageAndOffset.message.payload, "UTF-8"))
              offset = messageAndOffset.offset
              if(printOffsets)
                info("next offset = " + offset)
              consumed += 1
            }
          }
        }
      }
    }, false);
    thread.start()
    thread.join()
  }

}
