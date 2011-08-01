/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import kafka.server._
import kafka.consumer.SimpleConsumer

/**
 * Performance test for the simple consumer
 */
object SimpleConsumerPerformance {

  def main(args: Array[String]) {
    
    val parser = new OptionParser
    val urlOpt = parser.accepts("server", "REQUIRED: The hostname of the server to connect to.")
                           .withRequiredArg
                           .describedAs("kafka://hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val fetchSizeOpt = parser.accepts("fetch-size", "REQUIRED: The fetch size to use for consumption.")
                           .withRequiredArg
                           .describedAs("bytes")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1024*1024)
    
    val options = parser.parse(args : _*)
    
    for(arg <- List(urlOpt, topicOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    
    val url = new URI(options.valueOf(urlOpt))
    val topic = options.valueOf(topicOpt)
    val fetchSize = options.valueOf(fetchSizeOpt).intValue
    
    val consumer = new SimpleConsumer(url.getHost, url.getPort, 30*1000, 2*fetchSize)
    val startMs = System.currentTimeMillis
    var done = false
    var totalRead = 0
    val reportingInterval = 100000
    var consumedInInterval = 0
    var offset: Long = 0L
    while(!done) {
      val messages = consumer.fetch(new FetchRequest(topic, 0, offset, fetchSize))
      var messagesRead = 0
      for(message <- messages)
        messagesRead += 1
      
      if(messagesRead == 0)
        done = true
      else
        offset += messages.validBytes
      
      totalRead += messagesRead
      consumedInInterval += messagesRead
      
      if(consumedInInterval > reportingInterval) {
        println("Bytes read: " + totalRead)
        consumedInInterval = 0
      }
    }
    val ellapsedSeconds = (System.currentTimeMillis - startMs) / 1000.0
    println(totalRead + " messages read, " + offset + " bytes")
    println("Messages/sec: " + totalRead / ellapsedSeconds)
    println("MB/sec: " + offset / ellapsedSeconds / (1024.0*1024.0))
    System.exit(0)
  }
  
}
