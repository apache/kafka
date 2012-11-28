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

import java.io._
import joptsimple._
import kafka.producer._
import kafka.utils.Utils

/**
 * Interactive shell for producing messages from the command line
 */
object ProducerShell {

  def main(args: Array[String]) {
    
    val parser = new OptionParser
    val producerPropsOpt = parser.accepts("props", "REQUIRED: Properties file with the producer properties.")
                           .withRequiredArg
                           .describedAs("properties")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to produce to.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    
    val options = parser.parse(args : _*)
    
    for(arg <- List(producerPropsOpt, topicOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    
    val propsFile = options.valueOf(producerPropsOpt)
    val producerConfig = new ProducerConfig(Utils.loadProps(propsFile))
    val topic = options.valueOf(topicOpt)
    val producer = new Producer[String, String](producerConfig)

    val input = new BufferedReader(new InputStreamReader(System.in))
    var done = false
    while(!done) {
      val line = input.readLine()
      if(line == null) {
        done = true
      } else {
        val message = line.trim
        producer.send(new ProducerData[String, String](topic, message))
        println("Sent: %s (%d bytes)".format(line, message.getBytes.length))
      }
    }
    producer.close()
  }
}
