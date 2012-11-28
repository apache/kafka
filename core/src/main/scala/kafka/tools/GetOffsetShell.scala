/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package kafka.tools

import kafka.consumer._
import joptsimple._
import java.net.URI

object GetOffsetShell {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val urlOpt = parser.accepts("server", "REQUIRED: The hostname of the server to connect to.")
                           .withRequiredArg
                           .describedAs("kafka://hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to get offset from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val partitionOpt = parser.accepts("partition", "partition id")
                           .withRequiredArg
                           .describedAs("partition id")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(0)
    val timeOpt = parser.accepts("time", "timestamp of the offsets before that")
                           .withRequiredArg
                           .describedAs("timestamp/-1(latest)/-2(earliest)")
                           .ofType(classOf[java.lang.Long])
    val nOffsetsOpt = parser.accepts("offsets", "number of offsets returned")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)

    val options = parser.parse(args : _*)

    for(arg <- List(urlOpt, topicOpt, timeOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val url = new URI(options.valueOf(urlOpt))
    val topic = options.valueOf(topicOpt)
    val partition = options.valueOf(partitionOpt).intValue
    var time = options.valueOf(timeOpt).longValue
    val nOffsets = options.valueOf(nOffsetsOpt).intValue
    val consumer = new SimpleConsumer(url.getHost, url.getPort, 10000, 100000)
    val offsets = consumer.getOffsetsBefore(topic, partition, time, nOffsets)
    println("get " + offsets.length + " results")
    for (offset <- offsets)
      println(offset)
  }
}
