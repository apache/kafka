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

package kafka.admin

import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{Utils, ZKStringSerializer, ZkUtils}

object DeleteTopicCommand {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to be deleted.")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple URLS can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])

    val options = parser.parse(args : _*)

    for(arg <- List(topicOpt, zkConnectOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val topic = options.valueOf(topicOpt)
    val zkConnect = options.valueOf(zkConnectOpt)
    var zkClient: ZkClient = null
    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
      zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))
      println("deletion succeeded!")
    }
    catch {
      case e =>
        println("delection failed because of " + e.getMessage)
        println(Utils.stackTrace(e))
    }
    finally {
      if (zkClient != null)
        zkClient.close()
    }
  }
}