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
import kafka.common.ErrorMapping

object ListTopicCommand {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to be deleted.")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
                         .defaultsTo("")
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple URLS can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])

    val options = parser.parse(args : _*)

    for(arg <- List(zkConnectOpt)) {
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
      var topicList: Seq[String] = Nil
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)

      if (topic == "")
        topicList = ZkUtils.getChildren(zkClient, ZkUtils.BrokerTopicsPath)
      else
        topicList = List(topic)

      if (topicList.size <= 0)
        println("no topics exist!")

      for (t <- topicList)
        showTopic(t, zkClient)
    }
    catch {
      case e =>
        println("list topic failed because of " + e.getMessage)
        println(Utils.stackTrace(e))
    }
    finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def showTopic(topic: String, zkClient: ZkClient) {
    val topicMetaData = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
    topicMetaData.errorCode match {
      case ErrorMapping.UnknownTopicOrPartitionCode =>
        println("topic " + topic + " doesn't exist!")
      case _ =>
        println("topic: " + topic)
        for (part <- topicMetaData.partitionsMetadata)
          println(part.toString)
    }
  }
}