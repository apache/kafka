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

import java.util.Properties
import joptsimple._
import kafka.admin.AdminClient
import kafka.client.ClientUtils
import kafka.cluster.BrokerEndPoint
import kafka.utils.{CommandLineUtils, ToolsUtils}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.requests.{ListOffsetRequest, MetadataRequest}
import org.apache.kafka.common.utils.Utils
import scala.collection.JavaConverters._
import scala.util.Random



object GetOffsetShell {

  val clientId = "GetOffsetShell"

  private def createAdminClient(props: Properties): AdminClient = {
    AdminClient.create(props)
  }
  private def getNode(brokerEndPoint: BrokerEndPoint): Node = {
    new Node(brokerEndPoint.id, brokerEndPoint.host, brokerEndPoint.port)
  }


  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The list of hostname and port of the server to connect to.")
      .withRequiredArg
      .describedAs("hostname:port,...,hostname:port")
      .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to get offset from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val partitionOpt = parser.accepts("partitions", "comma separated list of partition ids. If not specified, it will find offsets for all partitions")
      .withRequiredArg
      .describedAs("partition ids")
      .ofType(classOf[String])
      .defaultsTo("")
    val timeOpt = parser.accepts("time", " REQUIRED: timestamp of the offsets before that")
      .withRequiredArg
      .describedAs("timestamp/-1(latest)/-2(earliest)")
      .ofType(classOf[java.lang.Long])

    val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "An interactive shell for getting consumer offsets.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt, topicOpt, timeOpt)


    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)
    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)
    val topic = options.valueOf(topicOpt)

    val partitionList = options.valueOf(partitionOpt)
    val time = options.valueOf(timeOpt).longValue
    val commandConfig = if (options.has(commandConfigOpt)) {
      Utils.loadProps(options.valueOf(commandConfigOpt))
    } else new Properties()


    commandConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val adminClient = createAdminClient(commandConfig)

    val shuffledBrokers = Random.shuffle(metadataTargetBrokers)

    val metadataRes = adminClient.getMetadata(new MetadataRequest.Builder(List(topic).asJava), getNode(shuffledBrokers(0)))

    if(metadataRes.errors.containsKey(topic)){
      metadataRes.errors().get(topic).exception()
    }else{

      val topicsPartitions =  metadataRes.cluster().availablePartitionsForTopic(topic).asScala

      val partitions =
        if(partitionList == "") {
          topicsPartitions.map(_.partition())
        } else {
          partitionList.split(",").map(_.toInt).toSeq
        }

      partitions.foreach { partitionId: Int =>
        val partitionMetadata =  topicsPartitions.toList.find(_.partition == partitionId)
        partitionMetadata match {
          case Some(metadata) => {

            val partitions:java.util.Map[TopicPartition, java.lang.Long] = Map(new TopicPartition(metadata.topic(), metadata.partition()) ->
              java.lang.Long.valueOf(time)).asJava

            val request= ListOffsetRequest.Builder.forConsumer(true).setTargetTimes(partitions)

            val listOffset= adminClient.getTopicListOffset(request,metadata.leader() )

            listOffset.keys.foreach(topicPartition =>{
              val data = listOffset.get(topicPartition).get

              if (data.error.code() == Errors.NONE.code) {
                println("%s:%d:%s".format(topic, partitionId, data.offset ))
              } else {
                val errormessage =Errors.forCode(data.error.code()).exception.getMessage
                println(s"Attempt to fetch offsets for partition $topicPartition failed due to: $errormessage")
              }
            })

          }
        }
      }
    }

  }

}
