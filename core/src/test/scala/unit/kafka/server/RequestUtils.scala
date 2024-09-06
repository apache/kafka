/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package unit.kafka.server

import kafka.network.SocketServer
import kafka.server.{ControllerServer, IntegrationTestUtils, KafkaBroker}
import kafka.test.ClusterInstance
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT

import java.util.Properties
import java.util.stream.Collectors
import scala.collection.Seq
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `map AsScala`}
import scala.reflect.ClassTag

class RequestUtils(cluster: ClusterInstance) {

  protected var producer: KafkaProducer[String, String] = _

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  protected def listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol)

  private def brokers(): Seq[KafkaBroker] = cluster.brokers.values().stream().collect(Collectors.toList[KafkaBroker]).toSeq

  private def controllerServers(): Seq[ControllerServer] = cluster.controllers().values().toSeq

  def createOffsetsTopic(): Unit = {
    TestUtils.createOffsetsTopicWithAdmin(
      admin = cluster.createAdminClient(),
      brokers = brokers(),
      controllers = controllerServers()
    )
  }

  def createTopic(topic: String,
                            numPartitions: Int): Unit = {
    TestUtils.createTopicWithAdmin(
      admin = cluster.createAdminClient(),
      brokers = brokers(),
      controllers = controllerServers(),
      topic = topic,
      numPartitions = numPartitions
    )
  }

  def createTopicAndReturnLeaders(topic: String,
                                               numPartitions: Int = 1,
                                               replicationFactor: Int = 1,
                                               topicConfig: Properties = new Properties): Map[TopicIdPartition, Int] = {
    val partitionToLeader = TestUtils.createTopicWithAdmin(
      admin = cluster.createAdminClient(),
      topic = topic,
      brokers = brokers(),
      controllers = controllerServers(),
      numPartitions = numPartitions,
      replicationFactor = replicationFactor,
      topicConfig = topicConfig
    )
    partitionToLeader.map { case (partition, leader) => new TopicIdPartition(getTopicIds(topic), new TopicPartition(topic, partition)) -> leader }
  }

  def getTopicIds: Map[String, Uuid] = {
    cluster.controllers().get(cluster.controllerIds().iterator().next()).controller.findAllTopicIds(ANONYMOUS_CONTEXT).get().toMap
  }

  def getBrokers: Seq[KafkaBroker] = {
    cluster.brokers.values().stream().collect(Collectors.toList[KafkaBroker]).toSeq
  }

  def brokerSocketServer(brokerId: Int): SocketServer = {
    getBrokers.find { broker =>
      broker.config.brokerId == brokerId
    }.map(_.socketServer).getOrElse(throw new IllegalStateException(s"Could not find broker with id $brokerId"))
  }

  def isUnstableApiEnabled: Boolean = {
    cluster.config.serverProperties.get("unstable.api.versions.enable") == "true"
  }

  def isNewGroupCoordinatorEnabled: Boolean = {
    cluster.config.serverProperties.get("group.coordinator.new.enable") == "true" ||
      cluster.config.serverProperties.get("group.coordinator.rebalance.protocols").contains("consumer")
  }

  def bootstrapServers(listenerName: ListenerName = listenerName): String = {
    TestUtils.bootstrapServers(getBrokers, listenerName)
  }

  def initProducer(): Unit = {
    producer = TestUtils.createProducer(cluster.bootstrapServers(),
      keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  def closeProducer(): Unit = {
    if( producer != null )
      producer.close()
  }

  def produceData(topicIdPartition: TopicIdPartition, numMessages: Int): Seq[RecordMetadata] = {
    val records = for {
      messageIndex <- 0 until numMessages
    } yield {
      val suffix = s"$topicIdPartition-$messageIndex"
      new ProducerRecord(topicIdPartition.topic, topicIdPartition.partition, s"key $suffix", s"value $suffix")
    }
    records.map(producer.send(_).get)
  }

  def produceData(topicIdPartition: TopicIdPartition, key: String, value: String): RecordMetadata = {
    producer.send(new ProducerRecord(topicIdPartition.topic, topicIdPartition.partition,
      key, value)).get
  }

  def connectAndReceive[T <: AbstractResponse](request: AbstractRequest)(implicit classTag: ClassTag[T]): T = {
    IntegrationTestUtils.connectAndReceive[T](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }

  def connectAndReceive[T <: AbstractResponse](request: AbstractRequest, destination: Int)(implicit classTag: ClassTag[T]): T = {
    IntegrationTestUtils.connectAndReceive[T](
      request,
      brokerSocketServer(destination),
      cluster.clientListener()
    )
  }
}
