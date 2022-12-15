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
package kafka.server

import kafka.log.LogConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import java.util
import java.util.{Optional, Properties}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class BaseFetchRequestTest extends BaseRequestTest {

  protected var producer: KafkaProducer[String, String] = _

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.FetchMaxBytes, Int.MaxValue.toString)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (producer != null)
      producer.close()
    super.tearDown()
  }

  protected def createConsumerFetchRequest(
    maxResponseBytes: Int,
    maxPartitionBytes: Int,
    topicPartitions: Seq[TopicPartition],
    offsetMap: Map[TopicPartition, Long],
    version: Short,
    maxWaitMs: Int = Int.MaxValue,
    minBytes: Int = 0,
    rackId: String = ""
  ): FetchRequest = {
    FetchRequest.Builder.forConsumer(version, maxWaitMs, minBytes, createPartitionMap(maxPartitionBytes, topicPartitions, offsetMap))
      .setMaxBytes(maxResponseBytes)
      .rackId(rackId)
      .build()
  }

  protected def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                   offsetMap: Map[TopicPartition, Long] = Map.empty): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    topicPartitions.foreach { tp =>
      partitionMap.put(tp,
        new FetchRequest.PartitionData(getTopicIds().getOrElse(tp.topic, Uuid.ZERO_UUID), offsetMap.getOrElse(tp, 0), 0L, maxPartitionBytes,
        Optional.empty()))
    }
    partitionMap
  }

  protected def sendFetchRequest(brokerId: Int, request: FetchRequest): FetchResponse = {
    connectAndReceive[FetchResponse](request, destination = brokerSocketServer(brokerId))
  }

  protected def initProducer(): Unit = {
    producer = TestUtils.createProducer(bootstrapServers(),
      keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  protected def createTopics(numTopics: Int, numPartitions: Int, configs: Map[String, String] = Map.empty): Map[TopicPartition, Int] = {
    val topics = (0 until numTopics).map(t => s"topic$t")
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, 2.toString)
    configs.foreach { case (k, v) => topicConfig.setProperty(k, v) }
    topics.flatMap { topic =>
      val partitionToLeader = createTopic(topic, numPartitions = numPartitions, replicationFactor = 2,
        topicConfig = topicConfig)
      partitionToLeader.map { case (partition, leader) => new TopicPartition(topic, partition) -> leader }
    }.toMap
  }

  protected def produceData(topicPartitions: Iterable[TopicPartition], numMessagesPerPartition: Int): Seq[RecordMetadata] = {
    val records = for {
      tp <- topicPartitions.toSeq
      messageIndex <- 0 until numMessagesPerPartition
    } yield {
      val suffix = s"$tp-$messageIndex"
      new ProducerRecord(tp.topic, tp.partition, s"key $suffix", s"value $suffix")
    }
    records.map(producer.send(_).get)
  }

  protected def records(partitionData: FetchResponseData.PartitionData): Seq[Record] = {
    FetchResponse.recordsOrFail(partitionData).records.asScala.toBuffer
  }

}
