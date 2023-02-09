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

package integration.kafka.api

import kafka.server.{BaseRequestTest, KafkaConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{CompressionType, DefaultRecordBatch, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

class ProduceRequestInvalidInputInterceptorTest extends BaseRequestTest {

  override def modifyConfigs(props: collection.Seq[Properties]): Unit = {
    props.foreach { p =>
      p.put(KafkaConfig.ProduceRequestInterceptorsProp, "integration.kafka.api.NoOpProduceRequestInterceptor")
    }
    super.modifyConfigs(props)
  }

  @Test
  def testInterceptorSkipsCorruptRequest(): Unit = {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val timestamp = 1000000
    val memoryRecords = MemoryRecords.withRecords(CompressionType.LZ4,
      new SimpleRecord(timestamp, "key".getBytes, "value".getBytes))
    // Change the lz4 checksum value (not the kafka record crc) so that it doesn't match the contents
    val lz4ChecksumOffset = 6
    memoryRecords.buffer.array.update(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
      .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
        new ProduceRequestData.TopicProduceData()
          .setName(topicPartition.topic())
          .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
            .setIndex(topicPartition.partition())
            .setRecords(memoryRecords)))).iterator))
      .setAcks((-1).toShort)
      .setTimeoutMs(3000)
      .setTransactionalId(null)).build()
    val response = connectAndReceive[ProduceResponse](produceRequest, destination = brokerSocketServer(leader))

    assertEquals(Errors.CORRUPT_MESSAGE, Errors.forCode(response.data().responses().asScala.head.partitionResponses().asScala.head.errorCode()))
  }

}
