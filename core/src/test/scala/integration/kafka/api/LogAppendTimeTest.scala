/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.api

import java.util.Collections
import java.util.concurrent.TimeUnit

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.TimestampType
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertNotEquals, assertTrue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Tests where the broker is configured to use LogAppendTime. For tests where LogAppendTime is configured via topic
  * level configs, see the *ProducerSendTest classes.
  */
class LogAppendTimeTest extends IntegrationTestHarness {
  val producerCount: Int = 1
  val consumerCount: Int = 1
  val serverCount: Int = 2

  // This will be used for the offsets topic as well
  serverConfig.put(KafkaConfig.LogMessageTimestampTypeProp, TimestampType.LOG_APPEND_TIME.name)
  serverConfig.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "2")

  private val topic = "topic"

  @Before
  override def setUp(): Unit = {
    super.setUp()
    createTopic(topic)
  }

  @Test
  def testProduceConsume(): Unit = {
    val producer = producers.head
    val now = System.currentTimeMillis()
    val createTime = now - TimeUnit.DAYS.toMillis(1)
    val producerRecords = (1 to 10).map(i => new ProducerRecord(topic, null, createTime, s"key$i".getBytes,
      s"value$i".getBytes))
    val recordMetadatas = producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))
    recordMetadatas.foreach { recordMetadata =>
      assertTrue(recordMetadata.timestamp >= now)
      assertTrue(recordMetadata.timestamp < now + TimeUnit.SECONDS.toMillis(60))
    }

    val consumer = consumers.head
    consumer.subscribe(Collections.singleton(topic))
    val consumerRecords = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    TestUtils.waitUntilTrue(() => {
      consumerRecords ++= consumer.poll(50).asScala
      consumerRecords.size == producerRecords.size
    }, s"Consumed ${consumerRecords.size} records until timeout instead of the expected ${producerRecords.size} records")

    consumerRecords.zipWithIndex.foreach { case (consumerRecord, index) =>
      val producerRecord = producerRecords(index)
      val recordMetadata = recordMetadatas(index)
      assertEquals(new String(producerRecord.key), new String(consumerRecord.key))
      assertEquals(new String(producerRecord.value), new String(consumerRecord.value))
      assertNotEquals(producerRecord.timestamp, consumerRecord.timestamp)
      assertEquals(recordMetadata.timestamp, consumerRecord.timestamp)
      assertEquals(TimestampType.LOG_APPEND_TIME, consumerRecord.timestampType)
    }
  }

}
