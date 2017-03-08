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

package kafka.api

import java.util.Collections
import java.util.concurrent.TimeUnit

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.TimestampType
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class LogAppendTimeTest extends IntegrationTestHarness {

  val producerCount: Int = 1
  val consumerCount: Int = 1
  val serverCount: Int = 2

  serverConfig.put(KafkaConfig.LogMessageTimestampTypeProp, TimestampType.LOG_APPEND_TIME.name)
  serverConfig.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "2")

  @Test
  def testProduceConsume() {
    val producer = producers.head
    val producerRecords = (1 to 10).map(i => new ProducerRecord(listenerName.value, s"key$i".getBytes,
      s"value$i".getBytes))
    producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))

    val consumer = consumers.head
    consumer.subscribe(Collections.singleton(listenerName.value))
    val records = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    TestUtils.waitUntilTrue(() => {
      records ++= consumer.poll(50).asScala
      records.size == producerRecords.size
    }, s"Consumed ${records.size} records until timeout instead of the expected ${producerRecords.size} records")
  }

}
