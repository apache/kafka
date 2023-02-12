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

import kafka.server.KafkaConfig
import kafka.utils.TestInfoUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.server.interceptors.{ProduceRequestInterceptor, ProduceRequestInterceptorResult}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.regex.Pattern

class MutationProduceRequestInterceptor extends ProduceRequestInterceptor {

  override def processRecord(key: Array[Byte], value: Array[Byte], topic: String, partition: Int, headers: Array[Header]): ProduceRequestInterceptorResult = {
    val newValue = {
      if (value == null) null
      else {
        val s = new String(value, StandardCharsets.UTF_8)
        s"mutated-$s".getBytes(StandardCharsets.UTF_8)
      }
    }
    new ProduceRequestInterceptorResult(key, newValue)
  }

  override def interceptorTopicPattern(): Pattern = Pattern.compile(".*")

  override def configure(): Unit = ()
}

class ProduceRequestMutationInterceptorTest extends ProducerSendTestHelpers {

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.put(KafkaConfig.ProduceRequestInterceptorsProp, "integration.kafka.api.MutationProduceRequestInterceptor")
    baseProps.map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInterceptorMutatesRecords(quorum: String): Unit = {
    val recordAssertion: (ConsumerRecord[Array[Byte], Array[Byte]], Int, Long, String, Int) => Unit = (record, i, now, topic, partition) => {
      assertEquals(topic, record.topic)
      assertEquals(partition, record.partition)
      assertEquals(i.toLong, record.offset)
      assertNull(record.key)
      assertEquals(s"mutated-value${i + 1}", new String(record.value))
      assertEquals(now, record.timestamp)
    }
    sendToPartition(quorum, numRecords, recordAssertion)
  }

}
