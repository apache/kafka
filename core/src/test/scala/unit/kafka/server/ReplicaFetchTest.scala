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

package kafka.server

import org.junit.jupiter.api.AfterEach
import kafka.utils.{TestInfoUtils, TestUtils}
import TestUtils._
import kafka.api.IntegrationTestHarness
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class ReplicaFetchTest extends IntegrationTestHarness {
  val topic1 = "foo"
  val topic2 = "bar"

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(brokers)
    super.tearDown()
  }

  override def brokerCount: Int = 2

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testReplicaFetcherThread(quorum: String): Unit = {
    val partition = 0
    val testMessageList1 = List("test1", "test2", "test3", "test4")
    val testMessageList2 = List("test5", "test6", "test7", "test8")

    // create a topic and partition and await leadership
    for (topic <- List(topic1,topic2)) {
      createTopic(topic, replicationFactor = 2)
    }

    // send test messages to leader
    val producer = TestUtils.createProducer(TestUtils.plaintextBootstrapServers(brokers),
                                               keySerializer = new StringSerializer,
                                               valueSerializer = new StringSerializer)
    val records = testMessageList1.map(m => new ProducerRecord(topic1, m, m)) ++
      testMessageList2.map(m => new ProducerRecord(topic2, m, m))
    records.map(producer.send).foreach(_.get)
    producer.close()

    def logsMatch(): Boolean = {
      var result = true
      for (topic <- List(topic1, topic2)) {
        val tp = new TopicPartition(topic, partition)
        val expectedOffset = brokers.head.logManager.getLog(tp).get.logEndOffset
        result = result && expectedOffset > 0 && brokers.forall { item =>
          expectedOffset == item.logManager.getLog(tp).get.logEndOffset
        }
      }
      result
    }
    waitUntilTrue(logsMatch _, "Broker logs should be identical")
  }
}
