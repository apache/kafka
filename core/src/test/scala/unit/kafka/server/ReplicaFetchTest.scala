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

import scala.collection.Seq

import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils
import TestUtils._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer

class ReplicaFetchTest extends ZooKeeperTestHarness  {
  var brokers: Seq[KafkaServer] = null
  val topic1 = "foo"
  val topic2 = "bar"

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    val props = createBrokerConfigs(2, zkConnect)
    brokers = props.map(KafkaConfig.fromProps).map(TestUtils.createServer(_))
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(brokers)
    super.tearDown()
  }

  @Test
  def testReplicaFetcherThread(): Unit = {
    val partition = 0
    val testMessageList1 = List("test1", "test2", "test3", "test4")
    val testMessageList2 = List("test5", "test6", "test7", "test8")

    // create a topic and partition and await leadership
    for (topic <- List(topic1,topic2)) {
      createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 2, servers = brokers)
    }

    // send test messages to leader
    val producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(brokers),
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
        val expectedOffset = brokers.head.getLogManager.getLog(tp).get.logEndOffset
        result = result && expectedOffset > 0 && brokers.forall { item =>
          expectedOffset == item.getLogManager.getLog(tp).get.logEndOffset
        }
      }
      result
    }
    waitUntilTrue(logsMatch _, "Broker logs should be identical")
  }
}
