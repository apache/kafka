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

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils._
import kafka.producer.KeyedMessage
import kafka.serializer.StringEncoder
import kafka.utils.TestUtils
import junit.framework.Assert._
import kafka.common._

class ReplicaFetchTest extends JUnit3Suite with ZooKeeperTestHarness  {
  val props = createBrokerConfigs(2,false)
  val configs = props.map(p => new KafkaConfig(p))
  var brokers: Seq[KafkaServer] = null
  val topic1 = "foo"
  val topic2 = "bar"

  override def setUp() {
    super.setUp()
    brokers = configs.map(config => TestUtils.createServer(config))
  }

  override def tearDown() {
    brokers.foreach(_.shutdown())
    super.tearDown()
  }

  def testReplicaFetcherThread() {
    val partition = 0
    val testMessageList1 = List("test1", "test2", "test3", "test4")
    val testMessageList2 = List("test5", "test6", "test7", "test8")

    // create a topic and partition and await leadership
    for (topic <- List(topic1,topic2)) {
      createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 2, servers = brokers)
    }

    // send test messages to leader
    val producer = TestUtils.createProducer[String, String](TestUtils.getBrokerListStrFromConfigs(configs),
                                                            encoder = classOf[StringEncoder].getName,
                                                            keyEncoder = classOf[StringEncoder].getName)
    val messages = testMessageList1.map(m => new KeyedMessage(topic1, m, m)) ++ testMessageList2.map(m => new KeyedMessage(topic2, m, m))
    producer.send(messages:_*)
    producer.close()

    def logsMatch(): Boolean = {
      var result = true
      for (topic <- List(topic1, topic2)) {
        val topicAndPart = TopicAndPartition(topic, partition)
        val expectedOffset = brokers.head.getLogManager().getLog(topicAndPart).get.logEndOffset
        result = result && expectedOffset > 0 && brokers.foldLeft(true) { (total, item) => total &&
          (expectedOffset == item.getLogManager().getLog(topicAndPart).get.logEndOffset) }
      }
      result
    }
    waitUntilTrue(logsMatch, "Broker logs should be identical")
  }
}
