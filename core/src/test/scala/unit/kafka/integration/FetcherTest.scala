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

package kafka.integration

import java.util.concurrent._
import java.util.concurrent.atomic._

import org.junit.{After, Before, Test}

import scala.collection._
import org.junit.Assert._
import kafka.cluster._
import kafka.server._
import kafka.consumer._
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import org.apache.kafka.common.security.JaasUtils

@deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
class FetcherTest extends KafkaServerTestHarness {
  val numNodes = 1
  def generateConfigs = TestUtils.createBrokerConfigs(numNodes, zkConnect).map(KafkaConfig.fromProps)

  val messages = new mutable.HashMap[Int, Seq[Array[Byte]]]
  val topic = "topic"
  val queue = new LinkedBlockingQueue[FetchedDataChunk]

  var fetcher: ConsumerFetcherManager = null
  var zkUtils: ZkUtils = null

  @Before
  override def setUp() {
    super.setUp
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled))

    createTopic(topic, partitionReplicaAssignment = Map(0 -> Seq(configs.head.brokerId)))

    val cluster = new Cluster(servers.map { s =>
      new Broker(s.config.brokerId, "localhost", boundPort(s), listenerName, securityProtocol)
    })

    fetcher = new ConsumerFetcherManager("consumer1", new ConsumerConfig(TestUtils.createConsumerProperties("", "", "")), zkUtils)
    fetcher.stopConnections()
    val topicInfos = configs.map(_ =>
      new PartitionTopicInfo(topic,
        0,
        queue,
        new AtomicLong(0),
        new AtomicLong(0),
        new AtomicInteger(0),
        ""))
    fetcher.startConnections(topicInfos, cluster)
  }

  @After
  override def tearDown() {
    fetcher.stopConnections()
    if (zkUtils != null)
     CoreUtils.swallow(zkUtils.close(), this)
    super.tearDown
  }

  @Test
  def testFetcher() {
    val perNode = 2
    var count = TestUtils.generateAndProduceMessages(servers, topic, perNode).size

    fetch(count)
    assertQueueEmpty()
    count = TestUtils.generateAndProduceMessages(servers, topic, perNode).size
    fetch(count)
    assertQueueEmpty()
  }

  def assertQueueEmpty(): Unit = assertEquals(0, queue.size)

  def fetch(expected: Int) {
    var count = 0
    while (count < expected) {
      val chunk = queue.poll(2L, TimeUnit.SECONDS)
      assertNotNull("Timed out waiting for data chunk " + (count + 1), chunk)
      count += chunk.messages.size
    }
  }
}
