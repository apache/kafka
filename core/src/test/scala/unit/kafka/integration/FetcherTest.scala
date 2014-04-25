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
import scala.collection._
import junit.framework.Assert._

import kafka.cluster._
import kafka.server._
import org.scalatest.junit.JUnit3Suite
import kafka.consumer._
import kafka.serializer._
import kafka.producer.{KeyedMessage, Producer}
import kafka.utils.TestUtils._
import kafka.utils.TestUtils

class FetcherTest extends JUnit3Suite with KafkaServerTestHarness {

  val numNodes = 1
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numNodes))
    yield new KafkaConfig(props)
  val messages = new mutable.HashMap[Int, Seq[Array[Byte]]]
  val topic = "topic"
  val cluster = new Cluster(configs.map(c => new Broker(c.brokerId, "localhost", c.port)))
  val shutdown = ZookeeperConsumerConnector.shutdownCommand
  val queue = new LinkedBlockingQueue[FetchedDataChunk]
  val topicInfos = configs.map(c => new PartitionTopicInfo(topic,
                                                           0,
                                                           queue,
                                                           new AtomicLong(0),
                                                           new AtomicLong(0),
                                                           new AtomicInteger(0),
                                                           ""))

  var fetcher: ConsumerFetcherManager = null

  override def setUp() {
    super.setUp
    createTopic(zkClient, topic, partitionReplicaAssignment = Map(0 -> Seq(configs.head.brokerId)), servers = servers)

    fetcher = new ConsumerFetcherManager("consumer1", new ConsumerConfig(TestUtils.createConsumerProperties("", "", "")), zkClient)
    fetcher.stopConnections()
    fetcher.startConnections(topicInfos, cluster)
  }

  override def tearDown() {
    fetcher.stopConnections()
    super.tearDown
  }

  def testFetcher() {
    val perNode = 2
    var count = sendMessages(perNode)

    fetch(count)
    assertQueueEmpty()
    count = sendMessages(perNode)
    fetch(count)
    assertQueueEmpty()
  }

  def assertQueueEmpty(): Unit = assertEquals(0, queue.size)

  def sendMessages(messagesPerNode: Int): Int = {
    var count = 0
    for(conf <- configs) {
      val producer: Producer[String, Array[Byte]] = TestUtils.createProducer(
        TestUtils.getBrokerListStrFromConfigs(configs),
        keyEncoder = classOf[StringEncoder].getName)
      val ms = 0.until(messagesPerNode).map(x => (conf.brokerId * 5 + x).toString.getBytes).toArray
      messages += conf.brokerId -> ms
      producer.send(ms.map(m => new KeyedMessage[String, Array[Byte]](topic, topic, m)):_*)
      producer.close()
      count += ms.size
    }
    count
  }

  def fetch(expected: Int) {
    var count = 0
    while(true) {
      val chunk = queue.poll(2L, TimeUnit.SECONDS)
      assertNotNull("Timed out waiting for data chunk " + (count + 1), chunk)
      for(message <- chunk.messages)
        count += 1
      if(count == expected)
        return
    }
  }

}
