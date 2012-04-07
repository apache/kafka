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

package kafka.consumer

import java.util.concurrent._
import java.util.concurrent.atomic._
import scala.collection._
import junit.framework.Assert._

import kafka.cluster._
import kafka.message._
import kafka.server._
import org.scalatest.junit.JUnit3Suite
import kafka.integration.KafkaServerTestHarness
import kafka.utils.TestUtils

class FetcherTest extends JUnit3Suite with KafkaServerTestHarness {

  val numNodes = 2
  val configs = 
    for(props <- TestUtils.createBrokerConfigs(numNodes))
      yield new KafkaConfig(props) {
        override val enableZookeeper = false
      }
  val messages = new mutable.HashMap[Int, ByteBufferMessageSet]
  val topic = "topic"
  val cluster = new Cluster(configs.map(c => new Broker(c.brokerId, c.brokerId.toString, "localhost", c.port)))
  val shutdown = ZookeeperConsumerConnector.shutdownCommand
  val queue = new LinkedBlockingQueue[FetchedDataChunk]
  val topicInfos = configs.map(c => new PartitionTopicInfo(topic,
                                                      c.brokerId,
                                                      new Partition(c.brokerId, 0), 
                                                      queue, 
                                                      new AtomicLong(0), 
                                                      new AtomicLong(0), 
                                                      new AtomicInteger(0)))
  
  var fetcher: Fetcher = null

  override def setUp() {
    super.setUp
    fetcher = new Fetcher(new ConsumerConfig(TestUtils.createConsumerProperties("", "", "")), null)
    fetcher.stopConnectionsToAllBrokers
    fetcher.startConnections(topicInfos, cluster)
  }

  override def tearDown() {
    fetcher.stopConnectionsToAllBrokers
    super.tearDown
  }
    
  def testFetcher() {
    val perNode = 2
    var count = sendMessages(perNode)
    fetch(count)
    Thread.sleep(100)
    assertQueueEmpty()
    count = sendMessages(perNode)
    fetch(count)
    Thread.sleep(100)
    assertQueueEmpty()
  }
  
  def assertQueueEmpty(): Unit = assertEquals(0, queue.size)
  
  def sendMessages(messagesPerNode: Int): Int = {
    var count = 0
    for(conf <- configs) {
      val producer = TestUtils.createProducer("localhost", conf.port)
      val ms = 0.until(messagesPerNode).map(x => new Message((conf.brokerId * 5 + x).toString.getBytes)).toArray
      val mSet = new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = ms: _*)
      messages += conf.brokerId -> mSet
      producer.send(topic, mSet)
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
