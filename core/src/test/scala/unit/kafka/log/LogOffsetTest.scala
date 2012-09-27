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

package kafka.log

import java.io.File
import kafka.utils._
import kafka.server.{KafkaConfig, KafkaServer}
import junit.framework.Assert._
import java.util.{Random, Properties}
import kafka.consumer.SimpleConsumer
import org.junit.{After, Before, Test}
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet, Message}
import kafka.zk.ZooKeeperTestHarness
import org.scalatest.junit.JUnit3Suite
import kafka.admin.CreateTopicCommand
import kafka.api.{PartitionOffsetRequestInfo, FetchRequestBuilder, OffsetRequest}
import kafka.utils.TestUtils._
import kafka.common.{ErrorMapping, TopicAndPartition, UnknownTopicOrPartitionException}

object LogOffsetTest {
  val random = new Random()  
}

class LogOffsetTest extends JUnit3Suite with ZooKeeperTestHarness {
  var logDir: File = null
  var topicLogDir: File = null
  var server: KafkaServer = null
  var logSize: Int = 100
  val brokerPort: Int = 9099
  var simpleConsumer: SimpleConsumer = null
  var time: Time = new MockTime()

  @Before
  override def setUp() {
    super.setUp()
    val config: Properties = createBrokerConfig(1, brokerPort)
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    time = new MockTime()
    server = TestUtils.createServer(new KafkaConfig(config), time)
    simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64*1024)
  }

  @After
  override def tearDown() {
    simpleConsumer.close
    server.shutdown
    Utils.rm(logDir)
    super.tearDown()
  }

  @Test
  def testGetOffsetsForUnknownTopic() {
    val topicAndPartition = TopicAndPartition("foo", 0)
    val request = OffsetRequest(
      Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 10)))
    val offsetResponse = simpleConsumer.getOffsetsBefore(request)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
                 offsetResponse.partitionErrorAndOffsets(topicAndPartition).error)
  }

  @Test
  def testGetOffsetsBeforeLatestTime() {
    val topicPartition = "kafka-" + 0
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    // setup brokers in zookeeper as owners of partitions for this test
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "1")

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)

    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, message))
    log.flush()

    val offsets = log.getOffsetsBefore(OffsetRequest.LatestTime, 10)
    assertEquals(Seq(240L, 216L, 108L, 0L), offsets)

    waitUntilTrue(() => isLeaderLocalOnBroker(topic, part, server), 1000)
    val topicAndPartition = TopicAndPartition(topic, part)
    val offsetRequest = OffsetRequest(
      Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 10)),
      replicaId = 0)
    val consumerOffsets =
      simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition).offsets
    assertEquals(Seq(240L, 216L, 108L, 0L), consumerOffsets)

    // try to fetch using latest offset
    val fetchResponse = simpleConsumer.fetch(
      new FetchRequestBuilder().addFetch(topic, 0, consumerOffsets.head, 300 * 1024).build())
    assertFalse(fetchResponse.messageSet(topic, 0).iterator.hasNext)
  }

  @Test
  def testEmptyLogsGetOffsets() {
    val topicPartition = "kafka-" + LogOffsetTest.random.nextInt(10)
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    topicLogDir = new File(topicPartitionPath)
    topicLogDir.mkdir

    val topic = topicPartition.split("-").head

    // setup brokers in zookeeper as owners of partitions for this test
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "1")
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)

    var offsetChanged = false
    for(i <- 1 to 14) {
      val topicAndPartition = TopicAndPartition(topic, 0)
      val offsetRequest =
        OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
      val consumerOffsets =
        simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition).offsets

      if(consumerOffsets(0) == 1) {
        offsetChanged = true
      }
    }
    assertFalse(offsetChanged)
  }

  @Test
  def testGetOffsetsBeforeNow() {
    val topicPartition = "kafka-" + LogOffsetTest.random.nextInt(3)
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    // setup brokers in zookeeper as owners of partitions for this test
    CreateTopicCommand.createTopic(zkClient, topic, 3, 1, "1,1,1")

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)
    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, message))
    log.flush()

    time.sleep(20)
    val now = time.milliseconds

    val offsets = log.getOffsetsBefore(now, 10)
    assertEquals(Seq(240L, 216L, 108L, 0L), offsets)

    waitUntilTrue(() => isLeaderLocalOnBroker(topic, part, server), 1000)
    val topicAndPartition = TopicAndPartition(topic, part)
    val offsetRequest = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(now, 10)), replicaId = 0)
    val consumerOffsets =
      simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition).offsets
    assertEquals(Seq(240L, 216L, 108L, 0L), consumerOffsets)
  }

  @Test
  def testGetOffsetsBeforeEarliestTime() {
    val topicPartition = "kafka-" + LogOffsetTest.random.nextInt(3)
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    // setup brokers in zookeeper as owners of partitions for this test
    CreateTopicCommand.createTopic(zkClient, topic, 3, 1, "1,1,1")

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)
    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, message))
    log.flush()

    val offsets = log.getOffsetsBefore(OffsetRequest.EarliestTime, 10)

    assertEquals(Seq(0L), offsets)

    waitUntilTrue(() => isLeaderLocalOnBroker(topic, part, server), 1000)
    val topicAndPartition = TopicAndPartition(topic, part)
    val offsetRequest =
      OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 10)))
    val consumerOffsets =
      simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition).offsets
    assertEquals(Seq(0L), consumerOffsets)
  }

  private def createBrokerConfig(nodeId: Int, port: Int): Properties = {
    val props = new Properties
    props.put("brokerid", nodeId.toString)
    props.put("port", port.toString)
    props.put("log.dir", getLogDir.getAbsolutePath)
    props.put("log.flush.interval", "1")
    props.put("enable.zookeeper", "false")
    props.put("num.partitions", "20")
    props.put("log.retention.hours", "10")
    props.put("log.cleanup.interval.mins", "5")
    props.put("log.file.size", logSize.toString)
    props.put("zk.connect", zkConnect.toString)
    props
  }

  private def getLogDir(): File = {
    val dir = TestUtils.tempDir()
    dir
  }

}
