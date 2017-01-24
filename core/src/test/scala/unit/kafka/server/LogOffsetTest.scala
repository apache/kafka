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

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import java.util.{Properties, Random}

import kafka.admin.AdminUtils
import kafka.api.{FetchRequestBuilder, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.log.{Log, LogSegment}
import kafka.utils.TestUtils._
import kafka.utils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, Record}
import org.apache.kafka.common.utils.{Time, Utils}
import org.easymock.{EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

class LogOffsetTest extends ZooKeeperTestHarness {
  val random = new Random()
  var logDir: File = null
  var topicLogDir: File = null
  var server: KafkaServer = null
  var logSize: Int = 100
  var simpleConsumer: SimpleConsumer = null
  var time: Time = new MockTime()

  @Before
  override def setUp() {
    super.setUp()
    val config: Properties = createBrokerConfig(1)
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    time = new MockTime()
    server = TestUtils.createServer(KafkaConfig.fromProps(config), time)
    simpleConsumer = new SimpleConsumer("localhost", TestUtils.boundPort(server), 1000000, 64*1024, "")
  }

  @After
  override def tearDown() {
    simpleConsumer.close
    server.shutdown
    Utils.delete(logDir)
    super.tearDown()
  }

  @Test
  def testGetOffsetsForUnknownTopic() {
    val topicAndPartition = TopicAndPartition("foo", 0)
    val request = OffsetRequest(
      Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 10)))
    val offsetResponse = simpleConsumer.getOffsetsBefore(request)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
                 offsetResponse.partitionErrorAndOffsets(topicAndPartition).error)
  }

  @Test
  def testGetOffsetsBeforeLatestTime() {
    val topicPartition = "kafka-" + 0
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    // setup brokers in zookeeper as owners of partitions for this test
    AdminUtils.createTopic(zkUtils, topic, 1, 1)

    val logManager = server.getLogManager
    waitUntilTrue(() => logManager.getLog(new TopicPartition(topic, part)).isDefined,
                  "Log for partition [topic,0] should be created")
    val log = logManager.getLog(new TopicPartition(topic, part)).get

    val record = Record.create(Integer.toString(42).getBytes())
    for (_ <- 0 until 20)
      log.append(MemoryRecords.withRecords(record))
    log.flush()

    val offsets = server.apis.fetchOffsets(logManager, new TopicPartition(topic, part), OffsetRequest.LatestTime, 15)
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), offsets)

    waitUntilTrue(() => isLeaderLocalOnBroker(topic, part, server), "Leader should be elected")
    val topicAndPartition = TopicAndPartition(topic, part)
    val offsetRequest = OffsetRequest(
      Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 15)),
      replicaId = 0)
    val consumerOffsets =
      simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition).offsets
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), consumerOffsets)

    // try to fetch using latest offset
    val fetchResponse = simpleConsumer.fetch(
      new FetchRequestBuilder().addFetch(topic, 0, consumerOffsets.head, 300 * 1024).build())
    assertFalse(fetchResponse.messageSet(topic, 0).iterator.hasNext)
  }

  @Test
  def testEmptyLogsGetOffsets() {
    val topicPartition = "kafka-" + random.nextInt(10)
    val topicPartitionPath = TestUtils.tempDir().getAbsolutePath + "/" + topicPartition
    topicLogDir = new File(topicPartitionPath)
    topicLogDir.mkdir()

    val topic = topicPartition.split("-").head

    // setup brokers in zookeeper as owners of partitions for this test
    createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = Seq(server))

    var offsetChanged = false
    for (_ <- 1 to 14) {
      val topicAndPartition = TopicAndPartition(topic, 0)
      val offsetRequest =
        OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
      val consumerOffsets =
        simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition).offsets

      if(consumerOffsets.head == 1) {
        offsetChanged = true
      }
    }
    assertFalse(offsetChanged)
  }

  @Test
  def testGetOffsetsBeforeNow() {
    val topicPartition = "kafka-" + random.nextInt(3)
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    // setup brokers in zookeeper as owners of partitions for this test
    AdminUtils.createTopic(zkUtils, topic, 3, 1)

    val logManager = server.getLogManager
    val log = logManager.createLog(new TopicPartition(topic, part), logManager.defaultConfig)
    val record = Record.create(Integer.toString(42).getBytes())
    for (_ <- 0 until 20)
      log.append(MemoryRecords.withRecords(record))
    log.flush()

    val now = time.milliseconds + 30000 // pretend it is the future to avoid race conditions with the fs

    val offsets = server.apis.fetchOffsets(logManager, new TopicPartition(topic, part), now, 15)
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), offsets)

    waitUntilTrue(() => isLeaderLocalOnBroker(topic, part, server), "Leader should be elected")
    val topicAndPartition = TopicAndPartition(topic, part)
    val offsetRequest = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(now, 15)), replicaId = 0)
    val consumerOffsets =
      simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition).offsets
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), consumerOffsets)
  }

  @Test
  def testGetOffsetsBeforeEarliestTime() {
    val topicPartition = "kafka-" + random.nextInt(3)
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    // setup brokers in zookeeper as owners of partitions for this test
    AdminUtils.createTopic(zkUtils, topic, 3, 1)

    val logManager = server.getLogManager
    val log = logManager.createLog(new TopicPartition(topic, part), logManager.defaultConfig)
    val record = Record.create(Integer.toString(42).getBytes())
    for (_ <- 0 until 20)
      log.append(MemoryRecords.withRecords(record))
    log.flush()

    val offsets = server.apis.fetchOffsets(logManager, new TopicPartition(topic, part), OffsetRequest.EarliestTime, 10)

    assertEquals(Seq(0L), offsets)

    waitUntilTrue(() => isLeaderLocalOnBroker(topic, part, server), "Leader should be elected")
    val topicAndPartition = TopicAndPartition(topic, part)
    val offsetRequest =
      OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 10)))
    val consumerOffsets =
      simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition).offsets
    assertEquals(Seq(0L), consumerOffsets)
  }

  /* We test that `fetchOffsetsBefore` works correctly if `LogSegment.size` changes after each invocation (simulating
   * a race condition) */
  @Test
  def testFetchOffsetsBeforeWithChangingSegmentSize() {
    val log = EasyMock.niceMock(classOf[Log])
    val logSegment = EasyMock.niceMock(classOf[LogSegment])
    EasyMock.expect(logSegment.size).andStubAnswer(new IAnswer[Long] {
      private val value = new AtomicLong(0)
      def answer: Long = value.getAndIncrement()
    })
    EasyMock.replay(logSegment)
    val logSegments = Seq(logSegment)
    EasyMock.expect(log.logSegments).andStubReturn(logSegments)
    EasyMock.replay(log)
    server.apis.fetchOffsetsBefore(log, System.currentTimeMillis, 100)
  }

  /* We test that `fetchOffsetsBefore` works correctly if `Log.logSegments` content and size are
   * different (simulating a race condition) */
  @Test
  def testFetchOffsetsBeforeWithChangingSegments() {
    val log = EasyMock.niceMock(classOf[Log])
    val logSegment = EasyMock.niceMock(classOf[LogSegment])
    EasyMock.expect(log.logSegments).andStubAnswer {
      new IAnswer[Iterable[LogSegment]] {
        def answer = new Iterable[LogSegment] {
          override def size = 2
          def iterator = Seq(logSegment).iterator
        }
      }
    }
    EasyMock.replay(logSegment)
    EasyMock.replay(log)
    server.apis.fetchOffsetsBefore(log, System.currentTimeMillis, 100)
  }

  private def createBrokerConfig(nodeId: Int): Properties = {
    val props = new Properties
    props.put("broker.id", nodeId.toString)
    props.put("port", TestUtils.RandomPort.toString())
    props.put("log.dir", TestUtils.tempDir().getAbsolutePath)
    props.put("log.flush.interval.messages", "1")
    props.put("enable.zookeeper", "false")
    props.put("num.partitions", "20")
    props.put("log.retention.hours", "10")
    props.put("log.retention.check.interval.ms", (5*1000*60).toString)
    props.put("log.segment.bytes", logSize.toString)
    props.put("zookeeper.connect", zkConnect.toString)
    props
  }

}
