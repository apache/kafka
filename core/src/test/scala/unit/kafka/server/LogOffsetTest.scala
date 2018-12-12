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
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Optional, Properties, Random}

import kafka.log.{Log, LogSegment}
import kafka.network.SocketServer
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, IsolationLevel, ListOffsetRequest, ListOffsetResponse}
import org.easymock.{EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class LogOffsetTest extends BaseRequestTest {

  private lazy val time = new MockTime

  protected override def numBrokers = 1

  protected override def brokerTime(brokerId: Int) = time

  protected override def propertyOverrides(props: Properties): Unit = {
    props.put("log.flush.interval.messages", "1")
    props.put("num.partitions", "20")
    props.put("log.retention.hours", "10")
    props.put("log.retention.check.interval.ms", (5 * 1000 * 60).toString)
    props.put("log.segment.bytes", "140")
  }

  @deprecated("ListOffsetsRequest V0", since = "")
  @Test
  def testGetOffsetsForUnknownTopic() {
    val topicPartition = new TopicPartition("foo", 0)
    val request = ListOffsetRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(Map(topicPartition ->
        new ListOffsetRequest.PartitionData(ListOffsetRequest.LATEST_TIMESTAMP, 10)).asJava).build(0)
    val response = sendListOffsetsRequest(request)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.responseData.get(topicPartition).error)
  }

  @deprecated("ListOffsetsRequest V0", since = "")
  @Test
  def testGetOffsetsAfterDeleteRecords() {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)

    createTopic(topic, 1, 1)

    val logManager = server.getLogManager
    TestUtils.waitUntilTrue(() => logManager.getLog(topicPartition).isDefined,
                  "Log for partition [topic,0] should be created")
    val log = logManager.getLog(topicPartition).get

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush()

    log.onHighWatermarkIncremented(log.logEndOffset)
    log.maybeIncrementLogStartOffset(3)
    log.deleteOldSegments()

    val offsets = log.legacyFetchOffsetsBefore(ListOffsetRequest.LATEST_TIMESTAMP, 15)
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 3L), offsets)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")
    val request = ListOffsetRequest.Builder.forReplica(0, 0)
      .setTargetTimes(Map(topicPartition ->
        new ListOffsetRequest.PartitionData(ListOffsetRequest.LATEST_TIMESTAMP, 15)).asJava).build()
    val consumerOffsets = sendListOffsetsRequest(request).responseData.get(topicPartition).offsets.asScala
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 3L), consumerOffsets)
  }

  @Test
  def testGetOffsetsBeforeLatestTime() {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)

    createTopic(topic, 1, 1)

    val logManager = server.getLogManager
    TestUtils.waitUntilTrue(() => logManager.getLog(topicPartition).isDefined,
      s"Log for partition $topicPartition should be created")
    val log = logManager.getLog(topicPartition).get

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush()

    val offsets = log.legacyFetchOffsetsBefore(ListOffsetRequest.LATEST_TIMESTAMP, 15)
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), offsets)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")
    val request = ListOffsetRequest.Builder.forReplica(0, 0)
      .setTargetTimes(Map(topicPartition ->
        new ListOffsetRequest.PartitionData(ListOffsetRequest.LATEST_TIMESTAMP, 15)).asJava).build()
    val consumerOffsets = sendListOffsetsRequest(request).responseData.get(topicPartition).offsets.asScala
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), consumerOffsets)

    // try to fetch using latest offset
    val fetchRequest = FetchRequest.Builder.forConsumer(0, 1,
      Map(topicPartition -> new FetchRequest.PartitionData(consumerOffsets.head, FetchRequest.INVALID_LOG_START_OFFSET,
        300 * 1024, Optional.empty())).asJava).build()
    val fetchResponse = sendFetchRequest(fetchRequest)
    assertFalse(fetchResponse.responseData.get(topicPartition).records.batches.iterator.hasNext)
  }

  @Test
  def testEmptyLogsGetOffsets() {
    val random = new Random
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, random.nextInt(10))
    val topicPartitionPath = s"${TestUtils.tempDir().getAbsolutePath}/$topic-${topicPartition.partition}"
    val topicLogDir = new File(topicPartitionPath)
    topicLogDir.mkdir()

    createTopic(topic, numPartitions = 1, replicationFactor = 1)

    var offsetChanged = false
    for (_ <- 1 to 14) {
      val topicPartition = new TopicPartition(topic, 0)
      val request = ListOffsetRequest.Builder.forReplica(0, 0)
        .setTargetTimes(Map(topicPartition ->
          new ListOffsetRequest.PartitionData(ListOffsetRequest.EARLIEST_TIMESTAMP, 1)).asJava).build()
      val consumerOffsets = sendListOffsetsRequest(request).responseData.get(topicPartition).offsets.asScala
      if (consumerOffsets.head == 1)
        offsetChanged = true
    }
    assertFalse(offsetChanged)
  }

  @deprecated("legacyFetchOffsetsBefore", since = "")
  @Test
  def testGetOffsetsBeforeNow() {
    val random = new Random
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, random.nextInt(3))

    createTopic(topic, 3, 1)

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topicPartition, logManager.initialDefaultConfig)

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush()

    val now = time.milliseconds + 30000 // pretend it is the future to avoid race conditions with the fs

    val offsets = log.legacyFetchOffsetsBefore(now, 15)
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), offsets)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")
    val request = ListOffsetRequest.Builder.forReplica(0, 0)
      .setTargetTimes(Map(topicPartition ->
        new ListOffsetRequest.PartitionData(now, 15)).asJava).build()
    val consumerOffsets = sendListOffsetsRequest(request).responseData.get(topicPartition).offsets.asScala
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), consumerOffsets)
  }

  @deprecated("legacyFetchOffsetsBefore", since = "")
  @Test
  def testGetOffsetsBeforeEarliestTime() {
    val random = new Random
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, random.nextInt(3))

    createTopic(topic, 3, 1)

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topicPartition, logManager.initialDefaultConfig)
    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush()

    val offsets = log.legacyFetchOffsetsBefore(ListOffsetRequest.EARLIEST_TIMESTAMP, 10)

    assertEquals(Seq(0L), offsets)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")
    val request = ListOffsetRequest.Builder.forReplica(0, 0)
      .setTargetTimes(Map(topicPartition ->
        new ListOffsetRequest.PartitionData(ListOffsetRequest.EARLIEST_TIMESTAMP, 10)).asJava).build()
    val consumerOffsets = sendListOffsetsRequest(request).responseData.get(topicPartition).offsets.asScala
    assertEquals(Seq(0L), consumerOffsets)
  }

  /* We test that `fetchOffsetsBefore` works correctly if `LogSegment.size` changes after each invocation (simulating
   * a race condition) */
  @Test
  def testFetchOffsetsBeforeWithChangingSegmentSize() {
    val log: Log = EasyMock.niceMock(classOf[Log])
    val logSegment: LogSegment = EasyMock.niceMock(classOf[LogSegment])
    EasyMock.expect(logSegment.size).andStubAnswer(new IAnswer[Int] {
      private val value = new AtomicInteger(0)
      def answer: Int = value.getAndIncrement()
    })
    EasyMock.replay(logSegment)
    val logSegments = Seq(logSegment)
    EasyMock.expect(log.logSegments).andStubReturn(logSegments)
    EasyMock.replay(log)
    log.legacyFetchOffsetsBefore(System.currentTimeMillis, 100)
  }

  /* We test that `fetchOffsetsBefore` works correctly if `Log.logSegments` content and size are
   * different (simulating a race condition) */
  @Test
  def testFetchOffsetsBeforeWithChangingSegments() {
    val log: Log = EasyMock.niceMock(classOf[Log])
    val logSegment: LogSegment = EasyMock.niceMock(classOf[LogSegment])
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
    log.legacyFetchOffsetsBefore(System.currentTimeMillis, 100)
  }

  private def server: KafkaServer = servers.head

  private def sendListOffsetsRequest(request: ListOffsetRequest, destination: Option[SocketServer] = None): ListOffsetResponse = {
    val response = connectAndSend(request, ApiKeys.LIST_OFFSETS, destination = destination.getOrElse(anySocketServer))
    ListOffsetResponse.parse(response, request.version)
  }

  private def sendFetchRequest(request: FetchRequest, destination: Option[SocketServer] = None): FetchResponse[MemoryRecords] = {
    val response = connectAndSend(request, ApiKeys.FETCH, destination = destination.getOrElse(anySocketServer))
    FetchResponse.parse(response, request.version)
  }

}
