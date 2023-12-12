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

package kafka.api

import java.lang.{Long => JLong}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}
import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import kafka.utils.TestUtils.{consumeRecords, waitUntilTrue}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException, TimeoutException}
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import scala.annotation.nowarn
import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.collection.mutable.{Buffer, ListBuffer}
import scala.concurrent.ExecutionException

class TransactionsTest extends IntegrationTestHarness {
  override def brokerCount = 3

  val transactionalProducerCount = 2
  val transactionalConsumerCount = 1
  val nonTransactionalConsumerCount = 1

  val topic1 = "topic1"
  val topic2 = "topic2"
  val numPartitions = 4

  val transactionalProducers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()
  val transactionalConsumers = Buffer[Consumer[Array[Byte], Array[Byte]]]()
  val nonTransactionalConsumers = Buffer[Consumer[Array[Byte], Array[Byte]]]()

  def overridingProps(): Properties = {
    val props = new Properties()
    props.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
     // Set a smaller value for the number of partitions for the __consumer_offsets topic + // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
    props.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)
    props.put(KafkaConfig.TransactionsTopicPartitionsProp, 3.toString)
    props.put(KafkaConfig.TransactionsTopicReplicationFactorProp, 2.toString)
    props.put(KafkaConfig.TransactionsTopicMinISRProp, 2.toString)
    props.put(KafkaConfig.ControlledShutdownEnableProp, true.toString)
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, false.toString)
    props.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
    props.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    props.put(KafkaConfig.TransactionsAbortTimedOutTransactionCleanupIntervalMsProp, "200")
    props

  }

  override protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach(p => p.putAll(overridingProps()))
  }

  override protected def kraftControllerConfigs(): Seq[Properties] = {
    Seq(overridingProps())

  }

  def topicConfig(): Properties = {
    val topicConfig = new Properties()
    topicConfig.put(KafkaConfig.MinInSyncReplicasProp, 2.toString)
    topicConfig
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(topic1, numPartitions, brokerCount, topicConfig())
    createTopic(topic2, numPartitions, brokerCount, topicConfig())

    for (_ <- 0 until transactionalProducerCount)
      createTransactionalProducer("transactional-producer")
    for (_ <- 0 until transactionalConsumerCount)
      createReadCommittedConsumer("transactional-group")
    for (_ <- 0 until nonTransactionalConsumerCount)
      createReadUncommittedConsumer("non-transactional-group")
  }

  @AfterEach
  override def tearDown(): Unit = {
    transactionalProducers.foreach(_.close())
    transactionalConsumers.foreach(_.close())
    nonTransactionalConsumers.foreach(_.close())
    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testBasicTransactions(quorum: String): Unit = {
    val producer = transactionalProducers.head
    val consumer = transactionalConsumers.head
    val unCommittedConsumer = nonTransactionalConsumers.head
    val tp11 = new TopicPartition(topic1, 1)
    val tp22 = new TopicPartition(topic2, 2)

    producer.initTransactions()

    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, 2, "2", "2", willBeCommitted = false))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 1, "4", "4", willBeCommitted = false))
    producer.flush()

    // Since we haven't committed/aborted any records, the last stable offset is still 0,
    // no segments should be offloaded to remote storage
    verifyLogStartOffsets(Map((tp11, 0), (tp22, 0)))
    maybeVerifyLocalLogStartOffsets(Map((tp11, 0), (tp22, 0)))
    producer.abortTransaction()

    maybeWaitForAtLeastOneSegmentUpload(Seq(tp11, tp22))

    // We've sent 1 record + 1 abort mark = 2 (segments) to each topic partition,
    // so 1 segment should be offloaded, the local log start offset should be 1
    // And log start offset is still 0
    verifyLogStartOffsets(Map((tp11, 0), (tp22, 0)))
    maybeVerifyLocalLogStartOffsets(Map((tp11, 1L), (tp22, 1L)))

    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 1, "1", "1", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, 2, "3", "3", willBeCommitted = true))

    // Before records are committed, these records won't be offloaded.
    verifyLogStartOffsets(Map((tp11, 0), (tp22, 0)))
    maybeVerifyLocalLogStartOffsets(Map((tp11, 1L), (tp22, 1L)))

    producer.commitTransaction()

    // We've sent 2 records + 1 abort mark + 1 commit mark = 4 (segments) to each topic partition,
    // so 3 segments should be offloaded, the local log start offset should be 3
    // And log start offset is still 0
    verifyLogStartOffsets(Map((tp11, 0), (tp22, 0)))
    maybeVerifyLocalLogStartOffsets(Map((tp11, 3L), (tp22, 3L)))

    consumer.subscribe(List(topic1, topic2).asJava)
    unCommittedConsumer.subscribe(List(topic1, topic2).asJava)

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }

    val allRecords = consumeRecords(unCommittedConsumer, 4)
    val expectedValues = List("1", "2", "3", "4").toSet
    allRecords.foreach { record =>
      assertTrue(expectedValues.contains(TestUtils.recordValueAsString(record)))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testReadCommittedConsumerShouldNotSeeUndecidedData(quorum: String): Unit = {
    val producer1 = transactionalProducers.head
    val producer2 = createTransactionalProducer("other")
    val readCommittedConsumer = transactionalConsumers.head
    val readUncommittedConsumer = nonTransactionalConsumers.head

    producer1.initTransactions()
    producer2.initTransactions()

    producer1.beginTransaction()
    producer2.beginTransaction()

    val latestVisibleTimestamp = System.currentTimeMillis()
    producer2.send(new ProducerRecord(topic1, 0, latestVisibleTimestamp, "x".getBytes, "1".getBytes))
    producer2.send(new ProducerRecord(topic2, 0, latestVisibleTimestamp, "x".getBytes, "1".getBytes))
    producer2.flush()

    val latestWrittenTimestamp = latestVisibleTimestamp + 1
    producer1.send(new ProducerRecord(topic1, 0, latestWrittenTimestamp, "a".getBytes, "1".getBytes))
    producer1.send(new ProducerRecord(topic1, 0, latestWrittenTimestamp, "b".getBytes, "2".getBytes))
    producer1.send(new ProducerRecord(topic2, 0, latestWrittenTimestamp, "c".getBytes, "3".getBytes))
    producer1.send(new ProducerRecord(topic2, 0, latestWrittenTimestamp, "d".getBytes, "4".getBytes))
    producer1.flush()

    producer2.send(new ProducerRecord(topic1, 0, latestWrittenTimestamp, "x".getBytes, "2".getBytes))
    producer2.send(new ProducerRecord(topic2, 0, latestWrittenTimestamp, "x".getBytes, "2".getBytes))
    producer2.commitTransaction()

    // ensure the records are visible to the read uncommitted consumer
    val tp1 = new TopicPartition(topic1, 0)
    val tp2 = new TopicPartition(topic2, 0)
    readUncommittedConsumer.assign(Set(tp1, tp2).asJava)
    consumeRecords(readUncommittedConsumer, 8)
    val readUncommittedOffsetsForTimes = readUncommittedConsumer.offsetsForTimes(Map(
      tp1 -> (latestWrittenTimestamp: JLong),
      tp2 -> (latestWrittenTimestamp: JLong)
    ).asJava)
    assertEquals(2, readUncommittedOffsetsForTimes.size)
    assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp1).timestamp)
    assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp2).timestamp)
    readUncommittedConsumer.unsubscribe()

    // we should only see the first two records which come before the undecided second transaction
    readCommittedConsumer.assign(Set(tp1, tp2).asJava)
    val records = consumeRecords(readCommittedConsumer, 2)
    records.foreach { record =>
      assertEquals("x", new String(record.key))
      assertEquals("1", new String(record.value))
    }

    // even if we seek to the end, we should not be able to see the undecided data
    assertEquals(2, readCommittedConsumer.assignment.size)
    readCommittedConsumer.seekToEnd(readCommittedConsumer.assignment)
    readCommittedConsumer.assignment.forEach { tp =>
      assertEquals(1L, readCommittedConsumer.position(tp))
    }

    // undecided timestamps should not be searchable either
    val readCommittedOffsetsForTimes = readCommittedConsumer.offsetsForTimes(Map(
      tp1 -> (latestWrittenTimestamp: JLong),
      tp2 -> (latestWrittenTimestamp: JLong)
    ).asJava)
    assertNull(readCommittedOffsetsForTimes.get(tp1))
    assertNull(readCommittedOffsetsForTimes.get(tp2))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDelayedFetchIncludesAbortedTransaction(quorum: String): Unit = {
    val producer1 = transactionalProducers.head
    val producer2 = createTransactionalProducer("other")
    val tp10 = new TopicPartition(topic1, 0)

    producer1.initTransactions()
    producer2.initTransactions()

    producer1.beginTransaction()
    producer2.beginTransaction()
    producer2.send(new ProducerRecord(topic1, 0, "x".getBytes, "1".getBytes))
    producer2.flush()

    producer1.send(new ProducerRecord(topic1, 0, "y".getBytes, "1".getBytes))
    producer1.send(new ProducerRecord(topic1, 0, "y".getBytes, "2".getBytes))
    producer1.flush()

    producer2.send(new ProducerRecord(topic1, 0, "x".getBytes, "2".getBytes))
    producer2.flush()

    // Since we haven't committed/aborted any records, the last stable offset is still 0,
    // no segments should be offloaded to remote storage
    verifyLogStartOffsets(Map((tp10, 0)))
    maybeVerifyLocalLogStartOffsets(Map((tp10, 0)))

    producer1.abortTransaction()
    producer2.commitTransaction()

    maybeWaitForAtLeastOneSegmentUpload(Seq(tp10))
    // We've sent 4 records + 1 abort mark + 1 commit mark = 6 (segments),
    // so 5 segments should be offloaded, the local log start offset should be 5
    // And log start offset is still 0
    verifyLogStartOffsets(Map((tp10, 0)))
    maybeVerifyLocalLogStartOffsets(Map((tp10, 5)))

    // ensure that the consumer's fetch will sit in purgatory
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100000")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100")
    val readCommittedConsumer = createReadCommittedConsumer(props = consumerProps)

    readCommittedConsumer.assign(Set(tp10).asJava)
    val records = consumeRecords(readCommittedConsumer, numRecords = 2)
    assertEquals(2, records.size)

    val first = records.head
    assertEquals("x", new String(first.key))
    assertEquals("1", new String(first.value))
    assertEquals(0L, first.offset)

    val second = records.last
    assertEquals("x", new String(second.key))
    assertEquals("2", new String(second.value))
    assertEquals(3L, second.offset)
  }

  @nowarn("cat=deprecation")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendOffsetsWithGroupId(quorum: String): Unit = {
    sendOffset((producer, groupId, consumer) =>
      producer.sendOffsetsToTransaction(TestUtils.consumerPositions(consumer).asJava, groupId))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendOffsetsWithGroupMetadata(quorum: String): Unit = {
    sendOffset((producer, _, consumer) =>
      producer.sendOffsetsToTransaction(TestUtils.consumerPositions(consumer).asJava, consumer.groupMetadata()))
  }

  private def sendOffset(commit: (KafkaProducer[Array[Byte], Array[Byte]],
    String, Consumer[Array[Byte], Array[Byte]]) => Unit) = {

    // The basic plan for the test is as follows:
    //  1. Seed topic1 with 500 unique, numbered, messages.
    //  2. Run a consume/process/produce loop to transactionally copy messages from topic1 to topic2 and commit
    //     offsets as part of the transaction.
    //  3. Randomly abort transactions in step2.
    //  4. Validate that we have 500 unique committed messages in topic2. If the offsets were committed properly with the
    //     transactions, we should not have any duplicates or missing messages since we should process in the input
    //     messages exactly once.

    val consumerGroupId = "foobar-consumer-group"
    val numSeedMessages = 500

    TestUtils.seedTopicWithNumberedRecords(topic1, numSeedMessages, brokers)

    val producer = transactionalProducers.head

    val consumer = createReadCommittedConsumer(consumerGroupId, maxPollRecords = numSeedMessages / 4)
    consumer.subscribe(List(topic1).asJava)
    producer.initTransactions()

    var shouldCommit = false
    var recordsProcessed = 0
    try {
      while (recordsProcessed < numSeedMessages) {
        val records = TestUtils.pollUntilAtLeastNumRecords(consumer, Math.min(10, numSeedMessages - recordsProcessed))

        producer.beginTransaction()
        shouldCommit = !shouldCommit

        records.foreach { record =>
          val key = new String(record.key(), StandardCharsets.UTF_8)
          val value = new String(record.value(), StandardCharsets.UTF_8)
          producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, key, value, willBeCommitted = shouldCommit))
        }

        commit(producer, consumerGroupId, consumer)
        if (shouldCommit) {
          producer.commitTransaction()
          recordsProcessed += records.size
          debug(s"committed transaction.. Last committed record: ${new String(records.last.value(), StandardCharsets.UTF_8)}. Num " +
            s"records written to $topic2: $recordsProcessed")
        } else {
          producer.abortTransaction()
          debug(s"aborted transaction Last committed record: ${new String(records.last.value(), StandardCharsets.UTF_8)}. Num " +
            s"records written to $topic2: $recordsProcessed")
          TestUtils.resetToCommittedPositions(consumer)
        }
      }
    } finally {
      consumer.close()
    }

    val partitions = ListBuffer.empty[TopicPartition]
    for (partition <- 0 until numPartitions) {
      partitions += new TopicPartition(topic2, partition)
    }
    maybeWaitForAtLeastOneSegmentUpload(partitions.toSeq)

    // In spite of random aborts, we should still have exactly 500 messages in topic2. I.e. we should not
    // re-copy or miss any messages from topic1, since the consumed offsets were committed transactionally.
    val verifyingConsumer = transactionalConsumers(0)
    verifyingConsumer.subscribe(List(topic2).asJava)
    val valueSeq = TestUtils.pollUntilAtLeastNumRecords(verifyingConsumer, numSeedMessages).map { record =>
      TestUtils.assertCommittedAndGetValue(record).toInt
    }
    val valueSet = valueSeq.toSet
    assertEquals(numSeedMessages, valueSeq.size, s"Expected $numSeedMessages values in $topic2.")
    assertEquals(valueSeq.size, valueSet.size, s"Expected ${valueSeq.size} unique messages in $topic2.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testFencingOnCommit(quorum: String): Unit = {
    val producer1 = transactionalProducers(0)
    val producer2 = transactionalProducers(1)
    val consumer = transactionalConsumers(0)

    consumer.subscribe(List(topic1, topic2).asJava)

    producer1.initTransactions()

    producer1.beginTransaction()
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = false))
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "3", "3", willBeCommitted = false))

    producer2.initTransactions()  // ok, will abort the open transaction.
    producer2.beginTransaction()
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "4", willBeCommitted = true))
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "4", willBeCommitted = true))

    assertThrows(classOf[ProducerFencedException], () => producer1.commitTransaction())

    producer2.commitTransaction()  // ok

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testFencingOnSendOffsets(quorum: String): Unit = {
    val producer1 = transactionalProducers(0)
    val producer2 = transactionalProducers(1)
    val consumer = transactionalConsumers(0)

    consumer.subscribe(List(topic1, topic2).asJava)

    producer1.initTransactions()

    producer1.beginTransaction()
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = false))
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "3", "3", willBeCommitted = false))
    producer1.flush()

    producer2.initTransactions()  // ok, will abort the open transaction.
    producer2.beginTransaction()
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "4", willBeCommitted = true))
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "4", willBeCommitted = true))

    assertThrows(classOf[ProducerFencedException], () => producer1.sendOffsetsToTransaction(Map(new TopicPartition("foobartopic", 0)
      -> new OffsetAndMetadata(110L)).asJava, new ConsumerGroupMetadata("foobarGroup")))

    producer2.commitTransaction()  // ok

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testOffsetMetadataInSendOffsetsToTransaction(quorum: String): Unit = {
    val tp = new TopicPartition(topic1, 0)
    val groupId = "group"

    val producer = transactionalProducers.head
    val consumer = createReadCommittedConsumer(groupId)

    consumer.subscribe(List(topic1).asJava)

    producer.initTransactions()

    producer.beginTransaction()
    val offsetAndMetadata = new OffsetAndMetadata(110L, Optional.of(15), "some metadata")
    producer.sendOffsetsToTransaction(Map(tp -> offsetAndMetadata).asJava, new ConsumerGroupMetadata(groupId))
    producer.commitTransaction()  // ok

    // The call to commit the transaction may return before all markers are visible, so we initialize a second
    // producer to ensure the transaction completes and the committed offsets are visible.
    val producer2 = transactionalProducers(1)
    producer2.initTransactions()

    TestUtils.waitUntilTrue(() => offsetAndMetadata.equals(consumer.committed(Set(tp).asJava).get(tp)), "cannot read committed offset")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInitTransactionsTimeout(quorum: String): Unit = {
    testTimeout(false, producer => producer.initTransactions())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSendOffsetsToTransactionTimeout(quorum: String): Unit = {
    testTimeout(true, producer => producer.sendOffsetsToTransaction(
      Map(new TopicPartition(topic1, 0) -> new OffsetAndMetadata(0)).asJava, new ConsumerGroupMetadata("test-group")))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCommitTransactionTimeout(quorum: String): Unit = {
    testTimeout(true, producer => producer.commitTransaction())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAbortTransactionTimeout(quorum: String): Unit = {
    testTimeout(true, producer => producer.abortTransaction())
  }

  private def testTimeout(needInitAndSendMsg: Boolean,
                  timeoutProcess: KafkaProducer[Array[Byte], Array[Byte]] => Unit): Unit = {
    val producer = createTransactionalProducer("transactionProducer", maxBlockMs = 3000)
    if (needInitAndSendMsg) {
      producer.initTransactions()
      producer.beginTransaction()
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic1, "foo".getBytes, "bar".getBytes))
    }

    for  (i <- brokers.indices) killBroker(i)

    assertThrows(classOf[TimeoutException], () => timeoutProcess(producer))
    producer.close(Duration.ZERO)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testFencingOnSend(quorum: String): Unit = {
    val producer1 = transactionalProducers(0)
    val producer2 = transactionalProducers(1)
    val consumer = transactionalConsumers(0)

    consumer.subscribe(List(topic1, topic2).asJava)

    producer1.initTransactions()

    producer1.beginTransaction()
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = false))
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "3", "3", willBeCommitted = false))

    producer2.initTransactions()  // ok, will abort the open transaction.
    producer2.beginTransaction()
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "4", willBeCommitted = true)).get()
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "4", willBeCommitted = true)).get()

    try {
      val result = producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "5", willBeCommitted = false))
      val recordMetadata = result.get()
      error(s"Missed a producer fenced exception when writing to ${recordMetadata.topic}-${recordMetadata.partition}. Grab the logs!!")
      brokers.foreach { broker =>
        error(s"log dirs: ${broker.logManager.liveLogDirs.map(_.getAbsolutePath).head}")
      }
      fail("Should not be able to send messages from a fenced producer.")
    } catch {
      case _: ProducerFencedException =>
        producer1.close()
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[InvalidProducerEpochException])
      case e: Exception =>
        throw new AssertionError("Got an unexpected exception from a fenced producer.", e)
    }

    producer2.commitTransaction() // ok

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testFencingOnAddPartitions(quorum: String): Unit = {
    val producer1 = transactionalProducers(0)
    val producer2 = transactionalProducers(1)
    val consumer = transactionalConsumers(0)

    consumer.subscribe(List(topic1, topic2).asJava)

    producer1.initTransactions()
    producer1.beginTransaction()
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = false))
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "3", "3", willBeCommitted = false))
    producer1.abortTransaction()

    producer2.initTransactions()  // ok, will abort the open transaction.
    producer2.beginTransaction()
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "4", willBeCommitted = true))
      .get(20, TimeUnit.SECONDS)
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "4", willBeCommitted = true))
      .get(20, TimeUnit.SECONDS)

    try {
      producer1.beginTransaction()
      val result =  producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "5", willBeCommitted = false))
      val recordMetadata = result.get()
      error(s"Missed a producer fenced exception when writing to ${recordMetadata.topic}-${recordMetadata.partition}. Grab the logs!!")
      brokers.foreach { broker =>
        error(s"log dirs: ${broker.logManager.liveLogDirs.map(_.getAbsolutePath).head}")
      }
      fail("Should not be able to send messages from a fenced producer.")
    } catch {
      case _: ProducerFencedException =>
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[ProducerFencedException])
      case e: Exception =>
        throw new AssertionError("Got an unexpected exception from a fenced producer.", e)
    }

    producer2.commitTransaction()  // ok

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testFencingOnTransactionExpiration(quorum: String): Unit = {
    val producer = createTransactionalProducer("expiringProducer", transactionTimeoutMs = 100)

    producer.initTransactions()
    producer.beginTransaction()

    // The first message and hence the first AddPartitions request should be successfully sent.
    val firstMessageResult = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = false)).get()
    assertTrue(firstMessageResult.hasOffset)

    // Wait for the expiration cycle to kick in.
    Thread.sleep(600)

    try {
      // Now that the transaction has expired, the second send should fail with a ProducerFencedException.
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "2", willBeCommitted = false)).get()
      fail("should have raised a ProducerFencedException since the transaction has expired")
    } catch {
      case _: ProducerFencedException =>
      case e: ExecutionException =>
      assertTrue(e.getCause.isInstanceOf[ProducerFencedException])
    }

    // Verify that the first message was aborted and the second one was never written at all.
    val nonTransactionalConsumer = nonTransactionalConsumers.head
    nonTransactionalConsumer.subscribe(List(topic1).asJava)

    // Attempt to consume the one written record. We should not see the second. The
    // assertion does not strictly guarantee that the record wasn't written, but the
    // data is small enough that had it been written, it would have been in the first fetch.
    val records = TestUtils.consumeRecords(nonTransactionalConsumer, numRecords = 1)
    assertEquals(1, records.size)
    assertEquals("1", TestUtils.recordValueAsString(records.head))

    val transactionalConsumer = transactionalConsumers.head
    transactionalConsumer.subscribe(List(topic1).asJava)

    val transactionalRecords = TestUtils.consumeRecordsFor(transactionalConsumer, 1000)
    assertTrue(transactionalRecords.isEmpty)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testMultipleMarkersOneLeader(quorum: String): Unit = {
    val firstProducer = transactionalProducers.head
    val consumer = transactionalConsumers.head
    val unCommittedConsumer = nonTransactionalConsumers.head
    val topicWith10Partitions = "largeTopic"
    val topicWith10PartitionsAndOneReplica = "largeTopicOneReplica"

    createTopic(topicWith10Partitions, 10, brokerCount, topicConfig())
    createTopic(topicWith10PartitionsAndOneReplica, 10, 1, new Properties())

    firstProducer.initTransactions()

    firstProducer.beginTransaction()
    sendTransactionalMessagesWithValueRange(firstProducer, topicWith10Partitions, 0, 5000, willBeCommitted = false)
    sendTransactionalMessagesWithValueRange(firstProducer, topicWith10PartitionsAndOneReplica, 5000, 10000, willBeCommitted = false)
    firstProducer.abortTransaction()

    firstProducer.beginTransaction()
    sendTransactionalMessagesWithValueRange(firstProducer, topicWith10Partitions, 10000, 11000, willBeCommitted = true)
    firstProducer.commitTransaction()

    consumer.subscribe(List(topicWith10PartitionsAndOneReplica, topicWith10Partitions).asJava)
    unCommittedConsumer.subscribe(List(topicWith10PartitionsAndOneReplica, topicWith10Partitions).asJava)

    val records = consumeRecords(consumer, 1000)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }

    val allRecords = consumeRecords(unCommittedConsumer, 11000)
    val expectedValues = Range(0, 11000).map(_.toString).toSet
    allRecords.foreach { record =>
      assertTrue(expectedValues.contains(TestUtils.recordValueAsString(record)))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testConsecutivelyRunInitTransactions(quorum: String): Unit = {
    val producer = createTransactionalProducer(transactionalId = "normalProducer")

    producer.initTransactions()
    assertThrows(classOf[IllegalStateException], () => producer.initTransactions())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testBumpTransactionalEpoch(quorum: String): Unit = {
    val producer = createTransactionalProducer("transactionalProducer",
      deliveryTimeoutMs = 5000, requestTimeoutMs = 5000)
    val consumer = transactionalConsumers.head
    try {
      // Create a topic with RF=1 so that a single broker failure will render it unavailable
      val testTopic = "test-topic"
      createTopic(testTopic, numPartitions, 1, new Properties)
      val partitionLeader = TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(testTopic, 0))

      producer.initTransactions()

      producer.beginTransaction()
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "4", "4", willBeCommitted = true))
      producer.commitTransaction()

      val activeProducersIter = brokers(partitionLeader).logManager.getLog(new TopicPartition(testTopic, 0)).get
        .producerStateManager.activeProducers.entrySet().iterator()
      assertTrue(activeProducersIter.hasNext)
      var producerStateEntry = activeProducersIter.next().getValue
      val producerId = producerStateEntry.producerId
      val initialProducerEpoch = producerStateEntry.producerEpoch

      producer.beginTransaction()
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "2", willBeCommitted = false))

      killBroker(partitionLeader) // kill the partition leader to prevent the batch from being submitted
      val failedFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", willBeCommitted = false))
      Thread.sleep(6000) // Wait for the record to time out
      restartDeadBrokers()

      org.apache.kafka.test.TestUtils.assertFutureThrows(failedFuture, classOf[TimeoutException])
      producer.abortTransaction()

      producer.beginTransaction()
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "2", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "4", "4", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", willBeCommitted = true))
      producer.commitTransaction()

      consumer.subscribe(List(topic1, topic2, testTopic).asJava)

      val records = consumeRecords(consumer, 5)
      records.foreach { record =>
        TestUtils.assertCommittedAndGetValue(record)
      }

      // Producers can safely abort and continue after the last record of a transaction timing out, so it's possible to
      // get here without having bumped the epoch. If bumping the epoch is possible, the producer will attempt to, so
      // check there that the epoch has actually increased
      producerStateEntry =
        brokers(partitionLeader).logManager.getLog(new TopicPartition(testTopic, 0)).get.producerStateManager.activeProducers.get(producerId)
      assertNotNull(producerStateEntry)
      assertTrue(producerStateEntry.producerEpoch > initialProducerEpoch)
    } finally {
      producer.close(Duration.ZERO)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testFailureToFenceEpoch(quorum: String): Unit = {
    val producer1 = transactionalProducers.head
    val producer2 = createTransactionalProducer("transactional-producer", maxBlockMs = 1000)

    producer1.initTransactions()

    producer1.beginTransaction()
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "4", "4", willBeCommitted = true))
    producer1.commitTransaction()

    val partitionLeader = TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(topic1, 0))
    val activeProducersIter = brokers(partitionLeader).logManager.getLog(new TopicPartition(topic1, 0)).get.producerStateManager
      .activeProducers.entrySet().iterator()
    assertTrue(activeProducersIter.hasNext)
    var producerStateEntry = activeProducersIter.next().getValue
    val producerId = producerStateEntry.producerId
    val initialProducerEpoch = producerStateEntry.producerEpoch

    // Kill two brokers to bring the transaction log under min-ISR
    killBroker(0)
    killBroker(1)

    try {
      producer2.initTransactions()
    } catch {
      case _: TimeoutException =>
        // good!
      case e: Exception =>
        throw new AssertionError("Got an unexpected exception from initTransactions", e)
    } finally {
      producer2.close()
    }

    restartDeadBrokers()

    // Because the epoch was bumped in memory, attempting to begin a transaction with producer 1 should fail
    try {
      producer1.beginTransaction()
    } catch {
      case _: ProducerFencedException =>
        // good!
      case e: Exception =>
        throw new AssertionError("Got an unexpected exception from commitTransaction", e)
    } finally {
      producer1.close()
    }

    val producer3 = createTransactionalProducer("transactional-producer", maxBlockMs = 5000)
    producer3.initTransactions()

    producer3.beginTransaction()
    producer3.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "4", "4", willBeCommitted = true))
    producer3.commitTransaction()

    // Check that the epoch only increased by 1
    producerStateEntry =
      brokers(partitionLeader).logManager.getLog(new TopicPartition(topic1, 0)).get.producerStateManager.activeProducers.get(producerId)
    assertNotNull(producerStateEntry)
    assertEquals((initialProducerEpoch + 1).toShort, producerStateEntry.producerEpoch)
  }

  private def sendTransactionalMessagesWithValueRange(producer: KafkaProducer[Array[Byte], Array[Byte]], topic: String,
                                                      start: Int, end: Int, willBeCommitted: Boolean): Unit = {
    for (i <- start until end) {
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic, null, value = i.toString, willBeCommitted = willBeCommitted, key = i.toString))
    }
    producer.flush()
  }

  private def createReadCommittedConsumer(group: String = "group",
                                          maxPollRecords: Int = 500,
                                          props: Properties = new Properties) = {
    val consumer = TestUtils.createConsumer(bootstrapServers(),
      groupId = group,
      enableAutoCommit = false,
      readCommitted = true,
      maxPollRecords = maxPollRecords)
    transactionalConsumers += consumer
    consumer
  }

  private def createReadUncommittedConsumer(group: String) = {
    val consumer = TestUtils.createConsumer(bootstrapServers(),
      groupId = group,
      enableAutoCommit = false)
    nonTransactionalConsumers += consumer
    consumer
  }

  private def createTransactionalProducer(transactionalId: String,
                                          transactionTimeoutMs: Long = 60000,
                                          maxBlockMs: Long = 60000,
                                          deliveryTimeoutMs: Int = 120000,
                                          requestTimeoutMs: Int = 30000): KafkaProducer[Array[Byte], Array[Byte]] = {
    val producer = TestUtils.createTransactionalProducer(
      transactionalId,
      brokers,
      transactionTimeoutMs = transactionTimeoutMs,
      maxBlockMs = maxBlockMs,
      deliveryTimeoutMs = deliveryTimeoutMs,
      requestTimeoutMs = requestTimeoutMs
    )
    transactionalProducers += producer
    producer
  }

  def maybeWaitForAtLeastOneSegmentUpload(topicPartitions: Seq[TopicPartition]): Unit = {
  }

  def verifyLogStartOffsets(partitionStartOffsets: Map[TopicPartition, Int]): Unit = {
    val offsets = new util.HashMap[Integer, JLong]()
    waitUntilTrue(() => {
      brokers.forall(broker => {
        partitionStartOffsets.forall {
          case (partition, offset) => {
            val lso = broker.replicaManager.localLog(partition).get.logStartOffset
            offsets.put(broker.config.brokerId, lso)
            offset == lso
          }
        }
      })
    }, s"log start offset doesn't change to the expected position: $partitionStartOffsets, current position: $offsets")
  }

  @throws(classOf[InterruptedException])
  def maybeVerifyLocalLogStartOffsets(partitionStartOffsets: Map[TopicPartition, JLong]): Unit = {
    // Non-tiered storage topic partition doesn't have local log start offset
  }
}
