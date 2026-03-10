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

import kafka.utils.TestUtils.{consumeRecords, waitUntilTrue}
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.TransactionState
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{ConcurrentTransactionsException, InvalidProducerEpochException, ProducerFencedException, TimeoutException}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, MethodSource}

import java.lang.{Long => JLong}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, mutable}
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

class TransactionsTest extends IntegrationTestHarness {
  override def brokerCount = 3

  val transactionalProducerCount = 2
  val transactionalConsumerCount = 1
  val nonTransactionalConsumerCount = 1

  val topic1 = "topic1"
  val topic2 = "topic2"
  val numPartitions = 4

  val transactionalProducers = mutable.Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()
  val transactionalConsumers = mutable.Buffer[Consumer[Array[Byte], Array[Byte]]]()
  val nonTransactionalConsumers = mutable.Buffer[Consumer[Array[Byte], Array[Byte]]]()

  def overridingProps(): Properties = {
    val props = new Properties()
    props.put(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, false.toString)
     // Set a smaller value for the number of partitions for the __consumer_offsets topic + // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
    props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, 1.toString)
    props.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, 3.toString)
    props.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, 2.toString)
    props.put(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, 2.toString)
    props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, true.toString)
    props.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, false.toString)
    props.put(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, false.toString)
    props.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0")
    props.put(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, "200")
    // Enable unstable API versions to support KIP-939 2PC (InitProducerId v6 with keepPreparedTxn)
    props.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
    props.put(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
    props
  }

  override protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach(p => p.putAll(overridingProps()))
  }

  override protected def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    Seq(overridingProps())

  }

  def topicConfig(): Properties = {
    val topicConfig = new Properties()
    topicConfig.put(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, 2.toString)
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testBasicTransactions(groupProtocol: String): Unit = {
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

    consumer.subscribe(java.util.List.of(topic1, topic2))
    unCommittedConsumer.subscribe(java.util.List.of(topic1, topic2))

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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testReadCommittedConsumerShouldNotSeeUndecidedData(groupProtocol: String): Unit = {
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
    readUncommittedConsumer.assign(java.util.Set.of(tp1, tp2))
    consumeRecords(readUncommittedConsumer, 8)
    val readUncommittedOffsetsForTimes = readUncommittedConsumer.offsetsForTimes(java.util.Map.of(
      tp1, latestWrittenTimestamp: JLong,
      tp2, latestWrittenTimestamp: JLong
    ))
    assertEquals(2, readUncommittedOffsetsForTimes.size)
    assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp1).timestamp)
    assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp2).timestamp)
    readUncommittedConsumer.unsubscribe()

    // we should only see the first two records which come before the undecided second transaction
    readCommittedConsumer.assign(java.util.Set.of(tp1, tp2))
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
    val readCommittedOffsetsForTimes = readCommittedConsumer.offsetsForTimes(java.util.Map.of(
      tp1, latestWrittenTimestamp: JLong,
      tp2, latestWrittenTimestamp: JLong
    ))
    assertNull(readCommittedOffsetsForTimes.get(tp1))
    assertNull(readCommittedOffsetsForTimes.get(tp2))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testDelayedFetchIncludesAbortedTransaction(groupProtocol: String): Unit = {
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

    readCommittedConsumer.assign(java.util.Set.of(tp10))
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSendOffsetsWithGroupMetadata(groupProtocol: String): Unit = {
    sendOffset((producer, _, consumer) =>
      producer.sendOffsetsToTransaction(TestUtils.consumerPositions(consumer).asJava, consumer.groupMetadata()))
  }

  private def sendOffset(commit: (KafkaProducer[Array[Byte], Array[Byte]],
    String, Consumer[Array[Byte], Array[Byte]]) => Unit): Unit = {

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
    consumer.subscribe(java.util.List.of(topic1))
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
    verifyingConsumer.subscribe(java.util.List.of(topic2))
    val valueSeq = TestUtils.pollUntilAtLeastNumRecords(verifyingConsumer, numSeedMessages).map { record =>
      TestUtils.assertCommittedAndGetValue(record).toInt
    }
    val valueSet = valueSeq.toSet
    assertEquals(numSeedMessages, valueSeq.size, s"Expected $numSeedMessages values in $topic2.")
    assertEquals(valueSeq.size, valueSet.size, s"Expected ${valueSeq.size} unique messages in $topic2.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testFencingOnCommit(groupProtocol: String): Unit = {
    val producer1 = transactionalProducers(0)
    val producer2 = transactionalProducers(1)
    val consumer = transactionalConsumers(0)

    consumer.subscribe(java.util.List.of(topic1, topic2))

    producer1.initTransactions()

    producer1.beginTransaction()
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = false))
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "3", "3", willBeCommitted = false))
    producer1.flush()

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

  @SuppressWarnings(Array("removal"))
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testFencingOnSendOffsets(groupProtocol: String): Unit = {
    val producer1 = transactionalProducers(0)
    val producer2 = transactionalProducers(1)
    val consumer = transactionalConsumers(0)

    consumer.subscribe(java.util.List.of(topic1, topic2))

    producer1.initTransactions()

    producer1.beginTransaction()
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = false))
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "3", "3", willBeCommitted = false))
    producer1.flush()

    producer2.initTransactions()  // ok, will abort the open transaction.
    producer2.beginTransaction()
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "4", willBeCommitted = true))
    producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "4", willBeCommitted = true))

    assertThrows(classOf[ProducerFencedException], () => producer1.sendOffsetsToTransaction(java.util.Map.of(new TopicPartition(topic1, 0),
      new OffsetAndMetadata(110L)), new ConsumerGroupMetadata("foobarGroup")))

    producer2.commitTransaction()  // ok

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  @SuppressWarnings(Array("removal"))
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testOffsetMetadataInSendOffsetsToTransaction(groupProtocol: String): Unit = {
    val tp = new TopicPartition(topic1, 0)
    val groupId = "group"

    val producer = transactionalProducers.head
    val consumer = createReadCommittedConsumer(groupId)

    consumer.subscribe(java.util.List.of(topic1))

    producer.initTransactions()

    producer.beginTransaction()
    val offsetAndMetadata = new OffsetAndMetadata(110L, Optional.of(15), "some metadata")
    producer.sendOffsetsToTransaction(java.util.Map.of(tp, offsetAndMetadata), new ConsumerGroupMetadata(groupId))
    producer.commitTransaction()  // ok

    // The call to commit the transaction may return before all markers are visible, so we initialize a second
    // producer to ensure the transaction completes and the committed offsets are visible.
    val producer2 = transactionalProducers(1)
    producer2.initTransactions()

    TestUtils.waitUntilTrue(() => offsetAndMetadata.equals(consumer.committed(java.util.Set.of(tp)).get(tp)), "cannot read committed offset")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testInitTransactionsTimeout(groupProtocol: String): Unit = {
    testTimeout(needInitAndSendMsg = false, producer => producer.initTransactions())
  }

  @SuppressWarnings(Array("removal"))
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSendOffsetsToTransactionTimeout(groupProtocol: String): Unit = {
    testTimeout(needInitAndSendMsg = true, producer => producer.sendOffsetsToTransaction(
      java.util.Map.of(new TopicPartition(topic1, 0), new OffsetAndMetadata(0)), new ConsumerGroupMetadata("test-group")))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCommitTransactionTimeout(groupProtocol: String): Unit = {
    testTimeout(needInitAndSendMsg = true, producer => producer.commitTransaction())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testAbortTransactionTimeout(groupProtocol: String): Unit = {
    testTimeout(needInitAndSendMsg = true, producer => producer.abortTransaction())
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testFencingOnSend(groupProtocol: String): Unit = {
    val producer1 = transactionalProducers(0)
    val producer2 = transactionalProducers(1)
    val consumer = transactionalConsumers(0)

    consumer.subscribe(java.util.List.of(topic1, topic2))

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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testFencingOnAddPartitions(groupProtocol: String): Unit = {
    val producer1 = transactionalProducers(0)
    val producer2 = transactionalProducers(1)
    val consumer = transactionalConsumers(0)

    consumer.subscribe(java.util.List.of(topic1, topic2))
    TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(topic1, 0))
    TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(topic2, 0))

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
      error(s"Missed an exception when writing to ${recordMetadata.topic}-${recordMetadata.partition}. Grab the logs!!")
      brokers.foreach { broker =>
        error(s"log dirs: ${broker.logManager.liveLogDirs.map(_.getAbsolutePath).head}")
      }
      fail("Should not be able to send messages from a fenced producer.")
    } catch {
      case _: InvalidProducerEpochException =>
      case e: ExecutionException =>
        // In kraft mode, transactionV2 is used.
        assertTrue(e.getCause.isInstanceOf[InvalidProducerEpochException])
      case e: Exception =>
        throw new AssertionError("Got an unexpected exception from a fenced producer.", e)
    }

    producer2.commitTransaction()  // ok

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testFencingOnTransactionExpiration(groupProtocol: String): Unit = {
    val producer = createTransactionalProducer("expiringProducer", transactionTimeoutMs = 300)

    producer.initTransactions()
    producer.beginTransaction()

    // The first message and hence the first AddPartitions request should be successfully sent.
    val firstMessageResult = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = false)).get()
    assertTrue(firstMessageResult.hasOffset)

    // Wait for the expiration cycle to kick in.
    Thread.sleep(600)

    try {
      // Now that the transaction has expired, the second send should fail with a InvalidProducerEpochException. We may see some concurrentTransactionsExceptions.
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "2", willBeCommitted = false)).get()
      fail("should have raised an error due to concurrent transactions or invalid producer epoch")
    } catch {
      case _: ConcurrentTransactionsException =>
      case _: InvalidProducerEpochException =>
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[InvalidProducerEpochException], "Error was " + e.getCause + " and not InvalidProducerEpochException")
    }

    // Verify that the first message was aborted and the second one was never written at all.
    val nonTransactionalConsumer = nonTransactionalConsumers.head
    nonTransactionalConsumer.subscribe(java.util.List.of(topic1))

    // Attempt to consume the one written record. We should not see the second. The
    // assertion does not strictly guarantee that the record wasn't written, but the
    // data is small enough that had it been written, it would have been in the first fetch.
    val records = TestUtils.consumeRecords(nonTransactionalConsumer, numRecords = 1)
    assertEquals(1, records.size)
    assertEquals("1", TestUtils.recordValueAsString(records.head))

    val transactionalConsumer = transactionalConsumers.head
    transactionalConsumer.subscribe(java.util.List.of(topic1))

    val transactionalRecords = consumeRecordsFor(transactionalConsumer)
    assertTrue(transactionalRecords.isEmpty)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testMultipleMarkersOneLeader(groupProtocol: String): Unit = {
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

    consumer.subscribe(java.util.List.of(topicWith10PartitionsAndOneReplica, topicWith10Partitions))
    unCommittedConsumer.subscribe(java.util.List.of(topicWith10PartitionsAndOneReplica, topicWith10Partitions))

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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsecutivelyRunInitTransactions(groupProtocol: String): Unit = {
    val producer = createTransactionalProducer(transactionalId = "normalProducer")

    producer.initTransactions()
    assertThrows(classOf[IllegalStateException], () => producer.initTransactions())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersConsumerGroupProtocolOnly"))
  def testRecoveryFromEpochOverflow(groupProtocol: String): Unit = {
    // We could encounter a bug (see https://issues.apache.org/jira/browse/KAFKA-20090)
    // that only reproduces when epoch gets to Short.MaxValue - 1 and transaction is
    // aborted on timeout.
    val transactionalId = "test-overflow"
    var producer = createTransactionalProducer(transactionalId, transactionTimeoutMs = 500)
    val abortedRecord = new ProducerRecord[Array[Byte], Array[Byte]](topic1, 0, "key".getBytes, "aborted".getBytes)

    // Create a transaction, produce one record, and abort
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(abortedRecord)
    producer.abortTransaction()
    producer.close()

    // Update the epoch close to Short.MaxValue to trigger the overflow scenario.
    // Set it high enough that subsequent operations will cause it to reach
    // Short.MaxValue - 1 before the timeout.
    setProducerEpoch(transactionalId, (Short.MaxValue - 2).toShort)

    // Re-initialize the producer which will bump epoch
    producer = createTransactionalProducer(transactionalId, transactionTimeoutMs = 500)
    producer.initTransactions()

    // Start a transaction
    producer.beginTransaction()
    // Produce one record and wait for it to complete
    producer.send(abortedRecord).get()
    producer.flush()

    // Check and assert that epoch of the transaction is Short.MaxValue - 1 (before timeout)
    val currentEpoch = getProducerEpoch(transactionalId)
    assertEquals((Short.MaxValue - 1).toShort, currentEpoch,
      s"Expected epoch to be ${Short.MaxValue - 1}, but got $currentEpoch")

    // Wait until state is complete abort
    val adminClient2 = createAdminClient()
    try {
      waitUntilTrue(() => {
        val listResult = adminClient2.listTransactions()
        val txns = listResult.all().get().asScala
        txns.exists(txn =>
          txn.transactionalId() == transactionalId &&
          txn.state() == TransactionState.COMPLETE_ABORT
        )
      }, "Transaction was not aborted on timeout")
    } finally {
      adminClient2.close()
    }

    // Abort, this should be treated as retry of the abort caused by timeout
    producer.abortTransaction()

    // Start a transaction, it would use the state from abort
    producer.beginTransaction()
    // Produce one record and wait for it to complete
    producer.send(abortedRecord).get()
    producer.flush()

    // Now init new producer and commit a transaction with a distinct value
    val producer2 = createTransactionalProducer(transactionalId, transactionTimeoutMs = 500)
    producer2.initTransactions()
    producer2.beginTransaction()
    val committedRecord = new ProducerRecord[Array[Byte], Array[Byte]](topic1, 0, "key".getBytes, "committed".getBytes)
    producer2.send(committedRecord).get()
    producer2.commitTransaction()

    // Verify that exactly one record is visible in read-committed mode
    val consumer = createReadCommittedConsumer("test-consumer-group")
    try {
      val tp = new TopicPartition(topic1, 0)
      consumer.assign(java.util.Set.of(tp))
      val records = consumeRecords(consumer, 1)

      val record = records.head
      assertArrayEquals("key".getBytes, record.key, "Record key should match")
      assertArrayEquals("committed".getBytes, record.value, "Record value should be 'committed'")
      assertEquals(0, record.partition, "Record should be in partition 0")
      assertEquals(topic1, record.topic, "Record should be in topic1")
    } finally {
      consumer.close()
    }
  }

  @ParameterizedTest
  @CsvSource(Array(
    "classic,false",
    "consumer,false",
  ))
  def testBumpTransactionalEpochWithTV2Disabled(groupProtocol: String, isTV2Enabled: Boolean): Unit = {
    val defaultLinger = 5
    val producer = createTransactionalProducer("transactionalProducer",
      deliveryTimeoutMs = 5000 + defaultLinger, requestTimeoutMs = 5000)
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
      val successfulFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "2", willBeCommitted = false))
      successfulFuture.get(20, TimeUnit.SECONDS)

      killBroker(partitionLeader) // kill the partition leader to prevent the batch from being submitted
      val failedFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", willBeCommitted = false))
      Thread.sleep(6000) // Wait for the record to time out
      restartDeadBrokers()

      org.apache.kafka.test.TestUtils.assertFutureThrows(classOf[TimeoutException], failedFuture)
      // Ensure the producer transitions to abortable_error state.
      TestUtils.waitUntilTrue(() => {
        var failed = false
        try {
          producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", willBeCommitted = false))
        } catch {
          case e: Exception =>
            if (e.isInstanceOf[KafkaException])
              failed = true
        }
        failed
      }, "The send request never failed as expected.")
      assertThrows(classOf[KafkaException], () => producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", willBeCommitted = false)))
      producer.abortTransaction()

      producer.beginTransaction()
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "2", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "4", "4", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", willBeCommitted = true))
      producer.commitTransaction()

      consumer.subscribe(java.util.List.of(topic1, topic2, testTopic))

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
      assertTrue(producerStateEntry.producerEpoch > initialProducerEpoch, "InitialProduceEpoch: " + initialProducerEpoch + " ProducerStateEntry: " + producerStateEntry)
    } finally {
      producer.close(Duration.ZERO)
    }
  }

  @ParameterizedTest
  @CsvSource(Array(
    "classic, true",
    "consumer, true"
  ))
  def testBumpTransactionalEpochWithTV2Enabled(groupProtocol: String, isTV2Enabled: Boolean): Unit = {
    val defaultLinger = 5
    val producer = createTransactionalProducer("transactionalProducer",
      deliveryTimeoutMs = 5000 + defaultLinger, requestTimeoutMs = 5000)
    val consumer = transactionalConsumers.head

    try {
      // Create a topic with RF=1 so that a single broker failure will render it unavailable
      val testTopic = "test-topic"
      createTopic(testTopic, numPartitions, 1, new Properties)
      val partitionLeader = TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(testTopic, 0))

      producer.initTransactions()

      // First transaction: commit
      producer.beginTransaction()
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "4", "4", willBeCommitted = true))
      producer.commitTransaction()

      // Second transaction: abort
      producer.beginTransaction()
      val successfulFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "2", "2", willBeCommitted = false))
      successfulFuture.get(20, TimeUnit.SECONDS)

      // Get producerId and epoch after first commit. Check after the first successful send of the next transaction to confirm the commit is complete.
      val log = brokers(partitionLeader).logManager.getLog(new TopicPartition(testTopic, 0)).get
      val producerStateManager = log.producerStateManager
      val activeProducersIter = producerStateManager.activeProducers.entrySet().iterator()
      assertTrue(activeProducersIter.hasNext)
      var producerStateEntry = activeProducersIter.next().getValue
      val producerId = producerStateEntry.producerId
      val previousProducerEpoch = producerStateEntry.producerEpoch

      killBroker(partitionLeader) // kill the partition leader to prevent the batch from being submitted
      val failedFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", willBeCommitted = false))
      Thread.sleep(6000) // Wait for the record to time out
      restartDeadBrokers()

      org.apache.kafka.test.TestUtils.assertFutureThrows(classOf[TimeoutException], failedFuture)
      producer.abortTransaction()

      // Third transaction: commit
      producer.beginTransaction()
      val nextSuccessfulFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "2", willBeCommitted = true))
      nextSuccessfulFuture.get(20, TimeUnit.SECONDS)

      // Confirm the epoch bumped after the previous abort.
      producerStateEntry =
        brokers(partitionLeader).logManager.getLog(new TopicPartition(topic2, 0)).get.producerStateManager.activeProducers.get(producerId)
      assertNotNull(producerStateEntry)
      assertTrue(producerStateEntry.producerEpoch > previousProducerEpoch)

      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "4", "4", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", willBeCommitted = true))
      producer.commitTransaction()

      consumer.subscribe(java.util.List.of(topic1, topic2, testTopic))

      val records = consumeRecords(consumer, 5)
      records.foreach { record =>
        TestUtils.assertCommittedAndGetValue(record)
      }

    } finally {
      producer.close(Duration.ZERO)
    }
  }

  @ParameterizedTest(name = "{displayName}.groupProtocol={0}.isTV2Enabled={1}")
  @CsvSource(Array(
    "classic, false",
    "consumer, false",
    "classic, true",
    "consumer, true",
  ))
  def testFailureToFenceEpoch(groupProtocol: String, isTV2Enabled: Boolean): Unit = {
    val producer1 = transactionalProducers.head
    val producer2 = createTransactionalProducer("transactional-producer", maxBlockMs = 1000)
    val initialProducerEpoch = 0

    producer1.initTransactions()

    producer1.beginTransaction()
    producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "4", "4", willBeCommitted = true))
    producer1.commitTransaction()

    val partitionLeader = TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(topic1, 0))
    var producerStateEntry = brokers(partitionLeader).logManager.getLog(new TopicPartition(topic1, 0)).get.producerStateManager
      .activeProducers.entrySet().iterator().next().getValue
    val producerId = producerStateEntry.producerId

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

    // Make sure to leave this producer enough time before request timeout. The broker restart can take some time.
    val producer3 = createTransactionalProducer("transactional-producer")
    producer3.initTransactions()

    producer3.beginTransaction()
    producer3.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "4", "4", willBeCommitted = true))
    producer3.commitTransaction()

    // Check that the epoch only increased by 1 when TV2 is disabled.
    // With TV2 and the latest EndTxnRequest version, the epoch will be bumped at the end of every transaction aka
    // three times (once after each commit and once after the timeout exception). The last bump is less consistent, so ensure the first two happen.
    producerStateEntry =
      brokers(partitionLeader).logManager.getLog(new TopicPartition(topic1, 0)).get.producerStateManager.activeProducers.get(producerId)
    assertNotNull(producerStateEntry)

    if (!isTV2Enabled) {
      assertEquals((initialProducerEpoch + 1).toShort, producerStateEntry.producerEpoch)
    } else {
      assertTrue((initialProducerEpoch + 1).toShort <= producerStateEntry.producerEpoch)
    }
  }

  @ParameterizedTest(name = "{displayName}.groupProtocol={0}.isTV2Enabled={1}")
  @CsvSource(Array(
    "consumer, true",
  ))
  def testEmptyAbortAfterCommit(groupProtocol: String, isTV2Enabled: Boolean): Unit = {
    val producer = transactionalProducers.head

    producer.initTransactions()
    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 1, "4", "4", willBeCommitted = false))
    producer.commitTransaction()

    producer.beginTransaction()
    producer.abortTransaction()
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
      groupProtocolFromTestParameters(),
      groupId = group,
      enableAutoCommit = false,
      readCommitted = true,
      maxPollRecords = maxPollRecords)
    transactionalConsumers += consumer
    consumer
  }

  private def createReadUncommittedConsumer(group: String) = {
    val consumer = TestUtils.createConsumer(bootstrapServers(),
      groupProtocolFromTestParameters(),
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
          case (partition, offset) =>
            val lso = broker.replicaManager.localLog(partition).get.logStartOffset
            offsets.put(broker.config.brokerId, lso)
            offset == lso
        }
      })
    }, s"log start offset doesn't change to the expected position: $partitionStartOffsets, current position: $offsets")
  }

  /**
   * Will consume all the records for the given consumer for the specified duration. If you want to drain all the
   * remaining messages in the partitions the consumer is subscribed to, the duration should be set high enough so
   * that the consumer has enough time to poll everything. This would be based on the number of expected messages left
   * in the topic, and should not be too large (ie. more than a second) in our tests.
   *
   * @return All the records consumed by the consumer within the specified duration.
   */
  private def consumeRecordsFor[K, V](consumer: Consumer[K, V]): Seq[ConsumerRecord[K, V]] = {
    val duration = 1000
    val startTime = System.currentTimeMillis()
    val records = new ArrayBuffer[ConsumerRecord[K, V]]()
    waitUntilTrue(() => {
      records ++= consumer.poll(Duration.ofMillis(50)).asScala
      System.currentTimeMillis() - startTime > duration
    }, s"The timeout $duration was greater than the maximum wait time.")
    records
  }

  @throws(classOf[InterruptedException])
  def maybeVerifyLocalLogStartOffsets(partitionStartOffsets: Map[TopicPartition, JLong]): Unit = {
    // Non-tiered storage topic partition doesn't have local log start offset
  }

  // KIP-939 2PC integration tests

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testProducerCrashAndRecoverWith2PC(groupProtocol: String): Unit = {
    // Test producer crash and recovery with 2PC keepPrepared transaction flow.
    // Note: This uses a standard transactional producer.  2PC is enabled server-side
    // and triggered by calling initTransactions(keepPreparedTxn=true) after crash.

    def test2PCRecovery(numCrashes: Int, shouldCommit: Boolean): Unit = {
      val transactionalId = s"test-2pc-recovery-${System.nanoTime()}"
      val testTopic = s"test-2pc-topic-${System.nanoTime()}"
      createTopic(testTopic, 1, brokerCount, topicConfig())

      val consumer = transactionalConsumers.head
      consumer.subscribe(Seq(testTopic).asJava)
      consumer.poll(Duration.ofMillis(100))  // Trigger assignment

      // Create producer and send records in a transaction
      var producer = createTransactionalProducer(transactionalId)
      producer.initTransactions()
      producer.beginTransaction()

      val numRecords = 5
      for (i <- 0 until numRecords) {
        producer.send(new ProducerRecord(testTopic, 0, s"key-$i".getBytes,
          s"value-$i".getBytes))
      }
      producer.flush()

      // Verify records not visible to read_committed consumer
      consumer.poll(Duration.ofMillis(100))
      assertEquals(0, consumer.assignment().asScala.map(tp =>
        consumer.position(tp)).sum, "Records should not be visible before commit")

      // Simulate crash by closing without committing
      producer.close(Duration.ZERO)

      val adminClient = createAdminClient()
      try {
        val baseEpoch: Short = 0

        // Crash and recover numCrashes times
        for (crashNum <- 1 to numCrashes) {
          val recoveredProducer = createTransactionalProducer(transactionalId)
          // Use keepPreparedTxn=true to preserve in-flight transaction
          recoveredProducer.initTransactions(true)

          // Verify dual identity after recovery
          val txnDescription = adminClient.describeTransactions(java.util.List.of(transactionalId))
            .description(transactionalId).get()

          // After crash recovery, server should set up dual identity
          assertTrue(txnDescription.producerEpoch() >= baseEpoch,
            s"Crash $crashNum: client epoch should be >= base epoch")

          // For simplicity in this test, we just verify epoch progression.
          // Detailed dual identity verification is in unit tests.

          if (crashNum < numCrashes) {
            // Simulate another crash
            recoveredProducer.close(Duration.ZERO)
          } else {
            // Last recovery - complete the transaction
            producer = recoveredProducer
          }
        }

        // Complete prepared transaction directly.  Cannot call beginTransaction()
        // in prepared state - must call commitTransaction() or abortTransaction() directly.
        if (shouldCommit) {
          producer.commitTransaction()
        } else {
          producer.abortTransaction()
        }

        // Wait for the transaction to fully complete on the server before proceeding.
        // The client-side commitTransaction() returns after receiving the EndTxn response,
        // but the server might still be writing markers and transitioning to COMPLETE_* state.
        val expectedState = if (shouldCommit) TransactionState.COMPLETE_COMMIT else TransactionState.COMPLETE_ABORT
        waitUntilTrue(() => {
          val listResult = adminClient.listTransactions()
          val txns = listResult.all().get().asScala
          txns.exists(txn =>
            txn.transactionalId() == transactionalId &&
            txn.state() == expectedState
          )
        }, s"Transaction did not reach $expectedState state")

        // Verify consumer sees correct records
        consumer.seekToBeginning(consumer.assignment())
        val consumedRecords = consumeRecordsFor(consumer)

        if (shouldCommit) {
          assertEquals(numRecords, consumedRecords.size,
            "Consumer should see all records after commit")
        } else {
          assertEquals(0, consumedRecords.size,
            "Consumer should see no records after abort")
        }

        // Verify fresh transaction works with bumped epoch
        producer.beginTransaction()
        producer.send(new ProducerRecord(testTopic, 0, "fresh-key".getBytes,
          "fresh-value".getBytes))
        producer.commitTransaction()

        // Seek to beginning to read all records (both 2PC and fresh)
        consumer.seekToBeginning(consumer.assignment())
        val allRecords = consumeRecordsFor(consumer)
        val expectedTotal = if (shouldCommit) numRecords + 1 else 1
        assertEquals(expectedTotal, allRecords.size,
          s"Should have $expectedTotal total records after fresh transaction")

        producer.close()

        // Unsubscribe consumer for next test iteration
        consumer.unsubscribe()
      } finally {
        adminClient.close()
      }
    }

    // Test single crash with commit
    test2PCRecovery(numCrashes = 1, shouldCommit = true)

    // Test single crash with abort
    test2PCRecovery(numCrashes = 1, shouldCommit = false)

    // Test multiple crashes before final commit (validates epoch progression)
    test2PCRecovery(numCrashes = 3, shouldCommit = true)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersConsumerGroupProtocolOnly"))
  def testProducerIdRotationWithEpochExhaustion(groupProtocol: String): Unit = {
    // Test producer ID rotation when client epoch is exhausted.  This tests both scenarios:
    // 1. Rotation during initTransactions(keepPreparedTxn=true)
    // 2. Rotation during commitTransaction()

    def testRotation(startEpoch: Short, doubleRotation: Boolean = false): Unit = {
      val transactionalId = s"test-rotation-${System.nanoTime()}"
      val testTopic = s"test-rotation-topic-${System.nanoTime()}"
      createTopic(testTopic, 1, brokerCount, topicConfig())

      val consumer = transactionalConsumers.head
      consumer.subscribe(Seq(testTopic).asJava)
      consumer.poll(Duration.ofMillis(100))

      // Establish transactional ID
      var producer = createTransactionalProducer(transactionalId)
      producer.initTransactions()
      producer.close()

      // Set epoch to trigger rotation at desired point
      setProducerEpoch(transactionalId, startEpoch)

      // Create producer and start a prepared transaction
      producer = createTransactionalProducer(transactionalId)
      producer.initTransactions()
      producer.beginTransaction()
      val numRecords = 3
      for (i <- 0 until numRecords) {
        producer.send(new ProducerRecord(testTopic, 0, s"key-$i".getBytes, s"value-$i".getBytes))
      }
      producer.flush()
      // Don't commit - leave transaction prepared (simulates crash)

      val originalProducerId = getProducerId(transactionalId)

      if (doubleRotation) {
        // First rotation: loop calling initTransactions(true) until rotation occurs
        var rotationCount = 0
        var currentClientId = originalProducerId
        var iteration = 0
        while (rotationCount == 0 && iteration < 20) {
          iteration += 1
          val recoveryProducer = createTransactionalProducer(transactionalId)
          recoveryProducer.initTransactions(true)  // keepPreparedTxn

          val (clientId, _) = getClientProducerIdAndEpoch(transactionalId)
          if (clientId != currentClientId) {
            rotationCount = 1
            currentClientId = clientId
          }
          recoveryProducer.close(Duration.ZERO)
        }

        assertTrue(rotationCount >= 1, s"First rotation should have occurred after $iteration iterations")
        assertEquals(0.toShort, getClientProducerIdAndEpoch(transactionalId)._2,
          "After first rotation, client epoch should be exactly 0")

        // Set client epoch high again to trigger second rotation
        setClientProducerEpoch(transactionalId, startEpoch)

        // Do a compensating epoch bump so that we get at the same epoch as w/o double rotation
        val recoveryProducer = createTransactionalProducer(transactionalId)
        recoveryProducer.initTransactions(true)
        recoveryProducer.close(Duration.ZERO)
      }

      // We do 3 total epoch increments from the initial startEpoch:
      //   1. First initTransactions(): startEpoch → startEpoch + 1
      //   2. Second initTransactions(true): startEpoch + 1 → startEpoch + 2
      //   3. commitTransaction(): startEpoch + 2 → startEpoch + 3
      // Rotation may happen during step 2 or 3 depending on startEpoch.
      val finalProducer = createTransactionalProducer(transactionalId)
      finalProducer.initTransactions(true)  // keepPreparedTxn - may rotate here

      // Complete the transaction - may rotate here
      finalProducer.commitTransaction()

      // Wait for transaction to reach COMPLETE_COMMIT state
      val adminClient = createAdminClient()
      try {
        waitUntilTrue(() => {
          val listResult = adminClient.listTransactions()
          val txns = listResult.all().get().asScala
          txns.exists(txn =>
            txn.transactionalId() == transactionalId &&
            txn.state() == TransactionState.COMPLETE_COMMIT
          )
        }, "Transaction did not reach COMPLETE_COMMIT state")
      } finally {
        adminClient.close()
      }

      // Verify that rotation happened (regardless of whether it occurred during
      // initTransactions() or commitTransaction()). After transaction completes, verify:
      //  - Producer ID changed (rotation occurred)
      //  - Total epoch increments = 3 (accounting for overflow)
      val finalProducerId = getProducerId(transactionalId)
      assertNotEquals(originalProducerId, finalProducerId,
        s"Producer ID should have rotated by transaction completion (original=$originalProducerId, final=$finalProducerId)")

      // Verify epoch overflow occurred and total epoch increments.
      val finalEpoch = getProducerEpoch(transactionalId)

      // Final epoch is less than startEpoch (overflow occurred)
      assertTrue(finalEpoch < startEpoch,
        s"Overflow should have occurred: finalEpoch=$finalEpoch < startEpoch=$startEpoch")

      // Total epoch increments = 3 (accounting for overflow)
      // Formula: finalEpoch + (MaxValue - startEpoch) = total increments
      // This accounts for increments from startEpoch to MaxValue boundary, then
      // wrap to 0 and increments to finalEpoch.
      val totalIncrements = finalEpoch + Short.MaxValue - startEpoch
      assertEquals(3, totalIncrements,
        s"Should have 3 total epoch increments: finalEpoch=$finalEpoch + " +
        s"(MaxValue=${Short.MaxValue} - startEpoch=$startEpoch) = $totalIncrements")

      // Verify consumer sees all records
      consumer.seekToBeginning(consumer.assignment())
      val consumedRecords = consumeRecordsFor(consumer)
      assertEquals(numRecords, consumedRecords.size,
        "Consumer should see all records despite rotation")

      finalProducer.close()
      producer.close()
      consumer.unsubscribe()
    }

    // Scenario 1: Rotation happens during initTransactions(keepPreparedTxn=true) call
    // Example flow:
    //  1. After setProducerEpoch: epoch=32765
    //  2. First initTransactions() + beginTransaction(): epoch=32766, transaction ONGOING
    //  3. initTransactions(keepPreparedTxn=true): tries 32766+1=32767 → rotation triggered
    //     → Creates dual identity: producerId unchanged, nextProducerId=<new>, nextProducerEpoch=0
    //  4. commitTransaction(): Completes transition with nextProducerEpoch bumped 0→1
    //     → Final state: producerId=<new>, epoch=1
    testRotation((Short.MaxValue - 2).toShort)  // 32765

    // Scenario 1 with double rotation: First rotation during iteration, second during final init/commit
    testRotation((Short.MaxValue - 2).toShort, doubleRotation = true)

    // Scenario 2: Rotation happens during commitTransaction() call
    // Example flow:
    //  1. After setProducerEpoch: epoch=32764
    //  2. First initTransactions() + beginTransaction(): epoch=32765, transaction ONGOING
    //  3. initTransactions(keepPreparedTxn=true): bumps to 32766 (not exhausted yet)
    //     → Creates dual identity: producerId unchanged, nextProducerId=<producerId>, nextProducerEpoch=32766
    //  4. commitTransaction(): tries 32766+1=32767 → rotation triggered
    //     → Final state: producerId=<new>, epoch=0
    testRotation((Short.MaxValue - 3).toShort)  // 32764

    // Scenario 2 with double rotation: First rotation during iteration, second during final init/commit
    testRotation((Short.MaxValue - 3).toShort, doubleRotation = true)
  }

  /**
   * Helper method to manually set producer epoch to a high value for testing epoch exhaustion.
   */
  private def setProducerEpoch(transactionalId: String, epoch: Short): Unit = {
    val adminClient = createAdminClient()
    try {
      val txnDescription = adminClient.describeTransactions(java.util.List.of(transactionalId))
        .description(transactionalId).get()
      val coordinatorId = txnDescription.coordinatorId()
      val coordinatorBroker = brokers.find(_.config.brokerId == coordinatorId).get
      val txnCoordinator = coordinatorBroker.asInstanceOf[kafka.server.BrokerServer].transactionCoordinator

      txnCoordinator.transactionManager.getTransactionState(transactionalId).foreach { txnMetadataOpt =>
        txnMetadataOpt.foreach { epochAndMetadata =>
          epochAndMetadata.transactionMetadata.inLock(() => {
            epochAndMetadata.transactionMetadata.setProducerEpoch(epoch)
            null // inLock expects a Supplier that returns a value
          })
        }
      }
    } finally {
      adminClient.close()
    }
  }

  /**
   * Helper method to manually set client producer epoch (nextProducerEpoch) for testing 2PC epoch exhaustion.
   * This is used when testing epoch exhaustion during prepared transactions where client credentials
   * are stored in nextProducerId/nextProducerEpoch.
   */
  private def setClientProducerEpoch(transactionalId: String, epoch: Short): Unit = {
    val adminClient = createAdminClient()
    try {
      val txnDescription = adminClient.describeTransactions(java.util.List.of(transactionalId))
        .description(transactionalId).get()
      val coordinatorId = txnDescription.coordinatorId()
      val coordinatorBroker = brokers.find(_.config.brokerId == coordinatorId).get
      val txnCoordinator = coordinatorBroker.asInstanceOf[kafka.server.BrokerServer].transactionCoordinator

      txnCoordinator.transactionManager.getTransactionState(transactionalId).foreach { txnMetadataOpt =>
        txnMetadataOpt.foreach { epochAndMetadata =>
          epochAndMetadata.transactionMetadata.inLock(() => {
            epochAndMetadata.transactionMetadata.setNextProducerEpoch(epoch)
            null // inLock expects a Supplier that returns a value
          })
        }
      }
    } finally {
      adminClient.close()
    }
  }

  /**
   * Helper method to get current producer ID from DescribeTransactions API.
   */
  private def getProducerId(transactionalId: String): Long = {
    val adminClient = createAdminClient()
    try {
      adminClient.describeTransactions(java.util.List.of(transactionalId))
        .description(transactionalId).get().producerId()
    } finally {
      adminClient.close()
    }
  }

  /**
   * Helper method to get current producer epoch from DescribeTransactions API.
   */
  private def getProducerEpoch(transactionalId: String): Short = {
    val adminClient = createAdminClient()
    try {
      adminClient.describeTransactions(java.util.List.of(transactionalId))
        .description(transactionalId).get().producerEpoch().toShort
    } finally {
      adminClient.close()
    }
  }

  /**
   * Helper method to get client-facing producer ID and epoch (for 2PC dual identity scenarios).
   * When a transaction is prepared, the ongoing transaction keeps its original ID/epoch,
   * but the client gets new credentials (nextProducerId/nextProducerEpoch).
   * Returns (clientProducerId, clientProducerEpoch)
   */
  private def getClientProducerIdAndEpoch(transactionalId: String): (Long, Short) = {
    val adminClient = createAdminClient()
    try {
      val txnDescription = adminClient.describeTransactions(java.util.List.of(transactionalId))
        .description(transactionalId).get()
      val coordinatorId = txnDescription.coordinatorId()
      val coordinatorBroker = brokers.find(_.config.brokerId == coordinatorId).get
      val txnCoordinator = coordinatorBroker.asInstanceOf[kafka.server.BrokerServer].transactionCoordinator

      var clientId: Long = -1
      var clientEpoch: Short = -1
      txnCoordinator.transactionManager.getTransactionState(transactionalId).foreach { txnMetadataOpt =>
        txnMetadataOpt.foreach { epochAndMetadata =>
          clientId = epochAndMetadata.transactionMetadata.clientProducerId()
          clientEpoch = epochAndMetadata.transactionMetadata.clientProducerEpoch()
        }
      }
      (clientId, clientEpoch)
    } finally {
      adminClient.close()
    }
  }
}
