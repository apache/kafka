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

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.{After, Before, Ignore, Test}
import org.junit.Assert._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionException
import scala.util.Random

class TransactionsTest extends KafkaServerTestHarness {
  val numServers = 3
  val topic1 = "topic1"
  val topic2 = "topic2"


  override def generateConfigs : Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(numServers, zkConnect, true).map(KafkaConfig.fromProps(_, serverProps()))
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val numPartitions = 3
    val topicConfig = new Properties()
    topicConfig.put(KafkaConfig.MinInSyncReplicasProp, 2.toString)
    TestUtils.createTopic(zkUtils, topic1, numPartitions, numServers, servers, topicConfig)
    TestUtils.createTopic(zkUtils, topic2, numPartitions, numServers, servers, topicConfig)
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
  }

  @Test
  def testBasicTransactions() = {
    val producer = TestUtils.createTransactionalProducer("my-hello-world-transactional-id", servers)
    val consumer = transactionalConsumer("transactional-group")
    val unCommittedConsumer = nonTransactionalConsumer("non-transactional-group")
    try {
      producer.initTransactions()

      producer.beginTransaction()
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "2", "2", willBeCommitted = false))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "4", "4", willBeCommitted = false))
      producer.abortTransaction()

      producer.beginTransaction()
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "1", "1", willBeCommitted = true))
      producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "3", "3", willBeCommitted = true))
      producer.commitTransaction()

      consumer.subscribe(List(topic1, topic2))
      unCommittedConsumer.subscribe(List(topic1, topic2))

      val records = pollUntilExactlyNumRecords(consumer, 2)
      records.zipWithIndex.foreach { case (record, i) =>
        TestUtils.assertCommittedAndGetValue(record)
      }

      val allRecords = pollUntilExactlyNumRecords(unCommittedConsumer, 4)
      val expectedValues = List("1", "2", "3", "4").toSet
      allRecords.zipWithIndex.foreach { case (record, i) =>
        assertTrue(expectedValues.contains(TestUtils.recordValueAsString(record)))
      }
    } finally {
      consumer.close()
      producer.close()
      unCommittedConsumer.close()
    }
  }

  @Test
  def testSendOffsets() = {
    // The basic plan for the test is as follows:
    //  1. Seed topic1 with 1000 unique, numbered, messages.
    //  2. Run a consume/process/produce loop to transactionally copy messages from topic1 to topic2 and commit
    //     offsets as part of the transaction.
    //  3. Randomly abort transactions in step2.
    //  4. Validate that we have 1000 unique committed messages in topic2. If the offsets were committed properly with the
    //     transactions, we should not have any duplicates or missing messages since we should process in the input
    //     messages exactly once.

    val transactionalId = "foobar-id"
    val consumerGroupId = "foobar-consumer-group"
    val numSeedMessages = 500

    TestUtils.seedTopicWithNumberedRecords(topic1, numSeedMessages, servers)

    val producer = TestUtils.createTransactionalProducer(transactionalId, servers)

    val consumer = transactionalConsumer(consumerGroupId, maxPollRecords = numSeedMessages / 4)
    consumer.subscribe(List(topic1))
    producer.initTransactions()

    val random = new Random()
    var shouldCommit = false
    var recordsProcessed = 0
    try {
      while (recordsProcessed < numSeedMessages) {
        producer.beginTransaction()
        shouldCommit = !shouldCommit

        val records = TestUtils.pollUntilAtLeastNumRecords(consumer, Math.min(10, numSeedMessages - recordsProcessed))
        records.zipWithIndex.foreach { case (record, i) =>
          val key = new String(record.key(), "UTF-8")
          val value = new String(record.value(), "UTF-8")
          producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, key, value, willBeCommitted = shouldCommit))
        }

        producer.sendOffsetsToTransaction(TestUtils.consumerPositions(consumer), consumerGroupId)
        if (shouldCommit) {
          producer.commitTransaction()
          recordsProcessed += records.size
          debug(s"committed transaction.. Last committed record: ${new String(records.last.value(), "UTF-8")}. Num " +
            s"records written to $topic2: $recordsProcessed")
        } else {
          producer.abortTransaction()
          debug(s"aborted transaction Last committed record: ${new String(records.last.value(), "UTF-8")}. Num " +
            s"records written to $topic2: $recordsProcessed")
          TestUtils.resetToCommittedPositions(consumer)
       }
      }
    } finally {
      producer.close()
      consumer.close()
    }

    // Inspite of random aborts, we should still have exactly 1000 messages in topic2. Ie. we should not
    // re-copy or miss any messages from topic1, since the consumed offsets were committed transactionally.
    val verifyingConsumer = transactionalConsumer("foobargroup")
    verifyingConsumer.subscribe(List(topic2))
    val valueSeq = TestUtils.pollUntilAtLeastNumRecords(verifyingConsumer, numSeedMessages).map { record =>
      TestUtils.assertCommittedAndGetValue(record).toInt
    }
    verifyingConsumer.close()
    val valueSet = valueSeq.toSet
    assertEquals(s"Expected $numSeedMessages values in $topic2.", numSeedMessages, valueSeq.size)
    assertEquals(s"Expected ${valueSeq.size} unique messages in $topic2.", valueSeq.size, valueSet.size)
  }

  @Test
  def testFencingOnCommit() = {
    val transactionalId = "my-t.id"
    val producer1 = TestUtils.createTransactionalProducer(transactionalId, servers)
    val producer2 = TestUtils.createTransactionalProducer(transactionalId, servers)
    val consumer = transactionalConsumer()
    consumer.subscribe(List(topic1, topic2))

    try {
      producer1.initTransactions()

      producer1.beginTransaction()
      producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "1", "1", willBeCommitted = false))
      producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "3", "3", willBeCommitted = false))

      producer2.initTransactions()  // ok, will abort the open transaction.
      producer2.beginTransaction()
      producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "2", "4", willBeCommitted = true))
      producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "2", "4", willBeCommitted = true))

      try {
        producer1.commitTransaction()
        fail("Should not be able to commit transactions from a fenced producer.")
      } catch {
        case e : ProducerFencedException =>
          // good!
        case e : Exception =>
          fail("Got an unexpected exception from a fenced producer.", e)
      }

      producer2.commitTransaction()  // ok

      val records = pollUntilExactlyNumRecords(consumer, 2)
      records.zipWithIndex.foreach { case (record, i) =>
        TestUtils.assertCommittedAndGetValue(record)
      }
    } finally {
      consumer.close()
      producer1.close()
      producer2.close()
    }
  }

  @Test
  def testFencingOnSendOffsets() = {
    val transactionalId = "my-t.id"
    val producer1 = TestUtils.createTransactionalProducer(transactionalId, servers)
    val producer2 = TestUtils.createTransactionalProducer(transactionalId, servers)
    val consumer = transactionalConsumer()
    consumer.subscribe(List(topic1, topic2))

    try {
      producer1.initTransactions()

      producer1.beginTransaction()
      producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "1", "1", willBeCommitted = false))
      producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "3", "3", willBeCommitted = false))

      producer2.initTransactions()  // ok, will abort the open transaction.
      producer2.beginTransaction()
      producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "2", "4", willBeCommitted = true))
      producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "2", "4", willBeCommitted = true))

      try {
        producer1.sendOffsetsToTransaction(Map(new TopicPartition("foobartopic", 0) -> new OffsetAndMetadata(110L)),  "foobarGroup")
        fail("Should not be able to send offsets from a fenced producer.")
      } catch {
        case e : ProducerFencedException =>
          // good!
        case e : Exception =>
          fail("Got an unexpected exception from a fenced producer.", e)
      }

      producer2.commitTransaction()  // ok

      val records = pollUntilExactlyNumRecords(consumer, 2)
      records.zipWithIndex.foreach { case (record, i) =>
        TestUtils.assertCommittedAndGetValue(record)
      }
    } finally {
      consumer.close()
      producer1.close()
      producer2.close()
    }
  }

  @Test
  def testFencingOnSend() {
    val transactionalId = "my-t.id"
    val producer1 = TestUtils.createTransactionalProducer(transactionalId, servers)
    val producer2 = TestUtils.createTransactionalProducer(transactionalId, servers)
    val consumer = transactionalConsumer()
    consumer.subscribe(List(topic1, topic2))

    try {
      producer1.initTransactions()

      producer1.beginTransaction()
      producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "1", "1", willBeCommitted = false))
      producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "3", "3", willBeCommitted = false))

      producer2.initTransactions()  // ok, will abort the open transaction.
      producer2.beginTransaction()
      producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "2", "4", willBeCommitted = true)).get()
      producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "2", "4", willBeCommitted = true)).get()

      try {
        val result =  producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "1", "5", willBeCommitted = false))
        val recordMetadata = result.get()
        error(s"Missed a producer fenced exception when writing to ${recordMetadata.topic()}-${recordMetadata.partition()}. Grab the logs!!")
        servers.foreach { case (server) =>
          error(s"log dirs: ${server.logManager.logDirs.map(_.getAbsolutePath).head}")
        }
        fail("Should not be able to send messages from a fenced producer.")
      } catch {
        case e : ProducerFencedException =>
          producer1.close()
        case e : ExecutionException =>
          assertTrue(e.getCause.isInstanceOf[ProducerFencedException])
        case e : Exception =>
          fail("Got an unexpected exception from a fenced producer.", e)
      }

      producer2.commitTransaction()  // ok

      val records = pollUntilExactlyNumRecords(consumer, 2)
      records.zipWithIndex.foreach { case (record, i) =>
        TestUtils.assertCommittedAndGetValue(record)
      }
    } finally {
      consumer.close()
      producer1.close()
      producer2.close()
    }
  }

  @Test
  def testFencingOnAddPartitions(): Unit = {
    val transactionalId = "my-t.id"
    val producer1 = TestUtils.createTransactionalProducer(transactionalId, servers)
    val producer2 = TestUtils.createTransactionalProducer(transactionalId, servers)
    val consumer = transactionalConsumer()
    consumer.subscribe(List(topic1, topic2))

    try {
      producer1.initTransactions()

      producer1.beginTransaction()
      producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "1", "1", willBeCommitted = false))
      producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "3", "3", willBeCommitted = false))
      producer1.abortTransaction()

      producer2.initTransactions()  // ok, will abort the open transaction.
      producer2.beginTransaction()
      producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "2", "4", willBeCommitted = true)).get()
      producer2.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, "2", "4", willBeCommitted = true)).get()

      try {
        producer1.beginTransaction()
        val result =  producer1.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, "1", "5", willBeCommitted = false))
        val recordMetadata = result.get()
        error(s"Missed a producer fenced exception when writing to ${recordMetadata.topic()}-${recordMetadata.partition()}. Grab the logs!!")
        servers.foreach { case (server) =>
          error(s"log dirs: ${server.logManager.logDirs.map(_.getAbsolutePath).head}")
        }
        fail("Should not be able to send messages from a fenced producer.")
      } catch {
        case e : ProducerFencedException =>
          producer1.close()
        case e : ExecutionException =>
          assertTrue(e.getCause.isInstanceOf[ProducerFencedException])
        case e : Exception =>
          fail("Got an unexpected exception from a fenced producer.", e)
      }

      producer2.commitTransaction()  // ok

      val records = pollUntilExactlyNumRecords(consumer, 2)
      records.zipWithIndex.foreach { case (record, i) =>
        TestUtils.assertCommittedAndGetValue(record)
      }
    } finally {
      consumer.close()
      producer1.close()
      producer2.close()
    }
  }

  private def serverProps() = {
    val serverProps = new Properties()
    serverProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
    // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
    // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
    serverProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)
    serverProps.put(KafkaConfig.TransactionsTopicPartitionsProp, 3.toString)
    serverProps.put(KafkaConfig.TransactionsTopicReplicationFactorProp, 2.toString)
    serverProps.put(KafkaConfig.TransactionsTopicMinISRProp, 2.toString)
    serverProps.put(KafkaConfig.ControlledShutdownEnableProp, true.toString)
    serverProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, false.toString)
    serverProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
    serverProps
  }

  private def transactionalConsumer(group: String = "group", maxPollRecords: Int = 500) = {
    val props = new Properties()
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers),
      groupId = group, securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(props))
  }

  private def nonTransactionalConsumer(group: String = "group") = {
    val props = new Properties()
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers),
      groupId = group, securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(props))
  }

  private def pollUntilExactlyNumRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int) : Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val records = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]()
    TestUtils.waitUntilTrue(() => {
      records ++= consumer.poll(50)
      records.size == numRecords
    }, s"Consumed ${records.size} records until timeout, but expected $numRecords records.")
    records
  }

}
