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
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.{After, Before, Ignore, Test}
import org.junit.Assert._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
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
    val topicConfig = new Properties();
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
    val producer = transactionalProducer("my-hello-world-transactional-id")
    val consumer = transactionalConsumer()
    try {
      producer.initTransactions()

      producer.beginTransaction()
      producer.send(producerRecord(topic1, "1", "1", willBeCommitted = true))
      producer.send(producerRecord(topic2, "3", "3", willBeCommitted = true))
      producer.commitTransaction()

      producer.beginTransaction()
      producer.send(producerRecord(topic2, "2", "2", willBeCommitted = false))
      producer.send(producerRecord(topic1, "4", "4", willBeCommitted = false))
      producer.abortTransaction()

      consumer.subscribe(List(topic1, topic2))

      val records = pollUntilExactlyNumRecords(consumer, 2)
      records.zipWithIndex.foreach { case (record, i) =>
        assertCommittedAndGetValue(record)
      }
    } catch {
      case e @ (_ : KafkaException | _ : ProducerFencedException) =>
        fail("Did not expect exception", e)
    } finally {
      consumer.close()
      producer.close()
    }
  }

  @Ignore @Test
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
    seedTopicWithRecords(topic1, 1000)

    val producer = transactionalProducer(transactionalId)

    var consumer = transactionalConsumer(consumerGroupId)
    consumer.subscribe(List(topic1))
    producer.initTransactions()

    val random = new Random()
    var recordsProcessed = 0
    try {
      while (recordsProcessed < 1000) {
        producer.beginTransaction()
        val shouldCommit = if (random.nextInt() % 2 == 0) true else false

        val records = pollUntilAtLeastNumRecords(consumer, Math.min(10, 1000 - recordsProcessed))
        records.zipWithIndex.foreach { case (record, i) =>
          val key = new String(record.key(), "UTF-8")
          val value = new String(record.value(), "UTF-8")
          producer.send(producerRecord(topic2, key, value, willBeCommitted = shouldCommit))
        }

        producer.sendOffsetsToTransaction(offsetsToCommit(consumer), consumerGroupId)
        if (shouldCommit) {
          producer.commitTransaction()
          recordsProcessed += records.size
          debug(s"committed transaction.. Last committed record: ${new String(records.last.value(), "UTF-8")}. Num " +
            s"records written to $topic2: $recordsProcessed")
        } else {
          producer.abortTransaction()
          trace("aborted transaction")
          // reset the subscription. Practically, we will have a new consumer instance when transactions are aborted
          // (like during a streams re balance). But we need to do this manually for this test, otherwise the consumer
          // will continue to fetch from the cached fetch position and the test will fail because it will fail to
          // produce the expected number of messages to the destination topic since some of the aborted transactions
          // will not result in offset resets.
          resetToLastCommittedPosition(consumer)
        }
      }
    } catch {
      case e : Exception =>
        fail ("Received an unexpected exception during the 'consume-process-produce' loop", e)
    } finally {
      producer.close()
      consumer.close()
    }

    // Inspite of random aborts, we should still have exactly 1000 messages in topic2. Ie. we should not
    // re-copy or miss any messages from topic1, since the consumed offsets were committed transactionally.
    val verifyingConsumer = transactionalConsumer("foobargroup")
    verifyingConsumer.subscribe(List(topic2))
    val valueSeq = pollUntilAtLeastNumRecords(verifyingConsumer, 1000).map { record =>
      assertCommittedAndGetValue(record).toInt
    }
    verifyingConsumer.close()
    val valueSet = valueSeq.toSet
    assertEquals(s"Expected 1000 values in $topic2.", 1000, valueSeq.size)
    assertEquals(s"Expected ${valueSeq.size} unique messages in $topic2.", valueSeq.size, valueSet.size)
  }

  @Test
  def testFencing() = {
    val transactionalId = "my-t.id"
    val producer1 = transactionalProducer(transactionalId)
    val producer2 = transactionalProducer(transactionalId)
    val consumer = transactionalConsumer()
    consumer.subscribe(List(topic1, topic2))

    try {
      producer1.initTransactions()

      producer1.beginTransaction()
      producer1.send(producerRecord(topic1, "1", "1", willBeCommitted = false))
      producer1.send(producerRecord(topic2, "3", "3", willBeCommitted = false))

      producer2.initTransactions()  // ok, will abort the open transaction.
      producer2.beginTransaction()
      producer2.send(producerRecord(topic1, "2", "4", willBeCommitted = true))
      producer2.send(producerRecord(topic2, "2", "4", willBeCommitted = true))

      try {
        producer1.commitTransaction()
        fail("Should not be able to commit transactions from a fenced producer.")
      } catch {
        case e : ProducerFencedException =>
          // good!
          producer1.close()
        case e : Exception =>
          fail("Got an unexpected exception from a fenced producer.", e)
      }

      producer2.commitTransaction()  // ok

      val records = pollUntilExactlyNumRecords(consumer, 2)
      records.zipWithIndex.foreach { case (record, i) =>
        assertCommittedAndGetValue(record)
      }
    } catch {
      case e @ (_ : KafkaException | _ : ProducerFencedException) =>
        fail("Did not expect exception", e)
    } finally {
      consumer.close()
      producer2.close()
    }
  }

  // Verifies that the record was intended to be committed by checking the suffix of the value. If true, this
  // will return the value with the '-committed' suffix removed.
  private def assertCommittedAndGetValue(record: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
    val recordValue = new String(record.value(), "UTF-8")
    assertTrue(recordValue.endsWith("committed"))
    recordValue.replace("-committed", "")
  }

  private def producerRecord(topic: String, key: String, value: String, willBeCommitted: Boolean) = {
    val suffixedValue = if (willBeCommitted)
      value + "-committed"
    else
      value + "-aborted"
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes("UTF-8"), suffixedValue.getBytes("UTF-8"))
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

  private def transactionalProducer(transactionalId: String) = {
    val props = new Properties()
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers), retries = Integer.MAX_VALUE, acks = -1, props = Some(props))
  }

  private def transactionalConsumer(group: String = "group") = {
    val props = new Properties()
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
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

  private def pollUntilAtLeastNumRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int) : Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val records = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]()
    TestUtils.waitUntilTrue(() => {
      records ++= consumer.poll(50)
      records.size >= numRecords
    }, s"Consumed ${records.size} records until timeout, but expected $numRecords records.")
    records
  }

  private def seedTopicWithRecords(topic: String, numRecords: Int): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    var recordsWritten = 0
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers), retries = Integer.MAX_VALUE, acks = -1, props = Some(props))
    try {
      for (i <- 0 until numRecords) {
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, i.toString.getBytes("UTF-8"), i.toString.getBytes("UTF-8"))).get()
        recordsWritten += 1
      }
    } catch {
      case e : Exception =>
        fail("Producer failed to send record with exception", e)
    } finally {
      producer.close()
    }
    debug(s"Wrote $recordsWritten records to $topic.")
  }

  private def offsetsToCommit(consumer: KafkaConsumer[Array[Byte], Array[Byte]]) : Map[TopicPartition, OffsetAndMetadata]  = {
    val offsetsToCommit = new mutable.HashMap[TopicPartition, OffsetAndMetadata]()
    consumer.assignment().foreach{ topicPartition =>
      offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
    }
    offsetsToCommit.toMap
  }

  private def resetToLastCommittedPosition(consumer: KafkaConsumer[Array[Byte], Array[Byte]]) = {
    consumer.assignment().foreach { topicPartition =>
      val committedOffset = consumer.committed(topicPartition)
      if (committedOffset == null)
        consumer.seekToBeginning(List(topicPartition))
      else
        consumer.seek(topicPartition, committedOffset.offset())
    }
  }
}
