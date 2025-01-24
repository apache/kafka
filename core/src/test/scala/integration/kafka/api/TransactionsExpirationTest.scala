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

import java.util.{Collections, Properties}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.utils.TestUtils.{consumeRecords, createAdminClient}
import org.apache.kafka.clients.admin.{Admin, ProducerState}
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{InvalidPidMappingException, TransactionalIdNotFoundException}
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.jdk.CollectionConverters._
import scala.collection.Seq

// Test class that uses a very small transaction timeout to trigger InvalidPidMapping errors
class TransactionsExpirationTest extends KafkaServerTestHarness {
  val topic1 = "topic1"
  val topic2 = "topic2"
  val numPartitions = 4
  val replicationFactor = 3
  val tp0 = new TopicPartition(topic1, 0)

  var producer: KafkaProducer[Array[Byte], Array[Byte]] = _
  var consumer: Consumer[Array[Byte], Array[Byte]] = _
  var admin: Admin = _

  override def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(3).map(KafkaConfig.fromProps(_, serverProps()))
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    producer = TestUtils.createTransactionalProducer("transactionalProducer", brokers)
    consumer = TestUtils.createConsumer(bootstrapServers(),
      groupProtocolFromTestParameters(),
      enableAutoCommit = false,
      readCommitted = true)
    admin = createAdminClient(brokers, listenerName)

    createTopic(topic1, numPartitions, 3)
    createTopic(topic2, numPartitions, 3)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (producer != null)
      producer.close()
    if (consumer != null)
      consumer.close()
    if (admin != null)
      admin.close()

    super.tearDown()
  }

  @ParameterizedTest
  @CsvSource(Array(
    "kraft,classic,false",
    "kraft,consumer,false",
    "kraft,classic,true",
    "kraft,consumer,true",
  ))
  def testFatalErrorAfterInvalidProducerIdMapping(quorum: String, groupProtocol: String, isTV2Enabled: Boolean): Unit = {
    producer.initTransactions()

    // Start and then abort a transaction to allow the transactional ID to expire.
    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "2", "2", willBeCommitted = false))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, 0, "4", "4", willBeCommitted = false))
    producer.abortTransaction()

    // Check the transactional state exists and then wait for it to expire.
    waitUntilTransactionalStateExists()
    waitUntilTransactionalStateExpires()

    // Start a new transaction and attempt to send, triggering an AddPartitionsToTxnRequest that will fail
    // due to the expired transactional ID, resulting in a fatal error.
    producer.beginTransaction()
    val failedFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 3, "1", "1", willBeCommitted = false))
    TestUtils.waitUntilTrue(() => failedFuture.isDone, "Producer future never completed.")
    org.apache.kafka.test.TestUtils.assertFutureThrows(failedFuture, classOf[InvalidPidMappingException])

    // Assert that aborting the transaction throws a KafkaException due to the fatal error.
    assertThrows(classOf[KafkaException], () => producer.abortTransaction())

    // Close the producer and reinitialize to recover from the fatal error.
    producer.close()
    producer = TestUtils.createTransactionalProducer("transactionalProducer", brokers)
    producer.initTransactions()

    // Proceed with a new transaction after reinitializing.
    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "2", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 2, "4", "4", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "1", "1", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 3, "3", "3", willBeCommitted = true))
    producer.commitTransaction()

    waitUntilTransactionalStateExists()

    consumer.subscribe(List(topic1, topic2).asJava)

    val records = consumeRecords(consumer, 4)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  @ParameterizedTest(name = "{displayName}.quorum={0}.groupProtocol={1}.isTV2Enabled={2}")
  @CsvSource(Array(
    "kraft,classic,false",
    "kraft,consumer,false",
    "kraft,classic,true",
    "kraft,consumer,true",
  ))
  def testTransactionAfterProducerIdExpires(quorum: String, groupProtocol: String, isTV2Enabled: Boolean): Unit = {
    producer.initTransactions()

    // Start and then abort a transaction to allow the producer ID to expire.
    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "2", "2", willBeCommitted = false))
    producer.flush()

    // Ensure producer IDs are added.
    var pState : List[ProducerState] = null
    TestUtils.waitUntilTrue(() => { pState = producerState; pState.nonEmpty}, "Producer IDs for topic1 did not propagate quickly")
    assertEquals(1, pState.size, "Unexpected producer to topic1")
    val oldProducerId = pState.head.producerId
    val oldProducerEpoch = pState.head.producerEpoch

    producer.abortTransaction()

    // Wait for the producer ID to expire.
    TestUtils.waitUntilTrue(() => producerState.isEmpty, "Producer IDs for topic1 did not expire.")

    // Create a new producer to check that we retain the producer ID in transactional state.
    producer.close()
    producer = TestUtils.createTransactionalProducer("transactionalProducer", brokers)
    producer.initTransactions()

    // Start a new transaction and attempt to send. This should work since only the producer ID was removed from its mapping in ProducerStateManager.
    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "4", "4", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 3, "3", "3", willBeCommitted = true))
    producer.commitTransaction()

    // Producer IDs should repopulate.
    var pState2 : List[ProducerState] = null
    TestUtils.waitUntilTrue(() => {pState2 = producerState; pState2.nonEmpty}, "Producer IDs for topic1 did not propagate quickly")
    assertEquals(1, pState2.size, "Unexpected producer to topic1")
    val newProducerId = pState2.head.producerId
    val newProducerEpoch = pState2.head.producerEpoch

    // Because the transaction IDs outlive the producer IDs, creating a producer with the same transactional id
    // soon after the first will re-use the same producerId, while bumping the epoch to indicate that they are distinct.
    assertEquals(oldProducerId, newProducerId)
    if (isTV2Enabled) {
      // TV2 bumps epoch on EndTxn, and the final commit may or may not have bumped the epoch in the producer state.
      // The epoch should be at least oldProducerEpoch + 2 for the first commit and the restarted producer.
      assertTrue(oldProducerEpoch + 2 <= newProducerEpoch)
    } else {
      assertEquals(oldProducerEpoch + 1, newProducerEpoch)
    }

    consumer.subscribe(List(topic1).asJava)

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  private def producerState: List[ProducerState] = {
    val describeResult = admin.describeProducers(Collections.singletonList(tp0))
    val activeProducers = describeResult.partitionResult(tp0).get().activeProducers
    activeProducers.asScala.toList
  }

  private def waitUntilTransactionalStateExpires(): Unit = {
    TestUtils.waitUntilTrue(() =>  {
      var removedTransactionState = false
      val txnDescribeResult = admin.describeTransactions(Collections.singletonList("transactionalProducer")).description("transactionalProducer")
      try {
        txnDescribeResult.get()
      } catch {
        case e: Exception => {
          removedTransactionState = e.getCause.isInstanceOf[TransactionalIdNotFoundException]
        }
      }
      removedTransactionState
    }, "Transaction state never expired.")
  }

  private def waitUntilTransactionalStateExists(): Unit = {
    val describeState = admin.describeTransactions(Collections.singletonList("transactionalProducer")).description("transactionalProducer")
    TestUtils.waitUntilTrue(() => describeState.isDone, "Transactional state was never added.")
  }

  private def serverProps(): Properties = {
    val serverProps = new Properties()
    serverProps.put(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, false.toString)
    // Set a smaller value for the number of partitions for the __consumer_offsets topic
    // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long.
    serverProps.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, 1.toString)
    serverProps.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, 3.toString)
    serverProps.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, 2.toString)
    serverProps.put(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, 2.toString)
    serverProps.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, true.toString)
    serverProps.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, false.toString)
    serverProps.put(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, false.toString)
    serverProps.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0")
    serverProps.put(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, "200")
    serverProps.put(TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG, "10000")
    serverProps.put(TransactionStateManagerConfig.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG, "500")
    serverProps.put(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG, "5000")
    serverProps.put(TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG, "500")
    serverProps
  }
}
