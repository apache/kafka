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

import java.util
import java.util.{Collections, List, Map, Properties}

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import kafka.utils.TestUtils.{consumeRecords, createAdminClient}
import org.apache.kafka.clients.admin.{Admin,AlterConfigOp, ConfigEntry, ProducerState}
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{InvalidPidMappingException, TransactionalIdNotFoundException}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.opentest4j.AssertionFailedError

import scala.collection.Seq

class ProducerIdExpirationTest extends KafkaServerTestHarness {
  val topic1 = "topic1"
  val numPartitions = 1
  val replicationFactor = 3
  val tp0 = new TopicPartition(topic1, 0)
  val configResource = new ConfigResource(ConfigResource.Type.BROKER, "")

  var producer: KafkaProducer[Array[Byte], Array[Byte]] = _
  var consumer: Consumer[Array[Byte], Array[Byte]] = _
  var admin: Admin = _

  override def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(3, zkConnectOrNull).map(KafkaConfig.fromProps(_, serverProps()))
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    consumer = TestUtils.createConsumer(bootstrapServers(),
      enableAutoCommit = false,
      readCommitted = true)
    admin = createAdminClient(brokers, listenerName)

    createTopic(topic1, numPartitions, 3)
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testProducerIdExpirationWithNoTransactions(quorum: String): Unit = {
    producer = TestUtils.createProducer(bootstrapServers(), enableIdempotence = true)

    // Send records to populate producer state cache.
    producer.send(new ProducerRecord(topic1, 0, null, "key".getBytes, "value".getBytes))
    producer.flush()

    // Ensure producer IDs are added.
    ensureConsistentKRaftMetadata()
    assertEquals(1, producerState.size)

    // Wait for the producer ID to expire.
    TestUtils.waitUntilTrue(() => producerState.isEmpty, "Producer ID did not expire.")

    // Send more records to send producer ID back to brokers.
    producer.send(new ProducerRecord(topic1, 0, null, "key".getBytes, "value".getBytes))
    producer.flush()

    // Producer IDs should repopulate.
    assertEquals(1, producerState.size)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTransactionAfterTransactionIdExpiresButProducerIdRemains(quorum: String): Unit = {
    producer = TestUtils.createTransactionalProducer("transactionalProducer", brokers)
    producer.initTransactions()

    // Start and then abort a transaction to allow the producer ID to expire.
    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "2", "2", willBeCommitted = false))
    producer.flush()

    // Ensure producer IDs are added.
    TestUtils.waitUntilTrue(() => producerState.size == 1, "Producer IDs were not added.")

    producer.abortTransaction()

    // Wait for the transactional ID to expire.
    waitUntilTransactionalStateExpires()

    // Producer IDs should be retained.
    assertEquals(1, producerState.size)

    // Start a new transaction and attempt to send, which will trigger an AddPartitionsToTxnRequest, which will fail due to the expired transactional ID.
    producer.beginTransaction()
    val failedFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "1", "1", willBeCommitted = false))
    TestUtils.waitUntilTrue(() => failedFuture.isDone, "Producer future never completed.")

    JTestUtils.assertFutureThrows(failedFuture, classOf[InvalidPidMappingException])
    producer.abortTransaction()

    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "4", "4", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "3", "3", willBeCommitted = true))

    // Producer IDs should be retained.
    assertEquals(1, producerState.size)

    producer.commitTransaction()

    // Check we can still consume the transaction.
    consumer.subscribe(Collections.singletonList(topic1))

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDynamicProducerIdExpirationMs(quorum: String): Unit = {
    producer = TestUtils.createProducer(bootstrapServers(), enableIdempotence = true)

    // Send records to populate producer state cache.
    producer.send(new ProducerRecord(topic1, 0, null, "key".getBytes, "value".getBytes))
    producer.flush()

    // Ensure producer IDs are added.
    ensureConsistentKRaftMetadata()
    assertEquals(1, producerState.size)

    // Wait for the producer ID to expire.
    TestUtils.waitUntilTrue(() => producerState.isEmpty, "Producer ID did not expire.")

    // Update the producer ID expiration ms to a very high value.
    admin.incrementalAlterConfigs(producerIdExpirationConfig("100000"))

    brokers.foreach(broker => TestUtils.waitUntilTrue(() => broker.logManager.producerStateManagerConfig.producerIdExpirationMs == 100000, "Configuration was not updated."))

    // Send more records to send producer ID back to brokers.
    producer.send(new ProducerRecord(topic1, 0, null, "key".getBytes, "value".getBytes))
    producer.flush()

    // Producer IDs should repopulate.
    assertEquals(1, producerState.size)

    // Ensure producer ID does not expire within 4 seconds.
    assertThrows(classOf[AssertionFailedError], () =>
      TestUtils.waitUntilTrue(() => producerState.isEmpty, "Producer ID did not expire.", 4000)
    )

    // Update the expiration time to a low value again.
    admin.incrementalAlterConfigs(producerIdExpirationConfig("100")).all().get()

    // restart a broker to ensure that dynamic config changes are picked up on restart
    killBroker(0)
    restartDeadBrokers()

    brokers.foreach(broker => TestUtils.waitUntilTrue(() => broker.logManager.producerStateManagerConfig.producerIdExpirationMs == 100, "Configuration was not updated."))

    // Ensure producer ID expires quickly again.
    TestUtils.waitUntilTrue(() => producerState.isEmpty, "Producer ID did not expire.")
  }

  private def producerState: List[ProducerState] = {
    val describeResult = admin.describeProducers(Collections.singletonList(tp0))
    val activeProducers = describeResult.partitionResult(tp0).get().activeProducers
    activeProducers
  }

  private def producerIdExpirationConfig(configValue: String): Map[ConfigResource, util.Collection[AlterConfigOp]] = {
    val producerIdCfg = new ConfigEntry(KafkaConfig.ProducerIdExpirationMsProp, configValue)
    val configs = Collections.singletonList(new AlterConfigOp(producerIdCfg, AlterConfigOp.OpType.SET))
    Collections.singletonMap(configResource, configs)
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

  private def serverProps(): Properties = {
    val serverProps = new Properties()
    serverProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
    // Set a smaller value for the number of partitions for the __consumer_offsets topic
    // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long.
    serverProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)
    serverProps.put(KafkaConfig.TransactionsTopicPartitionsProp, 3.toString)
    serverProps.put(KafkaConfig.TransactionsTopicReplicationFactorProp, 2.toString)
    serverProps.put(KafkaConfig.TransactionsTopicMinISRProp, 2.toString)
    serverProps.put(KafkaConfig.ControlledShutdownEnableProp, true.toString)
    serverProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, false.toString)
    serverProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
    serverProps.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    serverProps.put(KafkaConfig.TransactionsAbortTimedOutTransactionCleanupIntervalMsProp, "200")
    serverProps.put(KafkaConfig.TransactionalIdExpirationMsProp, "5000")
    serverProps.put(KafkaConfig.TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp, "500")
    serverProps.put(KafkaConfig.ProducerIdExpirationMsProp, "10000")
    serverProps.put(KafkaConfig.ProducerIdExpirationCheckIntervalMsProp, "500")
    serverProps
  }
}
