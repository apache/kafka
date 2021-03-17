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
import kafka.utils.TestUtils.consumeRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.errors.InvalidPidMappingException
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

// Test class that uses a very small transaction timeout to trigger InvalidPidMapping errors
class TransactionsExpirationTest extends KafkaServerTestHarness {
  val topic1 = "topic1"
  val topic2 = "topic2"
  val numPartitions = 4
  val replicationFactor = 3

  var producer: KafkaProducer[Array[Byte], Array[Byte]] = _
  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = _

  override def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(3, zkConnect).map(KafkaConfig.fromProps(_, serverProps()))
  }

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    producer = TestUtils.createTransactionalProducer("transactionalProducer", servers)
    consumer = TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(servers),
      enableAutoCommit = false,
      readCommitted = true)

    TestUtils.createTopic(zkClient, topic1, numPartitions, 3, servers, new Properties())
    TestUtils.createTopic(zkClient, topic2, numPartitions, 3, servers, new Properties())
  }

  @AfterEach
  override def tearDown(): Unit = {
    producer.close()
    consumer.close()

    super.tearDown()
  }

  @Test
  def testBumpTransactionalEpochAfterInvalidProducerIdMapping(): Unit = {
    producer.initTransactions()

    // Start and then abort a transaction to allow the transactional ID to expire
    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 0, "2", "2", willBeCommitted = false))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, 0, "4", "4", willBeCommitted = false))
    producer.abortTransaction()

    // Wait for the transactional ID to expire
    Thread.sleep(3000)

    // Start a new transaction and attempt to send, which will trigger an AddPartitionsToTxnRequest, which will fail due to the expired producer ID
    producer.beginTransaction()
    val failedFuture = producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 3, "1", "1", willBeCommitted = false))
    Thread.sleep(500)

    org.apache.kafka.test.TestUtils.assertFutureThrows(failedFuture, classOf[InvalidPidMappingException])
    producer.abortTransaction()

    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "2", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 2, "4", "4", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "1", "1", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, 3, "3", "3", willBeCommitted = true))
    producer.commitTransaction()

    consumer.subscribe(List(topic1, topic2).asJava)

    val records = consumeRecords(consumer, 4)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }
  private def serverProps() = {
    val serverProps = new Properties()
    serverProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
    // Set a smaller value for the number of partitions for the __consumer_offsets topic
    // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
    serverProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)
    serverProps.put(KafkaConfig.TransactionsTopicPartitionsProp, 3.toString)
    serverProps.put(KafkaConfig.TransactionsTopicReplicationFactorProp, 2.toString)
    serverProps.put(KafkaConfig.TransactionsTopicMinISRProp, 2.toString)
    serverProps.put(KafkaConfig.ControlledShutdownEnableProp, true.toString)
    serverProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, false.toString)
    serverProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
    serverProps.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    serverProps.put(KafkaConfig.TransactionsAbortTimedOutTransactionCleanupIntervalMsProp, "200")
    serverProps.put(KafkaConfig.TransactionalIdExpirationMsProp, "2000")
    serverProps.put(KafkaConfig.TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp, "500")
    serverProps
  }
}
