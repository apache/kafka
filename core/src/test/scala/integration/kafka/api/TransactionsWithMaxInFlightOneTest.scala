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

package integration.kafka.api

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.utils.TestUtils.consumeRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.junit.{After, Before, Test}
import org.junit.Assert.assertEquals

import scala.collection.Seq
import scala.collection.mutable.Buffer
import scala.jdk.CollectionConverters._

/**
 * This is used to test transactions with one broker and `max.in.flight.requests.per.connection=1`.
 * A single broker is used to verify edge cases where different requests are queued on the same connection.
 */
class TransactionsWithMaxInFlightOneTest extends KafkaServerTestHarness {
  val numServers = 1

  val topic1 = "topic1"
  val topic2 = "topic2"
  val numPartitions = 4

  val transactionalProducers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()
  val transactionalConsumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()

  override def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(numServers, zkConnect).map(KafkaConfig.fromProps(_, serverProps()))
  }

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val topicConfig = new Properties()
    topicConfig.put(KafkaConfig.MinInSyncReplicasProp, 1.toString)
    createTopic(topic1, numPartitions, numServers, topicConfig)
    createTopic(topic2, numPartitions, numServers, topicConfig)

    createTransactionalProducer("transactional-producer")
    createReadCommittedConsumer("transactional-group")
  }

  @After
  override def tearDown(): Unit = {
    transactionalProducers.foreach(_.close())
    transactionalConsumers.foreach(_.close())
    super.tearDown()
  }

  @Test
  def testTransactionalProducerSingleBrokerMaxInFlightOne() = {
    // We want to test with one broker to verify multiple requests queued on a connection
    assertEquals(1, servers.size)

    val producer = transactionalProducers.head
    val consumer = transactionalConsumers.head

    producer.initTransactions()

    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "2", willBeCommitted = false))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "4", "4", willBeCommitted = false))
    producer.flush()
    producer.abortTransaction()

    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "3", "3", willBeCommitted = true))
    producer.commitTransaction()

    consumer.subscribe(List(topic1, topic2).asJava)

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  private def serverProps() = {
    val serverProps = new Properties()
    serverProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
    serverProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)
    serverProps.put(KafkaConfig.OffsetsTopicReplicationFactorProp, 1.toString)
    serverProps.put(KafkaConfig.TransactionsTopicPartitionsProp, 1.toString)
    serverProps.put(KafkaConfig.TransactionsTopicReplicationFactorProp, 1.toString)
    serverProps.put(KafkaConfig.TransactionsTopicMinISRProp, 1.toString)
    serverProps.put(KafkaConfig.ControlledShutdownEnableProp, true.toString)
    serverProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, false.toString)
    serverProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
    serverProps.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    serverProps.put(KafkaConfig.TransactionsAbortTimedOutTransactionCleanupIntervalMsProp, "200")
    serverProps
  }

  private def createReadCommittedConsumer(group: String) = {
    val consumer = TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(servers),
      groupId = group,
      enableAutoCommit = false,
      readCommitted = true)
    transactionalConsumers += consumer
    consumer
  }

  private def createTransactionalProducer(transactionalId: String): KafkaProducer[Array[Byte], Array[Byte]] = {
    val producer = TestUtils.createTransactionalProducer(transactionalId, servers, maxInFlight = 1)
    transactionalProducers += producer
    producer
  }
}
