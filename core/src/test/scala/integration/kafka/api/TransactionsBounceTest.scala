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
import kafka.utils.{ShutdownableThread, TestUtils}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable


class TransactionsBounceTest extends KafkaServerTestHarness {
  private val producerBufferSize =  65536
  private val serverMessageMaxBytes =  producerBufferSize/2
  private val numPartitions = 3

  val numServers = 4
  private val outputTopic = "output-topic"
  private val inputTopic = "input-topic"

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
  overridingProps.put(KafkaConfig.MessageMaxBytesProp, serverMessageMaxBytes.toString)
  // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
  // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
  overridingProps.put(KafkaConfig.ControlledShutdownEnableProp, true.toString)
  overridingProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, false.toString)
  overridingProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)
  overridingProps.put(KafkaConfig.OffsetsTopicReplicationFactorProp, 3.toString)
  overridingProps.put(KafkaConfig.MinInSyncReplicasProp, 2.toString)
  overridingProps.put(KafkaConfig.TransactionsTopicPartitionsProp, 1.toString)
  overridingProps.put(KafkaConfig.TransactionsTopicReplicationFactorProp, 3.toString)
  overridingProps.put(KafkaConfig.GroupMinSessionTimeoutMsProp, "10") // set small enough session timeout
  overridingProps.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")


  // This is the one of the few tests we currently allow to preallocate ports, despite the fact that this can result in transient
  // failures due to ports getting reused. We can't use random ports because of bad behavior that can result from bouncing
  // brokers too quickly when they get new, random ports. If we're not careful, the client can end up in a situation
  // where metadata is not refreshed quickly enough, and by the time it's actually trying to, all the servers have
  // been bounced and have new addresses. None of the bootstrap nodes or current metadata can get them connected to a
  // running server.
  //
  // Since such quick rotation of servers is incredibly unrealistic, we allow this one test to preallocate ports, leaving
  // a small risk of hitting errors due to port conflicts. Hopefully this is infrequent enough to not cause problems.
  override def generateConfigs = {
    FixedPortTestUtils.createBrokerConfigs(numServers, zkConnect,enableControlledShutdown = true)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  @Test
  def testBrokerFailure() {
    // basic idea is to seed a topic with 10000 records, and copy it transactionally while bouncing brokers
    // constantly through the period.
    val consumerGroup = "myGroup"
    val numInputRecords = 10000
    createTopics()

    TestUtils.seedTopicWithNumberedRecords(inputTopic, numInputRecords, servers)
    val consumer = createConsumerAndSubscribeToTopics(consumerGroup, List(inputTopic))
    val producer = TestUtils.createTransactionalProducer("test-txn", servers, 512)

    producer.initTransactions()

    val scheduler = new BounceScheduler
    scheduler.start()

    var numMessagesProcessed = 0
    var iteration = 0
    try {
      while (numMessagesProcessed < numInputRecords) {
        val toRead = Math.min(200, numInputRecords - numMessagesProcessed)
        trace(s"$iteration: About to read $toRead messages, processed $numMessagesProcessed so far..")
        val records = TestUtils.pollUntilAtLeastNumRecords(consumer, toRead)
        trace(s"Received ${records.size} messages, sending them transactionally to $outputTopic")

        producer.beginTransaction()
        val shouldAbort = iteration % 3 == 0
        records.foreach { record =>
          producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(outputTopic, record.key, record.value,
            !shouldAbort), new ErrorLoggingCallback(outputTopic, record.key, record.value, true))
        }
        trace(s"Sent ${records.size} messages. Committing offsets.")
        producer.sendOffsetsToTransaction(TestUtils.consumerPositions(consumer).asJava, consumerGroup)

        if (shouldAbort) {
          trace(s"Committed offsets. Aborting transaction of ${records.size} messages.")
          producer.abortTransaction()
          TestUtils.resetToCommittedPositions(consumer)
        } else {
          trace(s"Committed offsets. committing transaction of ${records.size} messages.")
          producer.commitTransaction()
          numMessagesProcessed += records.size
        }
        iteration += 1
      }
    } finally {
      producer.close()
      consumer.close()
    }

    scheduler.shutdown()

    val verifyingConsumer = createConsumerAndSubscribeToTopics("randomGroup", List(outputTopic), readCommitted = true)
    val recordsByPartition = new mutable.HashMap[TopicPartition, mutable.ListBuffer[Int]]()
    TestUtils.pollUntilAtLeastNumRecords(verifyingConsumer, numInputRecords).foreach { record =>
      val value = TestUtils.assertCommittedAndGetValue(record).toInt
      val topicPartition = new TopicPartition(record.topic(), record.partition())
      recordsByPartition.getOrElseUpdate(topicPartition, new mutable.ListBuffer[Int])
        .append(value)
    }

    val outputRecords = new mutable.ListBuffer[Int]()
    recordsByPartition.values.foreach { case (partitionValues) =>
      assertEquals("Out of order messages detected", partitionValues, partitionValues.sorted)
      outputRecords.appendAll(partitionValues)
    }

    val recordSet = outputRecords.toSet
    assertEquals(numInputRecords, recordSet.size)

    val expectedValues = (0 until numInputRecords).toSet
    assertEquals(s"Missing messages: ${expectedValues -- recordSet}", expectedValues, recordSet)

    verifyingConsumer.close()
  }

  private def createConsumerAndSubscribeToTopics(groupId: String, topics: List[String], readCommitted: Boolean = false) = {
    val props = new Properties()
    if (readCommitted)
      props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = groupId,
      securityProtocol = SecurityProtocol.PLAINTEXT, props = Some(props))
    consumer.subscribe(topics.asJava)
    consumer
  }

  private def createTopics() =  {
    val topicConfig = new Properties()
    topicConfig.put(KafkaConfig.MinInSyncReplicasProp, 2.toString)
    createTopic(inputTopic, numPartitions, 3, topicConfig)
    createTopic(outputTopic, numPartitions, 3, topicConfig)
  }

  private class BounceScheduler extends ShutdownableThread("daemon-broker-bouncer", false) {
    override def doWork(): Unit = {
      for (server <- servers) {
        trace("Shutting down server : %s".format(server.config.brokerId))
        server.shutdown()
        server.awaitShutdown()
        Thread.sleep(500)
        trace("Server %s shut down. Starting it up again.".format(server.config.brokerId))
        server.startup()
        trace("Restarted server: %s".format(server.config.brokerId))
        Thread.sleep(500)
      }

      (0 until numPartitions).foreach(partition => TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, outputTopic, partition))
    }

    override def shutdown(){
      super.shutdown()
   }
  }
}
