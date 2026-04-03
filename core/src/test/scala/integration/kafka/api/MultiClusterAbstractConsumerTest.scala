/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.time.Duration
import java.util
import java.util.Properties

import kafka.server.{KafkaConfig, MultiClusterBaseRequestTest}
import kafka.utils.{ShutdownableThread, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.BeforeEach

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Buffer}

/**
 * Extension point for consumer integration tests.
 */
abstract class MultiClusterAbstractConsumerTest extends MultiClusterBaseRequestTest {

  val epsilon = 0.1
  override def brokerCountPerCluster: Int = 3

  val topicBaseName = "TestTopic"
  val topicNameCluster0 = "Cluster0" + topicBaseName
  val topicNameCluster1 = "Cluster1" + topicBaseName
  val tp1c0 = new TopicPartition(topicNameCluster0, 0)
  val tp2c0 = new TopicPartition(topicNameCluster0, 1)  // not actually used currently...
  val tp1c1 = new TopicPartition(topicNameCluster1, 0)
  // limit the test topics to no more than two brokers per partition as replicas
  val replicaCount = if (brokerCountPerCluster > 2) 2 else brokerCountPerCluster

  val producerClientId = "MultiClusterTestProducerID"  // all three of these are fine to be the same across physical
  val consumerClientId = "MultiClusterTestConsumerID"  // clusters even in the federated case since physical clusters
  val groupId = "MultiClusterTestConsumerGroupID"      // are invisible to clients; federated looks monolithic
  val groupMaxSessionTimeoutMs = 30000L


  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
    properties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
    properties.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "100") // set small enough session timeout
    properties.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, groupMaxSessionTimeoutMs.toString)
    properties.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "10")
  }

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    (0 until numClusters).foreach { i =>
      this.producerConfigs(i).setProperty(ProducerConfig.ACKS_CONFIG, "all")
      this.producerConfigs(i).setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)

      this.consumerConfigs(i).setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
      this.consumerConfigs(i).setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      this.consumerConfigs(i).setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      this.consumerConfigs(i).setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      this.consumerConfigs(i).setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")
      this.consumerConfigs(i).setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "6000")
    }

    // create the test topic(s)
    createTopic(topicNameCluster0, numPartitions = 2, replicaCount, clusterIndex = 0)
    if (numClusters > 1) {
      createTopic(topicNameCluster1, 1, replicaCount, clusterIndex = 1)  // single-partition topic in 2nd cluster for simplicity
    }
  }

  protected class TestConsumerReassignmentListener extends ConsumerRebalanceListener {
    var callsToAssigned = 0
    var callsToRevoked = 0

    def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
      info("onPartitionsAssigned called.")
      callsToAssigned += 1
    }

    def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {
      info("onPartitionsRevoked called.")
      callsToRevoked += 1
    }
  }

  protected def createConsumerWithGroupId(groupId: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val groupOverrideConfig = new Properties
    groupOverrideConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    createConsumer(configOverrides = groupOverrideConfig)
  }

  protected def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                            numRecords: Int,
                            tp: TopicPartition):
                            Seq[ProducerRecord[Array[Byte], Array[Byte]]] = {
    val records = (0 until numRecords).map { i =>
      // NOTE:  The use of i as a "timestamp" is strongly tied to the default TimestampType.CREATE_TIME in
      // consumeAndVerifyRecords() below, but it also means test cases are implicitly limited to no more
      // than 30 seconds between produce and consume (30 sec = default periodicity of checking for beyond-
      // retention log segments, and single-digit "times" are from January 1970 => WAY past any reasonable
      // expiry), OR they must explicitly set log.retention.ms to something extremely large.  An alternative
      // would be to add a boolean to this method to allow the caller to select record-index-as-time vs.
      // real time (Time.SYSTEM.milliseconds), but since 30-second+ test cases are highly undesirable in the
      // first place, we simply point out the issue here.
      val record = new ProducerRecord(tp.topic(), tp.partition(), i.toLong, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
      record
    }
    producer.flush()

    records
  }

  protected def consumeAndVerifyRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                                        numRecords: Int,
                                        startingOffset: Int,
                                        startingKeyAndValueIndex: Int = 0,
                                        startingTimestamp: Long = 0L,
                                        timestampType: TimestampType = TimestampType.CREATE_TIME,
                                        tp: TopicPartition = tp1c0,
                                        maxPollRecords: Int = Int.MaxValue): Unit = {
    val records = consumeRecords(consumer, numRecords, maxPollRecords = maxPollRecords)
    val now = System.currentTimeMillis()
    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(tp.topic, record.topic)
      assertEquals(tp.partition, record.partition)
      if (timestampType == TimestampType.CREATE_TIME) {
        assertEquals(timestampType, record.timestampType)
        val timestamp = startingTimestamp + i
        assertEquals(timestamp.toLong, record.timestamp)
      } else
        assertTrue(record.timestamp >= startingTimestamp && record.timestamp <= now,
          s"Got unexpected timestamp ${record.timestamp}. Timestamp should be between [$startingTimestamp, $now}]")
      assertEquals(offset.toLong, record.offset)
      val keyAndValueIndex = startingKeyAndValueIndex + i
      assertEquals(s"key $keyAndValueIndex", new String(record.key))
      assertEquals(s"value $keyAndValueIndex", new String(record.value))
      // this is true only because K and V are byte arrays
      assertEquals(s"key $keyAndValueIndex".length, record.serializedKeySize)
      assertEquals(s"value $keyAndValueIndex".length, record.serializedValueSize)
    }
  }

  protected def consumeRecords[K, V](consumer: Consumer[K, V],
                                     numRecords: Int,
                                     maxPollRecords: Int = Int.MaxValue): ArrayBuffer[ConsumerRecord[K, V]] = {
    val records = new ArrayBuffer[ConsumerRecord[K, V]]
    def pollAction(polledRecords: ConsumerRecords[K, V]): Boolean = {
      assertTrue(polledRecords.asScala.size <= maxPollRecords)
      records ++= polledRecords.asScala
      records.size >= numRecords
    }
    TestUtils.pollRecordsUntilTrue(consumer, pollAction, waitTimeMs = 60000,
      msg = s"Timed out before consuming expected $numRecords records. " +
        s"The number consumed was ${records.size}.")
    records
  }

  protected def sendAndAwaitAsyncCommit[K, V](consumer: Consumer[K, V],
                                              offsetsOpt: Option[Map[TopicPartition, OffsetAndMetadata]] = None): Unit = {

    def sendAsyncCommit(callback: OffsetCommitCallback) = {
      offsetsOpt match {
        case Some(offsets) => consumer.commitAsync(offsets.asJava, callback)
        case None => consumer.commitAsync(callback)
      }
    }

    class RetryCommitCallback extends OffsetCommitCallback {
      var isComplete = false
      var error: Option[Exception] = None

      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        exception match {
          case e: RetriableCommitFailedException =>
            sendAsyncCommit(this)
          case e =>
            isComplete = true
            error = Option(e)
        }
      }
    }

    val commitCallback = new RetryCommitCallback

    sendAsyncCommit(commitCallback)
    TestUtils.pollUntilTrue(consumer, () => commitCallback.isComplete,
      "Failed to observe commit callback before timeout", waitTimeMs = 10000)

    assertEquals(None, commitCallback.error)
  }

  /**
    * Create 'numConsumersToAdd' consumers add then to the consumer group 'consumerGroup', and create corresponding
    * pollers for these consumers. Wait for partition re-assignment and validate.
    *
    * Currently, assignment validation requires that total number of partitions is greater or equal to
    * number of consumers, so subscriptions.size must be greater or equal the resulting number of consumers in the group
    *
    * @param numConsumersToAdd number of consumers to create and add to the consumer group
    * @param consumerGroup current consumer group
    * @param consumerPollers current consumer pollers
    * @param topicsToSubscribe topics to which new consumers will subscribe to
    * @param subscriptions set of all topic partitions
    */
  def addConsumersToGroupAndWaitForGroupAssignment(numConsumersToAdd: Int,
                                                   consumerGroup: mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]],
                                                   consumerPollers: mutable.Buffer[ConsumerAssignmentPoller],
                                                   topicsToSubscribe: List[String],
                                                   subscriptions: Set[TopicPartition],
                                                   group: String = groupId): (mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]], mutable.Buffer[ConsumerAssignmentPoller]) = {
    assertTrue(consumerGroup.size + numConsumersToAdd <= subscriptions.size)
    addConsumersToGroup(numConsumersToAdd, consumerGroup, consumerPollers, topicsToSubscribe, subscriptions, group)
    // wait until topics get re-assigned and validate assignment
    validateGroupAssignment(consumerPollers, subscriptions)

    (consumerGroup, consumerPollers)
  }

  /**
    * Create 'numConsumersToAdd' consumers add then to the consumer group 'consumerGroup', and create corresponding
    * pollers for these consumers.
    *
    *
    * @param numConsumersToAdd number of consumers to create and add to the consumer group
    * @param consumerGroup current consumer group
    * @param consumerPollers current consumer pollers
    * @param topicsToSubscribe topics to which new consumers will subscribe to
    * @param subscriptions set of all topic partitions
    */
  def addConsumersToGroup(numConsumersToAdd: Int,
                          consumerGroup: mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]],
                          consumerPollers: mutable.Buffer[ConsumerAssignmentPoller],
                          topicsToSubscribe: List[String],
                          subscriptions: Set[TopicPartition],
                          group: String = groupId): (mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]], mutable.Buffer[ConsumerAssignmentPoller]) = {
    for (_ <- 0 until numConsumersToAdd) {
      val consumer = createConsumerWithGroupId(group)
      consumerGroup += consumer
      consumerPollers += subscribeConsumerAndStartPolling(consumer, topicsToSubscribe)
    }

    (consumerGroup, consumerPollers)
  }

  /**
    * Wait for consumers to get partition assignment and validate it.
    *
    * @param consumerPollers consumer pollers corresponding to the consumer group we are testing
    * @param subscriptions set of all topic partitions
    * @param msg message to print when waiting for/validating assignment fails
    */
  def validateGroupAssignment(consumerPollers: mutable.Buffer[ConsumerAssignmentPoller],
                              subscriptions: Set[TopicPartition],
                              msg: Option[String] = None,
                              waitTime: Long = 10000L): Unit = {
    val assignments = mutable.Buffer[Set[TopicPartition]]()
    TestUtils.waitUntilTrue(() => {
      assignments.clear()
      consumerPollers.foreach(assignments += _.consumerAssignment())
      isPartitionAssignmentValid(assignments, subscriptions)
    }, msg.getOrElse(s"Did not get valid assignment for partitions $subscriptions. Instead, got $assignments"), waitTime)
  }

  /**
    * Subscribes consumer 'consumer' to a given list of topics 'topicsToSubscribe', creates
    * consumer poller and starts polling.
    * Assumes that the consumer is not subscribed to any topics yet
    *
    * @param consumer consumer
    * @param topicsToSubscribe topics that this consumer will subscribe to
    * @return consumer poller for the given consumer
    */
  def subscribeConsumerAndStartPolling(consumer: Consumer[Array[Byte], Array[Byte]],
                                       topicsToSubscribe: List[String],
                                       partitionsToAssign: Set[TopicPartition] = Set.empty[TopicPartition]): ConsumerAssignmentPoller = {
    assertEquals(0, consumer.assignment().size)
    val consumerPoller = if (topicsToSubscribe.nonEmpty)
      new ConsumerAssignmentPoller(consumer, topicsToSubscribe)
    else
      new ConsumerAssignmentPoller(consumer, partitionsToAssign)

    consumerPoller.start()
    consumerPoller
  }

  protected def awaitRebalance(consumer: Consumer[_, _], rebalanceListener: TestConsumerReassignmentListener): Unit = {
    val numReassignments = rebalanceListener.callsToAssigned
    TestUtils.pollUntilTrue(consumer, () => rebalanceListener.callsToAssigned > numReassignments,
      "Timed out before expected rebalance completed")
  }

  protected def ensureNoRebalance(consumer: Consumer[_, _], rebalanceListener: TestConsumerReassignmentListener): Unit = {
    // The best way to verify that the current membership is still active is to commit offsets.
    // This would fail if the group had rebalanced.
    val initialRevokeCalls = rebalanceListener.callsToRevoked
    sendAndAwaitAsyncCommit(consumer)
    assertEquals(initialRevokeCalls, rebalanceListener.callsToRevoked)
  }

  protected class CountConsumerCommitCallback extends OffsetCommitCallback {
    var successCount = 0
    var failCount = 0
    var lastError: Option[Exception] = None

    override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      if (exception == null) {
        successCount += 1
      } else {
        failCount += 1
        lastError = Some(exception)
      }
    }
  }

  protected class ConsumerAssignmentPoller(consumer: Consumer[Array[Byte], Array[Byte]],
                                           topicsToSubscribe: List[String],
                                           partitionsToAssign: Set[TopicPartition])
    extends ShutdownableThread("daemon-consumer-assignment", false) {

    def this(consumer: Consumer[Array[Byte], Array[Byte]], topicsToSubscribe: List[String]) {
      this(consumer, topicsToSubscribe, Set.empty[TopicPartition])
    }

    def this(consumer: Consumer[Array[Byte], Array[Byte]], partitionsToAssign: Set[TopicPartition]) {
      this(consumer, List.empty[String], partitionsToAssign)
    }

    @volatile var thrownException: Option[Throwable] = None
    @volatile var receivedMessages = 0

    private val partitionAssignment = mutable.Set[TopicPartition]()
    @volatile private var subscriptionChanged = false
    private var topicsSubscription = topicsToSubscribe

    val rebalanceListener: ConsumerRebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {
        partitionAssignment ++= partitions.toArray(new Array[TopicPartition](0))
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {
        partitionAssignment --= partitions.toArray(new Array[TopicPartition](0))
      }
    }

    if (partitionsToAssign.isEmpty) {
      consumer.subscribe(topicsToSubscribe.asJava, rebalanceListener)
    } else {
      consumer.assign(partitionsToAssign.asJava)
    }

    def consumerAssignment(): Set[TopicPartition] = {
      partitionAssignment.toSet
    }

    /**
     * Subscribe consumer to a new set of topics.
     * Since this method most likely be called from a different thread, this function
     * just "schedules" the subscription change, and actual call to consumer.subscribe is done
     * in the doWork() method
     *
     * This method does not allow to change subscription until doWork processes the previous call
     * to this method. This is just to avoid race conditions and enough functionality for testing purposes
     * @param newTopicsToSubscribe
     */
    def subscribe(newTopicsToSubscribe: List[String]): Unit = {
      if (subscriptionChanged)
        throw new IllegalStateException("Do not call subscribe until the previous subscribe request is processed.")
      if (partitionsToAssign.nonEmpty)
        throw new IllegalStateException("Cannot call subscribe when configured to use manual partition assignment")

      topicsSubscription = newTopicsToSubscribe
      subscriptionChanged = true
    }

    def isSubscribeRequestProcessed: Boolean = {
      !subscriptionChanged
    }

    override def initiateShutdown(): Boolean = {
      val res = super.initiateShutdown()
      consumer.wakeup()
      res
    }

    override def doWork(): Unit = {
      if (subscriptionChanged) {
        consumer.subscribe(topicsSubscription.asJava, rebalanceListener)
        subscriptionChanged = false
      }
      try {
        receivedMessages += consumer.poll(Duration.ofMillis(50)).count()
      } catch {
        case _: WakeupException => // ignore for shutdown
        case e: Throwable =>
          thrownException = Some(e)
          throw e
      }
    }
  }

  /**
   * Check whether partition assignment is valid
   * Assumes partition assignment is valid iff
   * 1. Every consumer got assigned at least one partition
   * 2. Each partition is assigned to only one consumer
   * 3. Every partition is assigned to one of the consumers
   *
   * @param assignments set of consumer assignments; one per each consumer
   * @param partitions set of partitions that consumers subscribed to
   * @return true if partition assignment is valid
   */
  def isPartitionAssignmentValid(assignments: Buffer[Set[TopicPartition]],
                                 partitions: Set[TopicPartition]): Boolean = {
    val allNonEmptyAssignments = assignments.forall(assignment => assignment.nonEmpty)
    if (!allNonEmptyAssignments) {
      // at least one consumer got empty assignment
      return false
    }

    // make sure that sum of all partitions to all consumers equals total number of partitions
    val totalPartitionsInAssignments = assignments.foldLeft(0)(_ + _.size)
    if (totalPartitionsInAssignments != partitions.size) {
      // either same partitions got assigned to more than one consumer or some
      // partitions were not assigned
      return false
    }

    // The above checks could miss the case where one or more partitions were assigned to more
    // than one consumer and the same number of partitions were missing from assignments.
    // Make sure that all unique assignments are the same as 'partitions'
    val uniqueAssignedPartitions = assignments.foldLeft(Set.empty[TopicPartition])(_ ++ _)
    uniqueAssignedPartitions == partitions
  }

}
