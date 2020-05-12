/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tiered.storage

import java.io.PrintStream
import java.util.Properties
import java.util.concurrent.{ExecutionException, TimeUnit}

import kafka.utils.{TestUtils, nonthreadsafe}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageCondition.expectEvent
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.EventType.{FETCH_SEGMENT, OFFLOAD_SEGMENT}
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.{assertEquals, assertFalse, fail}
import unit.kafka.utils.RecordsKeyValueMatcher.correspondTo

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._
import scala.collection.{Seq, mutable}

/**
  * Specifies a remote log segment expected to be found in a second-tier storage.
  *
  * @param sourceBrokerId The broker which offloaded (uploaded) the segment to the second-tier storage.
  * @param topicPartition The topic-partition which the remote log segment belongs to.
  * @param baseOffset The base offset of the remote log segment.
  * @param records The records *expected* in the remote log segment.
  */
final case class OffloadedSegmentSpec(val sourceBrokerId: Int,
                                      val topicPartition: TopicPartition,
                                      val baseOffset: Int,
                                      val records: Seq[ProducerRecord[String, String]]) {

  override def toString: String =
    s"Segment[$topicPartition offloaded-by-broker-id=$sourceBrokerId base-offset=$baseOffset " +
      s"record-count=${records.size}]"
}

/**
  * Specifies a topic-partition with attributes customized for the purpose of tiered-storage tests.
  *
  * @param topicName The name of the topic.
  * @param partitionCount The number of partitions for the topic.
  * @param replicationFactor The replication factor of the topic.
  * @param maxBatchCountPerSegment The maximal number of batch in segments of the topic.
  *                    This allows to obtain a fixed, pre-determined size for the segment, which ease
  *                    reasoning on the expected states of local and tiered storages.
  * @param properties Configuration of the topic customized for the purpose of tiered-storage tests.
  */
final case class TopicSpec(val topicName: String,
                           val partitionCount: Int,
                           val replicationFactor: Int,
                           val maxBatchCountPerSegment: Int,
                           val assignment: Option[Map[Int, Seq[Int]]],
                           val properties: Properties = new Properties) {

  override def toString: String =
    s"Topic[name=$topicName partition-count=$partitionCount replication-factor=$replicationFactor " +
    s"segment-size=$maxBatchCountPerSegment assignment=$assignment]"
}

/**
  * Specifies a fetch (download) event from a second-tier storage. This is used to ensure the
  * interactions between Kafka and the second-tier storage match expectations.
  *
  * @param sourceBrokerId The broker which fetched (a) remote log segment(s) from the second-tier storage.
  * @param topicPartition The topic-partition which segment(s) were fetched.
  * @param count The number of remote log segment(s) fetched.
  */
// TODO Add more details on the specifications to perform more robust tests.
final case class RemoteFetchSpec(val sourceBrokerId: Int,
                                 val topicPartition: TopicPartition,
                                 val count: Int)

/**
  * An action, or step, taken during a test.
  */
trait TieredStorageTestAction {

  final def execute(context: TieredStorageTestContext): Unit = {
    try {
      doExecute(context)
      context.succeed(this)

    } catch {
      case e: Throwable =>
        context.fail(this)
        throw e
    }
  }

  protected def doExecute(context: TieredStorageTestContext): Unit

  def describe(output: PrintStream): Unit

}

final class CreateTopicAction(val spec: TopicSpec) extends TieredStorageTestAction {

  override def doExecute(context: TieredStorageTestContext): Unit = {
    //
    // Ensure offset and time indexes are generated for every record.
    //
    spec.properties.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1.toString)

    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, since the topic is configured to generate index entries for every record with, for
    // a "small" number of records (i.e. such that the average record size times the number of records is
    // much less than the segment size), the number of records which hold in a segment is the multiple of 12
    // defined below.
    //
    if (spec.maxBatchCountPerSegment != -1) {
      spec.properties.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, (12 * spec.maxBatchCountPerSegment).toString)
    }

    //
    // To verify records physically absent from Kafka's storage can be consumed via the second tier storage, we
    // want to delete log segments as soon as possible. When tiered storage is active, an inactive log
    // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
    // should be offloaded before deletion, and their consumption is possible thereafter.
    //
    spec.properties.put(TopicConfig.RETENTION_BYTES_CONFIG, 1.toString)

    context.createTopic(spec)
  }

  override def describe(output: PrintStream): Unit = output.println(s"create-topic: $spec")
}

/**
  * Produce records and verify resulting states in the first and second-tier storage.
  *
  * @param offloadedSegmentSpecs The segments expected to be offloaded to the second-tier storage.
  * @param recordsToProduce The records to produce per topic-partition.
  */
final class ProduceAction(val topicPartition: TopicPartition,
                          val offloadedSegmentSpecs: Seq[OffloadedSegmentSpec],
                          val recordsToProduce: Seq[ProducerRecord[String, String]],
                          val batchSize: Int,
                          val expectedEarliestOffset: Long)
  extends TieredStorageTestAction {

  /**
    * How much time to wait for all remote log segments of a topic-partition to be offloaded
    * to the second-tier storage.
    */
  private val offloadWaitTimeoutSec = 20

  private implicit val serde: Serde[String] = Serdes.String()

  override def doExecute(context: TieredStorageTestContext): Unit = {
    val tieredStorages = context.getTieredStorages
    val localStorages = context.getLocalStorages

    val tieredStorageConditions = offloadedSegmentSpecs.map { spec =>
      expectEvent(tieredStorages.asJava, OFFLOAD_SEGMENT, spec.sourceBrokerId, spec.topicPartition, false)
    }

    //
    // Retrieve the offset of the next record which would be consumed from the topic-partition
    // before records are produced. This allows to consume only the newly produced records afterwards.
    //
    val startOffset = context.nextOffset(topicPartition)

    //
    // Records are produced here.
    //
    context.produce(recordsToProduce, batchSize)

    if (!tieredStorageConditions.isEmpty) {
        tieredStorageConditions.reduce(_ and _).waitUntilTrue(offloadWaitTimeoutSec, TimeUnit.SECONDS)
    }

    //
    // At this stage, records were produced and the expected remote log segments found in the second-tier storage.
    // Further steps are:
    //
    // 1) verify the local (first-tier) storages contain only the expected log segments - that is to say,
    //    in the special case of these integration tests, only the active segment.
    // 2) consume the records and verify they match the produced records.
    //
    val topicSpec = context.topicSpec(topicPartition.topic())
    val earliestOffset = if (expectedEarliestOffset != -1L) expectedEarliestOffset else {
      startOffset + recordsToProduce.size - (recordsToProduce.size % topicSpec.maxBatchCountPerSegment) - 1
    }

    localStorages
      //
      // Select brokers which are assigned a replica of the topic-partition
      //
      .filter(s => context.isAssignedReplica(topicPartition, s.brokerId))
      //
      // Filter out inactive brokers, which may still contain log segments we would expect
      // to be deleted based on the retention configuration.
      //
      .filter(s => context.isActive(s.brokerId))
      //
      // Wait until the brokers local storage have been cleared from the inactive log segments.
      //
      .foreach(_.waitForEarliestOffset(topicPartition, earliestOffset))

    val consumedRecords = context.consume(topicPartition, recordsToProduce.size, startOffset)
    assertThat(consumedRecords, correspondTo(recordsToProduce, topicPartition))

    //
    // Take a physical snapshot of the second-tier storage, and compare the records found with
    // those of the expected log segments.
    //
    val snapshot = context.takeTieredStorageSnapshot()

    snapshot.getFilesets(topicPartition).asScala
      //
      // Snapshot does not sort the filesets by base offset.
      //
      .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
      //
      // Don't include the records which were stored before our records were produced.
      //
      .drop(startOffset.toInt)
      // TODO: Add check on size
      .zip(offloadedSegmentSpecs)
      .foreach {
        pair => compareRecords(pair._1, pair._2, topicPartition)
      }
  }

  override def describe(output: PrintStream) = {
    output.println(s"produce-records: $topicPartition")
    recordsToProduce.foreach(record => output.println(s"    ${record}"))
    offloadedSegmentSpecs.foreach(spec => output.println(s"    $spec"))
  }

  private def compareRecords(fileset: RemoteLogSegmentFileset,
                             spec: OffloadedSegmentSpec,
                             topicPartition: TopicPartition): Unit = {

    // Records found in the local tiered storage.
    val discoveredRecords = fileset.getRecords.asScala

    // Records expected to be found, based on what was sent by the producer.
    val producerRecords = spec.records

    assertThat(discoveredRecords, correspondTo(producerRecords, topicPartition))
    assertEquals("Base offset of segment mismatch", spec.baseOffset, discoveredRecords.head.offset())
  }
}

/**
  * Consume records for the topic-partition and verify they match the formulated expectation.
  *
  * @param topicPartition The topic-partition which to consume records from.
  * @param fetchOffset The first offset to consume from.
  * @param expectedTotalCount The number of records expected to be consumed.
  * @param expectedFromSecondTierCount The number of records expected to be retrieved from the second-tier storage.
  * @param remoteFetchSpec Specifies the interactions required with the second-tier storage (if any)
  *                        to fulfill the consumer fetch request.
  */
final class ConsumeAction(val topicPartition: TopicPartition,
                          val fetchOffset: Long,
                          val expectedTotalCount: Int,
                          val expectedFromSecondTierCount: Int,
                          val remoteFetchSpec: RemoteFetchSpec) extends TieredStorageTestAction {

  private implicit val serde: Serde[String] = Serdes.String()

  override def doExecute(context: TieredStorageTestContext): Unit = {
    //
    // Retrieve the history (which stores the chronological sequence of interactions with the second-tier
    // storage) for the expected broker. Note that while the second-tier storage is unique, each broker
    // maintains a local instance of LocalTieredStorage, which is the server-side plug-in interface which
    // allows Kafka to interact with that storage. These instances record the interactions (or events)
    // between the broker which they belong to and the second-tier storage.
    //
    // The latest event at the time of invocation for the interaction of type "FETCH_SEGMENT" between the
    // given broker and the second-tier storage is retrieved. It can be empty if an interaction of this
    // type has yet to happen.
    //
    val history = context.getTieredStorageHistory(remoteFetchSpec.sourceBrokerId)
    val latestEventSoFar = history.latestEvent(FETCH_SEGMENT, topicPartition).asScala

    //
    // Records are consumed here.
    //
    val consumedRecords = context.consume(topicPartition, expectedTotalCount, fetchOffset)

    //
    // (A) Comparison of records consumed with records in the second-tier storage.
    //
    // Reads all records physically found in the second-tier storage ∂for the given topic-partition.
    // The resulting sequence is sorted by records offset, as there is no guarantee on ordering from
    // the LocalTieredStorageSnapshot.
    //
    val tieredStorageRecords = context.takeTieredStorageSnapshot()
        .getFilesets(topicPartition).asScala
        .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
        .flatMap(_.getRecords.asScala)

    //
    // Try to find a record from the second-tier storage which should be included in the
    // sequence of records consumed.
    //
    val firstExpectedRecordOpt = tieredStorageRecords.find(_.offset() >= fetchOffset)

    if (firstExpectedRecordOpt.isEmpty) {
      //
      // If no records could be found in the second-tier storage or their offset are less
      // than the consumer fetch offset, no record would be consumed from that storage.
      //
      if (expectedFromSecondTierCount > 0) {
        fail(s"Could not find any record with offset >= $fetchOffset from tier storage.")
      }
      return
    }

    val indexOfFetchOffsetInTieredStorage = tieredStorageRecords.indexOf(firstExpectedRecordOpt.get)
    val recordsCountFromFirstIndex = tieredStorageRecords.size - indexOfFetchOffsetInTieredStorage

    assertFalse(
      s"Not enough records found in tiered storage from offset $fetchOffset for $topicPartition. " +
      s"Expected: $expectedFromSecondTierCount, Was $recordsCountFromFirstIndex",
      expectedFromSecondTierCount > recordsCountFromFirstIndex)

    assertFalse(
      s"Too many records found in tiered storage from offset $fetchOffset for $topicPartition. " +
      s"Expected: $expectedFromSecondTierCount, Was $recordsCountFromFirstIndex",
      expectedFromSecondTierCount < recordsCountFromFirstIndex
    )

    val storedRecords = tieredStorageRecords.splitAt(indexOfFetchOffsetInTieredStorage)._2
    val readRecords = consumedRecords.take(expectedFromSecondTierCount)

    assertThat(storedRecords, correspondTo(readRecords, topicPartition))

    //
    // (B) Assessment of the interactions between the source broker and the second-tier storage.
    //     Events which occurred before the consumption of records are discarded.
    //
    val events = history.getEvents(FETCH_SEGMENT, topicPartition).asScala
    val eventsInScope = latestEventSoFar.map(e => events.filter(_.isAfter(e))).getOrElse(events)

    assertEquals(s"Number of fetch requests from broker ${remoteFetchSpec.sourceBrokerId} to the " +
      s"tier storage does not match the expected value for topic-partition ${remoteFetchSpec.topicPartition}",
      remoteFetchSpec.count, eventsInScope.size)
  }

  override def describe(output: PrintStream): Unit = {
    output.println(s"consume-action:")
    output.println(s"  topic-partition = $topicPartition")
    output.println(s"  fetch-offset = $fetchOffset")
    output.println(s"  expected-record-count = $expectedTotalCount")
    output.println(s"  expected-record-from-tiered-storage = $expectedFromSecondTierCount")
  }
}

final class BounceBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def doExecute(context: TieredStorageTestContext): Unit = context.bounce(brokerId)
  override def describe(output: PrintStream): Unit = output.println(s"bounce-broker: $brokerId")
}

final class StopBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def doExecute(context: TieredStorageTestContext): Unit = context.stop(brokerId)
  override def describe(output: PrintStream): Unit = output.println(s"stop-broker: $brokerId")
}

final class StartBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def doExecute(context: TieredStorageTestContext): Unit = context.start(brokerId)
  override def describe(output: PrintStream): Unit = output.println(s"start-broker: $brokerId")
}

final class EraseBrokerStorageAction(val brokerId: Int) extends TieredStorageTestAction {
  override def doExecute(context: TieredStorageTestContext): Unit = context.eraseBrokerStorage(brokerId)
  override def describe(output: PrintStream): Unit = output.println(s"erase-broker-storage: $brokerId")
}

final class ExpectLeaderAction(val topicPartition: TopicPartition, val replicaId: Int, val electLeader: Boolean)
  extends TieredStorageTestAction {

  override def doExecute(context: TieredStorageTestContext): Unit = {
    if (electLeader) {
      context.admin().electLeaders(ElectionType.PREFERRED, Set(topicPartition).asJava)
    }

    val topic = topicPartition.topic()
    val partition = topicPartition.partition()
    var actualLeader = -1

    TestUtils.waitUntilTrue(() => {
      try {
        val topicResult = context.admin().describeTopics(List(topic).asJava).all.get.get(topic)
        actualLeader = Option(topicResult.partitions.get(partition).leader()).map(_.id).getOrElse(-1)
        replicaId == actualLeader

      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false
      }
    }, s"Leader of $topicPartition was not $replicaId. Actual leader: $actualLeader")
  }

  override def describe(output: PrintStream): Unit = {
    output.println(s"expect-leader: topic-partition: $topicPartition replica-id: $replicaId")
  }
}

final class ExpectBrokerInISR(val topicPartition: TopicPartition, replicaId: Int) extends TieredStorageTestAction {
  override def doExecute(context: TieredStorageTestContext): Unit = {
    TestUtils.waitForBrokersInIsr(context.admin(), topicPartition, Set(replicaId))
  }

  override def describe(output: PrintStream): Unit = {
    output.println(s"expect-broker-in-isr: topic-partition $topicPartition broker-id: $replicaId")
  }
}

/**
  * This builder helps to formulate a test case exercising the tiered storage functionality and formulate
  * the expectations following the execution of the test.
  */
@nonthreadsafe
final class TieredStorageTestBuilder {
  private val defaultProducedBatchSize = 1
  private val defaultEarliestOffsetExpectedInLogDirectory = 0

  private var producables:
    mutable.Map[TopicPartition, (mutable.Buffer[ProducerRecord[String, String]], Int, Long)] = mutable.Map()

  private var offloadables:
    mutable.Map[TopicPartition, mutable.Buffer[(Int, Int, Seq[ProducerRecord[String, String]])]] = mutable.Map()

  private var consumables: mutable.Map[TopicPartition, (Long, Int, Int)] = mutable.Map()
  private var fetchables: mutable.Map[TopicPartition, (Int, Int)] = mutable.Map()

  private val actions = mutable.Buffer[TieredStorageTestAction]()

  def createTopic(topic: String,
                  partitionsCount: Int,
                  replicationFactor: Int,
                  maxBatchCountPerSegment: Int,
                  replicaAssignment: Map[Int, Seq[Int]] = Map()): this.type = {

    assert(maxBatchCountPerSegment >= 1, s"Segments size for topic ${topic} needs to be >= 1")
    assert(partitionsCount >= 1, s"Partition count for topic ${topic} needs to be >= 1")
    assert(replicationFactor >= 1, s"Replication factor for topic ${topic} needs to be >= 1")

    maybeCreateProduceAction()
    maybeCreateConsumeActions()

    val assignment = if (replicaAssignment.isEmpty) None else Some(replicaAssignment)
    actions += new CreateTopicAction(TopicSpec(topic, partitionsCount, replicationFactor, maxBatchCountPerSegment, assignment))
    this
  }

  def produce(topic: String, partition: Int, keyValues: (String, String)*): this.type = {
    assert(partition >= 0, "Partition must be >= 0")
    maybeCreateConsumeActions()

    val p = getOrCreateProducable(topic, partition)
    keyValues.foreach {
      case (key, value) =>
        p._1 += new ProducerRecord[String, String](topic, partition, key, value)
    }

    this
  }

  def withBatchSize(topic: String, partition: Int, batchSize: Int): this.type = {
    assert(batchSize >= 1, "The size of a batch of produced records must >= 1")

    val p = getOrCreateProducable(topic, partition)
    producables(new TopicPartition(topic, partition)) = (p._1, batchSize, p._3)

    this
  }

  def expectEarliestOffsetInLogDirectory(topic: String, partition: Int, earliestOffset: Long): this.type = {
    assert(earliestOffset >= 0, "Record offset must be >= 0")

    val p = getOrCreateProducable(topic, partition)
    producables(new TopicPartition(topic, partition)) = (p._1, p._2, earliestOffset)

    this
  }

  def expectSegmentToBeOffloaded(fromBroker: Int, topic: String, partition: Int,
                                 baseOffset: Int, keyValues: (String, String)*): this.type = {

    val topicPartition = new TopicPartition(topic, partition)
    val records = keyValues.map { case (key, value) => new ProducerRecord(topic, partition, key, value) }
    val attrsAndRecords = (fromBroker, baseOffset, records)

    offloadables.get(topicPartition) match {
      case Some(buffer) => buffer += attrsAndRecords
      case None => offloadables += topicPartition -> mutable.Buffer(attrsAndRecords)
    }

    this
  }

  def consume(topic: String,
              partition: Int,
              fetchOffset: Long,
              expectedTotalRecord: Int,
              expectedRecordsFromSecondTier: Int): this.type = {

    assert(partition >= 0, "Partition must be >= 0")
    assert(fetchOffset >= 0, "Fecth offset must be >=0")
    assert(expectedTotalRecord >= 1, "Must read at least one record")
    assert(expectedRecordsFromSecondTier >= 0, "Expected read cannot be < 0")
    assert(expectedRecordsFromSecondTier <= expectedTotalRecord, "Cannot fetch more records than consumed")

    maybeCreateProduceAction()
    val topicPartition = new TopicPartition(topic, partition)

    assert(!consumables.contains(topicPartition), s"Consume already in progress for $topicPartition")
    consumables += topicPartition -> (fetchOffset, expectedTotalRecord, expectedRecordsFromSecondTier)
    this
  }

  def expectLeader(topic: String, partition: Int, brokerId: Int, electLeader: Boolean = false): this.type = {
    actions += new ExpectLeaderAction(new TopicPartition(topic, partition), brokerId, electLeader)
    this
  }

  def expectInIsr(topic: String, partition: Int, brokerId: Int): this.type = {
    actions += new ExpectBrokerInISR(new TopicPartition(topic, partition), brokerId)
    this
  }

  def expectFetchFromTieredStorage(fromBroker: Int, topic: String, partition: Int, remoteFetchRequestCount: Int): this.type = {
    assert(partition >= 0, "Partition must be >= 0")
    assert(remoteFetchRequestCount >= 0, "Expected fetch count from tiered storage must be >= 0")

    val topicPartition = new TopicPartition(topic, partition)

    assert(!fetchables.contains(topicPartition), s"Consume already in progress for $topicPartition")
    fetchables += topicPartition -> (fromBroker, remoteFetchRequestCount)
    this
  }

  def bounce(brokerId: Int): this.type = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions += new BounceBrokerAction(brokerId)
    this
  }

  def stop(brokerId: Int): this.type = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions += new StopBrokerAction(brokerId)
    this
  }

  def start(brokerId: Int): this.type = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions += new StartBrokerAction(brokerId)
    this
  }

  def eraseBrokerStorage(brokerId: Int): this.type = {
    actions += new EraseBrokerStorageAction(brokerId)
    this
  }

  def complete(): Seq[TieredStorageTestAction] = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions
  }

  private def maybeCreateProduceAction(): Unit = {
    if (!producables.isEmpty) {
      producables.foreach {
        case (topicPartition, (records, batchSize, earliestOffsetInLogDirectory)) =>
          val recordsToProduce = Seq() ++ records

          val offloadedSegmentSpecs =
            offloadables.getOrElse(topicPartition, mutable.Buffer())
            .map {
              case (sourceBroker, baseOffset, records) =>
                new OffloadedSegmentSpec(sourceBroker, topicPartition, baseOffset, records)
            }

          actions += new ProduceAction(
            topicPartition, offloadedSegmentSpecs,recordsToProduce, batchSize, earliestOffsetInLogDirectory)
      }

      producables = mutable.Map()
      offloadables = mutable.Map()
    }
  }

  private def maybeCreateConsumeActions(): Unit = {
    if (!consumables.isEmpty) {
      consumables.foreach {
        case (topicPartition, spec) =>
          val (sourceBroker, fetchCount) = fetchables.getOrElse(topicPartition, (0, 0))
          val remoteFetchSpec = RemoteFetchSpec(sourceBroker, topicPartition, fetchCount)
          actions += new ConsumeAction(topicPartition, spec._1, spec._2, spec._3, remoteFetchSpec)
      }
      consumables = mutable.Map()
      fetchables = mutable.Map()
    }
  }

  private def getOrCreateProducable(topic: String, partition: Int):
    (mutable.Buffer[ProducerRecord[String, String]], Int, Long) = {

    val topicPartition = new TopicPartition(topic, partition)
    if (!producables.contains(topicPartition)) {
      producables +=
        (topicPartition -> (mutable.Buffer(), defaultProducedBatchSize, defaultEarliestOffsetExpectedInLogDirectory))
    }

    producables(topicPartition)
  }
}
