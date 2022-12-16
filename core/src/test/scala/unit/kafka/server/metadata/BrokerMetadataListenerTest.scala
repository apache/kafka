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

package kafka.server.metadata

import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.{Collections, Optional}
import org.apache.kafka.common.metadata.{FeatureLevelRecord, PartitionChangeRecord, PartitionRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Endpoint, Uuid}
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.metadata.util.SnapshotReason
import org.apache.kafka.metadata.{BrokerRegistration, RecordTestUtils, VersionRange}
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.fault.{FaultHandler, MockFaultHandler}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

import scala.jdk.CollectionConverters._

class BrokerMetadataListenerTest {
  private val metadataLoadingFaultHandler = new MockFaultHandler("metadata loading")

  @AfterEach
  def verifyNoFaults(): Unit = {
    metadataLoadingFaultHandler.maybeRethrowFirstException()
  }

  private def newBrokerMetadataListener(
    metrics: BrokerServerMetrics = BrokerServerMetrics(new Metrics()),
    snapshotter: Option[MetadataSnapshotter] = None,
    maxBytesBetweenSnapshots: Long = 1000000L,
    faultHandler: FaultHandler = metadataLoadingFaultHandler
  ): BrokerMetadataListener = {
    new BrokerMetadataListener(
      brokerId = 0,
      time = Time.SYSTEM,
      threadNamePrefix = None,
      maxBytesBetweenSnapshots = maxBytesBetweenSnapshots,
      snapshotter = snapshotter,
      brokerMetrics = metrics,
      _metadataLoadingFaultHandler = faultHandler
    )
  }

  @Test
  def testCreateAndClose(): Unit = {
    val listener = newBrokerMetadataListener()
    listener.close()
  }

  @Test
  def testPublish(): Unit = {
    val metrics = BrokerServerMetrics(new Metrics())
    val listener = newBrokerMetadataListener(metrics = metrics)
    try {
      val unfencedTimestamp = 300L
      listener.handleCommit(
        RecordTestUtils.mockBatchReader(
          100,
          unfencedTimestamp,
          util.Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
            setBrokerId(0).
            setBrokerEpoch(100L).
            setFenced(false).
            setRack(null).
            setIncarnationId(Uuid.fromString("GFBwlTcpQUuLYQ2ig05CSg")), 0.toShort))
        )
      )
      val imageRecords = listener.getImageRecords().get()
      assertEquals(0, imageRecords.size())
      assertEquals(100L, listener.highestMetadataOffset)
      assertEquals(-1L, metrics.lastAppliedOffset())
      assertEquals(-1L, metrics.lastAppliedTimestamp())
      assertEquals(0L, metrics.metadataLoadErrorCount.get)
      assertEquals(0L, metrics.metadataApplyErrorCount.get)

      val fencedTimestamp = 500L
      val fencedLastOffset = 200L
      listener.handleCommit(
        RecordTestUtils.mockBatchReader(
          fencedLastOffset,
          fencedTimestamp,
          util.Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
            setBrokerId(1).
            setBrokerEpoch(200L).
            setFenced(true).
            setRack(null).
            setIncarnationId(Uuid.fromString("QkOQtNKVTYatADcaJ28xDg")), 0.toShort))
        )
      )
      listener.startPublishing(new MetadataPublisher {
        override def publish(delta: MetadataDelta, newImage: MetadataImage): Unit = {
          assertEquals(200L, newImage.highestOffsetAndEpoch().offset)
          assertEquals(new BrokerRegistration(0, 100L,
            Uuid.fromString("GFBwlTcpQUuLYQ2ig05CSg"), Collections.emptyList[Endpoint](),
            Collections.emptyMap[String, VersionRange](), Optional.empty[String](), false, false),
            delta.clusterDelta().broker(0))
          assertEquals(new BrokerRegistration(1, 200L,
            Uuid.fromString("QkOQtNKVTYatADcaJ28xDg"), Collections.emptyList[Endpoint](),
            Collections.emptyMap[String, VersionRange](), Optional.empty[String](), true, false),
            delta.clusterDelta().broker(1))
        }

        override def publishedOffset: Long = -1
      }).get()

      assertEquals(fencedLastOffset, metrics.lastAppliedOffset())
      assertEquals(fencedTimestamp, metrics.lastAppliedTimestamp())
      assertEquals(0L, metrics.metadataLoadErrorCount.get)
      assertEquals(0L, metrics.metadataApplyErrorCount.get)
    } finally {
      listener.close()
    }
  }

  class MockMetadataSnapshotter extends MetadataSnapshotter {
    var image = MetadataImage.EMPTY
    val failure = new AtomicReference[Throwable](null)
    var activeSnapshotOffset = -1L
    var prevCommittedOffset = -1L
    var prevCommittedEpoch = -1
    var prevLastContainedLogTime = -1L

    override def maybeStartSnapshot(lastContainedLogTime: Long, newImage: MetadataImage, reason: Set[SnapshotReason]): Boolean = {
      try {
        if (activeSnapshotOffset == -1L) {
          assertTrue(prevCommittedOffset <= newImage.highestOffsetAndEpoch().offset)
          assertTrue(prevCommittedEpoch <= newImage.highestOffsetAndEpoch().epoch)
          assertTrue(prevLastContainedLogTime <= lastContainedLogTime)
          prevCommittedOffset = newImage.highestOffsetAndEpoch().offset
          prevCommittedEpoch = newImage.highestOffsetAndEpoch().epoch
          prevLastContainedLogTime = lastContainedLogTime
          image = newImage
          activeSnapshotOffset = newImage.highestOffsetAndEpoch().offset
          true
        } else {
          false
        }
      } catch {
        case t: Throwable => failure.compareAndSet(null, t)
      }
    }
  }

  class MockMetadataPublisher extends MetadataPublisher {
    var image = MetadataImage.EMPTY

    override def publish(delta: MetadataDelta, newImage: MetadataImage): Unit = {
      image = newImage
    }

    override def publishedOffset: Long = -1
  }

  private val FOO_ID = Uuid.fromString("jj1G9utnTuCegi_gpnRgYw")
  private val BAR_ID = Uuid.fromString("SzN5j0LvSEaRIJHrxfMAlg")

  private def generateManyRecords(listener: BrokerMetadataListener,
                                  endOffset: Long): Unit = {
    (0 to 10000).foreach { _ =>
      listener.handleCommit(
        RecordTestUtils.mockBatchReader(
          endOffset,
          0,
          util.Arrays.asList(
            new ApiMessageAndVersion(new PartitionChangeRecord().
              setPartitionId(0).
              setTopicId(FOO_ID).
              setRemovingReplicas(Collections.singletonList(1)), 0.toShort),
            new ApiMessageAndVersion(new PartitionChangeRecord().
              setPartitionId(0).
              setTopicId(FOO_ID).
              setRemovingReplicas(Collections.emptyList()), 0.toShort)
          )
        )
      )
    }
    listener.getImageRecords().get()
  }

  private def generateBadRecords(listener: BrokerMetadataListener,
                                endOffset: Long): Unit = {
    listener.handleCommit(
      RecordTestUtils.mockBatchReader(
        endOffset,
        0,
        util.Arrays.asList(
          new ApiMessageAndVersion(new PartitionChangeRecord().
            setPartitionId(0).
            setTopicId(BAR_ID).
            setRemovingReplicas(Collections.singletonList(1)), 0.toShort),
          new ApiMessageAndVersion(new PartitionChangeRecord().
            setPartitionId(0).
            setTopicId(BAR_ID).
            setRemovingReplicas(Collections.emptyList()), 0.toShort)
        )
      )
    )
    listener.getImageRecords().get()
  }

  @Test
  def testHandleCommitsWithNoSnapshotterDefined(): Unit = {
    val listener = newBrokerMetadataListener(maxBytesBetweenSnapshots = 1000L)
    try {
      val brokerIds = 0 to 3

      registerBrokers(listener, brokerIds, endOffset = 100L)
      createTopicWithOnePartition(listener, replicas = brokerIds, endOffset = 200L)
      listener.getImageRecords().get()
      assertEquals(200L, listener.highestMetadataOffset)

      generateManyRecords(listener, endOffset = 1000L)
      assertEquals(1000L, listener.highestMetadataOffset)
    } finally {
      listener.close()
    }
  }

  @Test
  def testCreateSnapshot(): Unit = {
    val snapshotter = new MockMetadataSnapshotter()
    val listener = newBrokerMetadataListener(snapshotter = Some(snapshotter),
      maxBytesBetweenSnapshots = 1000L)
    try {
      val brokerIds = 0 to 3

      registerBrokers(listener, brokerIds, endOffset = 100L)
      createTopicWithOnePartition(listener, replicas = brokerIds, endOffset = 200L)
      listener.getImageRecords().get()
      assertEquals(200L, listener.highestMetadataOffset)

      // Check that we generate at least one snapshot once we see enough records.
      assertEquals(-1L, snapshotter.prevCommittedOffset)
      generateManyRecords(listener, 1000L)
      assertEquals(1000L, snapshotter.prevCommittedOffset)
      assertEquals(1000L, snapshotter.activeSnapshotOffset)
      snapshotter.activeSnapshotOffset = -1L

      // Test creating a new snapshot after publishing it.
      val publisher = new MockMetadataPublisher()
      listener.startPublishing(publisher).get()
      generateManyRecords(listener, 2000L)
      listener.getImageRecords().get()
      assertEquals(2000L, snapshotter.activeSnapshotOffset)
      assertEquals(2000L, snapshotter.prevCommittedOffset)

      // Test how we handle the snapshotter returning false.
      generateManyRecords(listener, 3000L)
      assertEquals(2000L, snapshotter.activeSnapshotOffset)
      generateManyRecords(listener, 4000L)
      assertEquals(2000L, snapshotter.activeSnapshotOffset)
      snapshotter.activeSnapshotOffset = -1L
      generateManyRecords(listener, 5000L)
      assertEquals(5000L, snapshotter.activeSnapshotOffset)
      assertEquals(null, snapshotter.failure.get())
    } finally {
      listener.close()
    }
  }

  @Test
  def testNotSnapshotAfterMetadataVersionChangeBeforePublishing(): Unit = {
    val snapshotter = new MockMetadataSnapshotter()
    val listener = newBrokerMetadataListener(snapshotter = Some(snapshotter),
      maxBytesBetweenSnapshots = 1000L)

    updateFeature(listener, feature = MetadataVersion.FEATURE_NAME, MetadataVersion.latest.featureLevel(), 100L)
    listener.getImageRecords().get()
    assertEquals(-1L, snapshotter.activeSnapshotOffset, "We won't generate snapshot on metadata version change before starting publishing")
  }

  @Test
  def testSnapshotAfterMetadataVersionChangeWhenStarting(): Unit = {
    val snapshotter = new MockMetadataSnapshotter()
    val listener = newBrokerMetadataListener(snapshotter = Some(snapshotter),
      maxBytesBetweenSnapshots = 1000L)

    val endOffset = 100L
    updateFeature(listener, feature = MetadataVersion.FEATURE_NAME, MetadataVersion.latest.featureLevel(), endOffset)
    listener.startPublishing(new MockMetadataPublisher()).get()
    assertEquals(endOffset, snapshotter.activeSnapshotOffset, "We should try to generate snapshot when starting publishing")
  }

  @Test
  def testSnapshotAfterMetadataVersionChange(): Unit = {
    val snapshotter = new MockMetadataSnapshotter()
    val listener = newBrokerMetadataListener(snapshotter = Some(snapshotter),
      maxBytesBetweenSnapshots = 1000L)
    listener.startPublishing(new MockMetadataPublisher()).get()

    val endOffset = 100L
    updateFeature(listener, feature = MetadataVersion.FEATURE_NAME, (MetadataVersion.latest().featureLevel() - 1).toShort, endOffset)
    // Waiting for the metadata version update to get processed
    listener.getImageRecords().get()
    assertEquals(endOffset, snapshotter.activeSnapshotOffset, "We should generate snapshot on feature update")
  }

  @Test
  def testNoSnapshotAfterError(): Unit = {
    val snapshotter = new MockMetadataSnapshotter()
    val faultHandler = new MockFaultHandler("metadata loading")

    val listener = newBrokerMetadataListener(
      snapshotter = Some(snapshotter),
      maxBytesBetweenSnapshots = 1000L,
      faultHandler = faultHandler)
    try {
      val brokerIds = 0 to 3

      registerBrokers(listener, brokerIds, endOffset = 100L)
      createTopicWithOnePartition(listener, replicas = brokerIds, endOffset = 200L)
      listener.getImageRecords().get()
      assertEquals(200L, listener.highestMetadataOffset)
      assertEquals(-1L, snapshotter.prevCommittedOffset)
      assertEquals(-1L, snapshotter.activeSnapshotOffset)

      // Append invalid records that will normally trigger a snapshot
      generateBadRecords(listener, 1000L)
      assertEquals(-1L, snapshotter.prevCommittedOffset)
      assertEquals(-1L, snapshotter.activeSnapshotOffset)

      // Generate some records that will not throw an error, verify still no snapshots
      generateManyRecords(listener, 2000L)
      assertEquals(-1L, snapshotter.prevCommittedOffset)
      assertEquals(-1L, snapshotter.activeSnapshotOffset)
    } finally {
      listener.close()
    }
  }

  private def registerBrokers(
    listener: BrokerMetadataListener,
    brokerIds: Iterable[Int],
    endOffset: Long
  ): Unit = {
    brokerIds.foreach { brokerId =>
      listener.handleCommit(
        RecordTestUtils.mockBatchReader(
          endOffset,
          0,
          util.Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
            setBrokerId(brokerId).
            setBrokerEpoch(100L).
            setFenced(false).
            setRack(null).
            setIncarnationId(Uuid.fromString("GFBwlTcpQUuLYQ2ig05CS" + brokerId)), 0.toShort))
        )
      )
    }
  }

  private def createTopicWithOnePartition(
    listener: BrokerMetadataListener,
    replicas: Seq[Int],
    endOffset: Long
  ): Unit = {
    listener.handleCommit(
      RecordTestUtils.mockBatchReader(
        endOffset,
        0,
        util.Arrays.asList(
          new ApiMessageAndVersion(new TopicRecord().
            setName("foo").
            setTopicId(FOO_ID), 0.toShort),
          new ApiMessageAndVersion(new PartitionRecord().
            setPartitionId(0).
            setTopicId(FOO_ID).
            setIsr(replicas.map(Int.box).asJava).
            setLeader(0).
            setReplicas(replicas.map(Int.box).asJava), 0.toShort)
        )
      )
    )
  }

  private def updateFeature(
    listener: BrokerMetadataListener,
    feature: String,
    version: Short,
    endOffset: Long
  ): Unit = {
    listener.handleCommit(
      RecordTestUtils.mockBatchReader(
        endOffset,
        0,
        util.Arrays.asList(
          new ApiMessageAndVersion(new FeatureLevelRecord().
            setName(feature).
            setFeatureLevel(version), 0.toShort)
        )
      )
    )
  }

}
