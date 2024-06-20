/*
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
package kafka.zk

import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metadata.{FeatureLevelRecord, TopicRecord}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.controller.QuorumFeatures
import org.apache.kafka.controller.metrics.QuorumControllerMetrics
import org.apache.kafka.image.loader.LogDeltaManifest
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.KafkaConfigSchema
import org.apache.kafka.metadata.migration._
import org.apache.kafka.raft.{LeaderAndEpoch, OffsetAndEpoch}
import org.apache.kafka.security.PasswordEncoder
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.fault.FaultHandler
import org.apache.zookeeper.client.ZKClientConfig
import org.junit.jupiter.api.Assertions.{assertTrue, fail}
import org.junit.jupiter.api.Test

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.{Optional, OptionalInt}
import scala.collection.mutable

class ZkMigrationFailoverTest extends Logging {

  class CapturingFaultHandler(nodeId: Int) extends FaultHandler {
    val faults = mutable.Buffer[Throwable]()
    var future: CompletableFuture[Throwable] = CompletableFuture.completedFuture(new RuntimeException())
    var waitingForMsg = ""

    override def handleFault(failureMessage: String, cause: Throwable): RuntimeException = {
      error(s"Fault handled on node $nodeId", cause)
      faults.append(cause)
      if (!future.isDone && cause.getMessage.contains(waitingForMsg)) {
        future.complete(cause)
      }
      new RuntimeException(cause)
    }

    def checkAndClear(verifier: Seq[Throwable] => Unit): Unit = {
      val faultsSoFar = faults.toSeq
      try {
        verifier.apply(faultsSoFar)
      } catch {
        case ae: AssertionError => fail(s"Assertion failed. Faults on $nodeId were: $faultsSoFar", ae)
      }
    }

    def waitForError(message: String): CompletableFuture[Throwable] = {
      future = new CompletableFuture[Throwable]()
      waitingForMsg = message
      future
    }
  }

  def buildMigrationDriver(nodeId: Int, zkMigrationClient: ZkMigrationClient): (KRaftMigrationDriver, CapturingFaultHandler) = {
    val faultHandler = new CapturingFaultHandler(nodeId)
    val driver = KRaftMigrationDriver.newBuilder
      .setNodeId(nodeId)
      .setZkRecordConsumer(new ZkRecordConsumer {
        override def beginMigration(): CompletableFuture[_] = ???

        override def acceptBatch(recordBatch: util.List[ApiMessageAndVersion]): CompletableFuture[_] = ???

        override def completeMigration(): CompletableFuture[OffsetAndEpoch] = ???

        override def abortMigration(): Unit = ???
      })
      .setInitialZkLoadHandler((_: MetadataPublisher) => {})
      .setZkMigrationClient(zkMigrationClient)
      .setFaultHandler(faultHandler)
      .setQuorumFeatures(new QuorumFeatures(nodeId, QuorumFeatures.defaultFeatureMap(true), util.Arrays.asList(3000, 3001, 3002)))
      .setConfigSchema(KafkaConfigSchema.EMPTY)
      .setControllerMetrics(new QuorumControllerMetrics(Optional.empty(), Time.SYSTEM, true))
      .setTime(Time.SYSTEM)
      .setPropagator(new LegacyPropagator() {
        override def startup(): Unit = ???

        override def shutdown(): Unit = ???

        override def publishMetadata(image: MetadataImage): Unit = ???

        override def sendRPCsToBrokersFromMetadataDelta(delta: MetadataDelta, image: MetadataImage, zkControllerEpoch: Int): Unit = {

        }

        override def sendRPCsToBrokersFromMetadataImage(image: MetadataImage, zkControllerEpoch: Int): Unit = {

        }

        override def clear(): Unit = ???
      })
      .build()
    (driver, faultHandler)
  }

  def readMigrationZNode(zkMigrationClient: ZkMigrationClient): ZkMigrationLeadershipState = {
    zkMigrationClient.getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState.EMPTY)
  }

  def safeGet[T](future: CompletableFuture[T]): T = {
    future.get(10, TimeUnit.SECONDS)
  }

  @Test
  def testControllerFailoverZkRace(): Unit = {
    val zookeeper = new EmbeddedZookeeper()
    var zkClient: KafkaZkClient = null
    val zkConnect = s"127.0.0.1:${zookeeper.port}"
    try {
      zkClient = KafkaZkClient(
        zkConnect,
        isSecure = false,
        30000,
        60000,
        1,
        Time.SYSTEM,
        name = "ZkMigrationFailoverTest",
        new ZKClientConfig)
    } catch {
      case t: Throwable =>
        Utils.closeQuietly(zookeeper, "EmbeddedZookeeper")
        zookeeper.shutdown()
        if (zkClient != null) Utils.closeQuietly(zkClient, "KafkaZkClient")
        throw t
    }

    // Safe to reuse these since they don't keep any state
    val zkMigrationClient = ZkMigrationClient(zkClient, PasswordEncoder.NOOP)

    val (driver1, faultHandler1) = buildMigrationDriver(3000, zkMigrationClient)
    val (driver2, faultHandler2) = buildMigrationDriver(3001, zkMigrationClient)

    // Initialize data into /controller and /controller_epoch
    zkClient.registerControllerAndIncrementControllerEpoch(0)
    var zkState = zkMigrationClient.claimControllerLeadership(
      ZkMigrationLeadershipState.EMPTY.withNewKRaftController(3000, 1)
    )

    // Fake a complete migration
    zkState = zkState.withKRaftMetadataOffsetAndEpoch(100, 10)
    zkState = zkMigrationClient.getOrCreateMigrationRecoveryState(zkState)

    try {
      driver1.start()
      driver2.start()

      val newLeader1 = new LeaderAndEpoch(OptionalInt.of(3000), 2)
      var image1 = MetadataImage.EMPTY
      val delta1 = new MetadataDelta(image1)
      delta1.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.IBP_3_6_IV1.featureLevel))
      delta1.replay(ZkMigrationState.MIGRATION.toRecord.message)
      delta1.replay(new TopicRecord().setTopicId(Uuid.randomUuid()).setName("topic-to-sync"))

      val provenance1 = new MetadataProvenance(210, 11, 1)
      image1 = delta1.apply(provenance1)

      val manifest1 = LogDeltaManifest.newBuilder()
        .provenance(provenance1)
        .leaderAndEpoch(newLeader1)
        .numBatches(1)
        .elapsedNs(100)
        .numBytes(42)
        .build()

      // Load an image into 3000 image and a leader event, this lets it become active and sync to ZK
      driver1.onMetadataUpdate(delta1, image1, manifest1)
      driver1.onControllerChange(newLeader1)

      // Hold off on loading image to to 3001. This lets us artificially defer it from claiming leadership in ZK
      driver2.onControllerChange(newLeader1)

      // Wait for driver 1 to become leader in ZK
      TestUtils.waitUntilTrue(() => zkClient.getControllerId match {
        case Some(nodeId) => nodeId == 3000
        case None => false
      }, "waiting for 3000 to claim ZK leadership")

      // Now 3001 becomes leader, and loads migration recovery state from ZK.
      // Since an image hasn't been published to it yet, it will stay in WAIT_FOR_CONTROLLER_QUORUM
      val newLeader2 = new LeaderAndEpoch(OptionalInt.of(3001), 3)
      driver2.onControllerChange(newLeader2)
      TestUtils.waitUntilTrue(
        () => safeGet(driver2.migrationState()).equals(MigrationDriverState.WAIT_FOR_CONTROLLER_QUORUM),
        "waiting for node 3001 to enter WAIT_FOR_CONTROLLER_QUORUM")

      // Node 3000 still thinks that its the leader, do a delta update
      val delta2 = new MetadataDelta(image1)
      delta2.replay(new TopicRecord().setTopicId(Uuid.randomUuid()).setName("another-topic-to-sync"))
      val provenance2 = new MetadataProvenance(211, 11, 1)
      val image2 = delta2.apply(provenance2)
      val manifest2 = LogDeltaManifest.newBuilder()
        .provenance(provenance2)
        .leaderAndEpoch(newLeader1)
        .numBatches(1)
        .elapsedNs(100)
        .numBytes(42)
        .build()
      val migrationZkVersion = readMigrationZNode(zkMigrationClient).migrationZkVersion()
      driver1.onMetadataUpdate(delta2, image2, manifest2)

      // Wait for /migration znode update from 3000 SYNC_KRAFT_TO_ZK
      TestUtils.waitUntilTrue(() => readMigrationZNode(zkMigrationClient).migrationZkVersion() > migrationZkVersion,
        "waiting for /migration znode to change")

      // Now unblock 3001 from claiming ZK. This will let it move to BECOME_CONTROLLER
      val delta3 = new MetadataDelta(image1)
      delta3.replay(new TopicRecord().setTopicId(Uuid.randomUuid()).setName("another-topic-to-sync"))
      val provenance3 = new MetadataProvenance(211, 11, 1)
      val image3 = delta3.apply(provenance3)
      val manifest3 = LogDeltaManifest.newBuilder()
        .provenance(provenance3)
        .leaderAndEpoch(newLeader2)
        .numBatches(1)
        .elapsedNs(100)
        .numBytes(42)
        .build()
      driver2.onMetadataUpdate(delta3, image3, manifest3)

      // Now wait for 3001 to become leader in ZK
      TestUtils.waitUntilTrue(() => zkClient.getControllerId match {
        case Some(nodeId) => nodeId == 3001
        case None => false
      }, "waiting for 3001 to claim ZK leadership")

      // Now, 3001 will reload the /migration state and should not see any errors
      faultHandler2.checkAndClear(faults => assertTrue(faults.isEmpty))

      // 3000 should not be able to make any more ZK updates now
      driver1.onMetadataUpdate(delta3, image3, manifest3)
      safeGet(faultHandler1.waitForError("Controller epoch zkVersion check fails"))

      // 3000 finally processes new leader event
      driver1.onControllerChange(newLeader2)

      // 3001 should still not have any errors
      faultHandler2.checkAndClear(faults => assertTrue(faults.isEmpty))

      // Wait until new leader has sync'd to ZK
      TestUtils.waitUntilTrue(
        () => safeGet(driver2.migrationState()).equals(MigrationDriverState.DUAL_WRITE),
        "waiting for driver to enter DUAL_WRITE"
      )

      // Ensure we still dont have errors on the new leader
      faultHandler2.checkAndClear(faults => assertTrue(faults.isEmpty))

    } finally {
      driver1.close()
      driver2.close()
      Utils.closeQuietly(zookeeper, "EmbeddedZookeeper")
      zookeeper.shutdown()
      if (zkClient != null) Utils.closeQuietly(zkClient, "KafkaZkClient")
    }
  }
}
