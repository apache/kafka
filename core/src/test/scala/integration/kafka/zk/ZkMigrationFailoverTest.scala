package kafka.zk


import kafka.utils.{PasswordEncoder, TestUtils}
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
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.fault.FaultHandler
import org.apache.zookeeper.client.ZKClientConfig
import org.junit.jupiter.api.Test

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.{Optional, OptionalInt}
import scala.collection.mutable

class ZkMigrationFailoverTest {

  class CapturingFaultHandler(nodeId: Int) extends FaultHandler {
    val faults = mutable.Buffer[Throwable]()

    var waitingString = ""
    var future: CompletableFuture[Throwable] = CompletableFuture.completedFuture(new RuntimeException())

    override def handleFault(failureMessage: String, cause: Throwable): RuntimeException = {
      System.err.println(cause)
      faults.append(cause)
      if (!future.isDone && failureMessage.contains(waitingString)) {
        future.complete(cause)
      }
      new RuntimeException(cause)
    }

    def waitForError(message: String): CompletableFuture[Throwable] = {
      future = new CompletableFuture[Throwable]()
      waitingString = message
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
    val zkMigrationClient = ZkMigrationClient(zkClient, PasswordEncoder.noop())

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

      // Now 3001 becomes leader, and loads migration recovery state from ZK. 3000 does not learn about this yet
      val newLeader2 = new LeaderAndEpoch(OptionalInt.of(3001), 3)
      driver2.onControllerChange(newLeader2)

      // While 3000 still thinks that its the leader, do a delta update
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

      // Wait for /migration znode update from delta2
      TestUtils.waitUntilTrue(() => readMigrationZNode(zkMigrationClient).migrationZkVersion() > migrationZkVersion,
        "waiting for /migration znode to change")

      // Now unblock 3001 from claiming ZK. This will let it move to BECOME_CONTROLLER
      driver2.onMetadataUpdate(delta1, image1, manifest1)

      // Now wait for driver 2 to become leader in ZK
      TestUtils.waitUntilTrue(() => zkClient.getControllerId match {
        case Some(nodeId) => nodeId == 3001
        case None => false
      }, "waiting for 3001 to claim ZK leadership")

      // 3001 will try to SYNC_KRAFT_TO_ZK, but it will fail because of /migration check op
      faultHandler2.waitForError("Unhandled error in SyncKRaftMetadataEvent").get(10, TimeUnit.SECONDS)

      // 3000 finally processes new leader
      driver1.onControllerChange(newLeader2)

      // 3001 still has error, indefinitely
      faultHandler2.waitForError("Unhandled error in SyncKRaftMetadataEvent").get(10, TimeUnit.SECONDS)
    } finally {
      driver1.close()
      driver2.close()
    }
  }
}
