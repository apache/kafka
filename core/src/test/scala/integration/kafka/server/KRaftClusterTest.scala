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

package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.errors.{PolicyViolationException, UnsupportedVersionException}
import org.apache.kafka.common.metadata.{ConfigRecord, FeatureLevelRecord}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.controller.{QuorumController, QuorumControllerIntegrationTestUtils}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.KRaftConfigs
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test, Timeout}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Files, Path, Paths}
import java.util
import java.util.concurrent.{ExecutionException, TimeUnit}
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.util.Using

@Timeout(120)
@Tag("integration")
class KRaftClusterTest {
  @Test
  def testCreateClusterAndRestartControllerNode(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(3).build()).build()
    try {
      cluster.format()
      cluster.startup()
      val controller = cluster.controllers().values().iterator().asScala.filter(_.controller.isActive).next()
      val port = controller.socketServer.boundPort(ListenerName.normalised(controller.config.controllerListeners.head.listener))

      // shutdown active controller
      controller.shutdown()
      // Rewrite The `listeners` config to avoid controller socket server init using different port
      val config = controller.sharedServer.controllerConfig.props
      config.asInstanceOf[util.HashMap[String,String]].put(SocketServerConfigs.LISTENERS_CONFIG, s"CONTROLLER://localhost:$port")
      controller.sharedServer.controllerConfig.updateCurrentConfig(new KafkaConfig(config))

      // restart controller
      controller.startup()
      TestUtils.waitUntilTrue(() => cluster.controllers().values().iterator().asScala.exists(_.controller.isActive), "Timeout waiting for new controller election")
    } finally {
      cluster.close()
    }
  }

  private def waitForTopicListing(admin: Admin,
                                  expectedPresent: Seq[String],
                                  expectedAbsent: Seq[String]): Unit = {
    val topicsNotFound = new util.HashSet[String]
    var extraTopics: mutable.Set[String] = null
    expectedPresent.foreach(topicsNotFound.add)
    TestUtils.waitUntilTrue(() => {
      admin.listTopics().names().get().forEach(name => topicsNotFound.remove(name))
      extraTopics = admin.listTopics().names().get().asScala.filter(expectedAbsent.contains(_))
      topicsNotFound.isEmpty && extraTopics.isEmpty
    }, s"Failed to find topic(s): ${topicsNotFound.asScala} and NOT find topic(s): $extraTopics")
  }

  @Test
  def testSnapshotCount(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp("metadata.log.max.snapshot.interval.ms", "500")
      .setConfigProp("metadata.max.idle.interval.ms", "50") // Set this low to generate metadata
      .build()

    try {
      cluster.format()
      cluster.startup()
      def snapshotCounter(path: Path): Long = {
       path.toFile.listFiles((_: File, name: String) => {
          name.toLowerCase.endsWith("checkpoint")
        }).length
      }

      val metaLog = FileSystems.getDefault.getPath(cluster.controllers().get(3000).config.metadataLogDir, "__cluster_metadata-0")
      TestUtils.waitUntilTrue(() => { snapshotCounter(metaLog) > 0 }, "Failed to see at least one snapshot")
      Thread.sleep(500 * 10) // Sleep for 10 snapshot intervals
      val countAfterTenIntervals = snapshotCounter(metaLog)
      assertTrue(countAfterTenIntervals > 1, s"Expected to see at least one more snapshot, saw $countAfterTenIntervals")
      assertTrue(countAfterTenIntervals < 20, s"Did not expect to see more than twice as many snapshots as snapshot intervals, saw $countAfterTenIntervals")
      TestUtils.waitUntilTrue(() => {
        val emitterMetrics = cluster.controllers().values().iterator().next().
          sharedServer.snapshotEmitter.metrics()
        emitterMetrics.latestSnapshotGeneratedBytes() > 0
      }, "Failed to see latestSnapshotGeneratedBytes > 0")
    } finally {
      cluster.close()
    }
  }

  /**
   * Test a single broker, single controller cluster at the minimum bootstrap level. This tests
   * that we can function without having periodic NoOpRecords written.
   */
  @Test
  def testSingleControllerSingleBrokerCluster(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.MINIMUM_VERSION).
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
    } finally {
      cluster.close()
    }
  }

  @Test
  def testOverlyLargeCreateTopics(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      val admin = cluster.admin()
      try {
        val newTopics = new util.ArrayList[NewTopic]()
        for (i <- 0 to 10000) {
          newTopics.add(new NewTopic("foo" + i, 100000, 1.toShort))
        }
        val executionException = assertThrows(classOf[ExecutionException],
            () => admin.createTopics(newTopics).all().get())
        assertNotNull(executionException.getCause)
        assertEquals(classOf[PolicyViolationException], executionException.getCause.getClass)
        assertEquals("Excessively large number of partitions per request.",
          executionException.getCause.getMessage)
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testTimedOutHeartbeats(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(3).
        setNumControllerNodes(1).build()).
      setConfigProp(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, 10.toString).
      setConfigProp(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, 1000.toString).
      build()
    try {
      cluster.format()
      cluster.startup()
      val controller = cluster.controllers().values().iterator().next()
      controller.controller.waitForReadyBrokers(3).get()
      TestUtils.retry(60000) {
        val latch = QuorumControllerIntegrationTestUtils.pause(controller.controller.asInstanceOf[QuorumController])
        Thread.sleep(1001)
        latch.countDown()
        assertEquals(0, controller.sharedServer.controllerServerMetrics.fencedBrokerCount())
        assertTrue(controller.quorumControllerMetrics.timedOutHeartbeats() > 0,
          "Expected timedOutHeartbeats to be greater than 0.")
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testRegisteredControllerEndpoints(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(3).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      TestUtils.retry(60000) {
        val controller = cluster.controllers().values().iterator().next()
        val registeredControllers = controller.registrationsPublisher.controllers()
        assertEquals(3, registeredControllers.size(), "Expected 3 controller registrations")
        registeredControllers.values().forEach(registration => {
          assertNotNull(registration.listeners.get("CONTROLLER"))
          assertNotEquals(0, registration.listeners.get("CONTROLLER").port())
        })
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testDirectToControllerCommunicationFailsOnOlderMetadataVersion(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_6_IV2).
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val admin = cluster.admin(util.Map.of(), true)
      try {
        val exception = assertThrows(classOf[ExecutionException],
          () => admin.describeCluster().clusterId().get(1, TimeUnit.MINUTES))
        assertNotNull(exception.getCause)
        assertEquals(classOf[UnsupportedVersionException], exception.getCause.getClass)
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testStartupWithNonDefaultKControllerDynamicConfiguration(): Unit = {
    val bootstrapRecords = util.List.of(
      new ApiMessageAndVersion(new FeatureLevelRecord().
        setName(MetadataVersion.FEATURE_NAME).
        setFeatureLevel(MetadataVersion.IBP_3_7_IV0.featureLevel), 0.toShort),
      new ApiMessageAndVersion(new ConfigRecord().
        setResourceType(ConfigResource.Type.BROKER.id).
        setResourceName("").
        setName("num.io.threads").
        setValue("9"), 0.toShort))
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder(BootstrapMetadata.fromRecords(bootstrapRecords, "testRecords")).
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val controller = cluster.controllers().values().iterator().next()
      TestUtils.retry(60000) {
        assertNotNull(controller.controllerApisHandlerPool)
        assertEquals(9, controller.controllerApisHandlerPool.threadPoolSize.get())
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testTopicDeletedAndRecreatedWhileBrokerIsDown(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_6_IV2).
        setNumBrokerNodes(3).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val admin = cluster.admin()
      try {
        val broker0 = cluster.brokers().get(0)
        val broker1 = cluster.brokers().get(1)
        val foo0 = new TopicPartition("foo", 0)

        admin.createTopics(util.List.of(
          new NewTopic("foo", 3, 3.toShort))).all().get()

        // Wait until foo-0 is created on broker0.
        TestUtils.retry(60000) {
          assertTrue(broker0.logManager.getLog(foo0).isDefined)
        }

        // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
        broker0.shutdown()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getLeaderAndIsr("foo", 0)
          assertTrue(info.isPresent)
          assertEquals(Set(1, 2), info.get.isr().asScala.toSet)
        }

        // Modify foo-0 so that it has the wrong topic ID.
        val logDir = broker0.logManager.getLog(foo0).get.dir
        val partitionMetadataFile = new File(logDir, "partition.metadata")
        Files.write(partitionMetadataFile.toPath,
          "version: 0\ntopic_id: AAAAAAAAAAAAA7SrBWaJ7g\n".getBytes(StandardCharsets.UTF_8))

        // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
        broker0.startup()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getLeaderAndIsr("foo", 0)
          assertTrue(info.isPresent)
          assertEquals(Set(0, 1, 2), info.get.isr().asScala.toSet)
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testAbandonedFutureReplicaRecovered_mainReplicaInOfflineLogDir(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_7_IV2).
        setNumBrokerNodes(3).
        setNumDisksPerBroker(2).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val admin = cluster.admin()
      try {
        val broker0 = cluster.brokers().get(0)
        val broker1 = cluster.brokers().get(1)
        val foo0 = new TopicPartition("foo", 0)

        admin.createTopics(util.List.of(
          new NewTopic("foo", 3, 3.toShort))).all().get()

        // Wait until foo-0 is created on broker0.
        TestUtils.retry(60000) {
          assertTrue(broker0.logManager.getLog(foo0).isDefined)
        }

        // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
        broker0.shutdown()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getLeaderAndIsr("foo", 0)
          assertTrue(info.isPresent)
          assertEquals(Set(1, 2), info.get.isr().asScala.toSet)
        }

        // Modify foo-0 so that it refers to a future replica.
        // This is equivalent to a failure during the promotion of the future replica and a restart with directory for
        // the main replica being offline
        val log = broker0.logManager.getLog(foo0).get
        log.renameDir(UnifiedLog.logFutureDirName(foo0), false)

        // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
        broker0.startup()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getLeaderAndIsr("foo", 0)
          assertTrue(info.isPresent)
          assertEquals(Set(0, 1, 2), info.get.isr().asScala.toSet)
          assertTrue(broker0.logManager.getLog(foo0, isFuture = true).isEmpty)
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  def copyDirectory(src: String, dest: String): Unit = {
    Files.walk(Paths.get(src)).forEach(p => {
      val out = Paths.get(dest, p.toString.substring(src.length()))
      if (!p.toString.equals(src)) {
        Files.copy(p, out)
      }
    })
  }

  @Test
  def testAbandonedFutureReplicaRecovered_mainReplicaInOnlineLogDir(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_7_IV2).
        setNumBrokerNodes(3).
        setNumDisksPerBroker(2).
        setNumControllerNodes(1).build()).
      build()
    try {
      cluster.format()
      cluster.startup()
      val admin = cluster.admin()
      try {
        val broker0 = cluster.brokers().get(0)
        val broker1 = cluster.brokers().get(1)
        val foo0 = new TopicPartition("foo", 0)

        admin.createTopics(util.List.of(
          new NewTopic("foo", 3, 3.toShort))).all().get()

        // Wait until foo-0 is created on broker0.
        TestUtils.retry(60000) {
          assertTrue(broker0.logManager.getLog(foo0).isDefined)
        }

        // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
        broker0.shutdown()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getLeaderAndIsr("foo", 0)
          assertTrue(info.isPresent)
          assertEquals(Set(1, 2), info.get.isr().asScala.toSet)
        }

        val log = broker0.logManager.getLog(foo0).get

        // Copy foo-0 to targetParentDir
        // This is so that we can rename the main replica to a future down below
        val parentDir = log.parentDir
        val targetParentDir = broker0.config.logDirs.stream().filter(l => !l.equals(parentDir)).findFirst().get()
        val targetDirFile = new File(targetParentDir, log.dir.getName)
        targetDirFile.mkdir()
        copyDirectory(log.dir.toString, targetDirFile.toString)
        assertTrue(targetDirFile.exists())

        // Rename original log to a future
        // This is equivalent to a failure during the promotion of the future replica and a restart with directory for
        // the main replica being online
        val originalLogFile = log.dir
        log.renameDir(UnifiedLog.logFutureDirName(foo0), false)
        assertFalse(originalLogFile.exists())

        // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
        broker0.startup()
        TestUtils.retry(60000) {
          val info = broker1.metadataCache.getLeaderAndIsr("foo", 0)
          assertTrue(info.isPresent)
          assertEquals(Set(0, 1, 2), info.get.isr().asScala.toSet)
          assertTrue(broker0.logManager.getLog(foo0, isFuture = true).isEmpty)
          assertFalse(targetDirFile.exists())
          assertTrue(originalLogFile.exists())
        }
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testControllerFailover(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(5).build()).build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      TestUtils.waitUntilTrue(() => cluster.brokers().get(0).brokerState == BrokerState.RUNNING,
        "Broker never made it to RUNNING state.")
      TestUtils.waitUntilTrue(() => cluster.raftManagers().get(0).client.leaderAndEpoch().leaderId.isPresent,
        "RaftManager was not initialized.")

      val admin = cluster.admin()
      try {
        // Create a test topic
        admin.createTopics(util.List.of(
          new NewTopic("test-topic", 1, 1.toShort))).all().get()
        waitForTopicListing(admin, Seq("test-topic"), Seq())

        // Shut down active controller
        val active = cluster.waitForActiveController()
        cluster.raftManagers().get(active.asInstanceOf[QuorumController].nodeId()).shutdown()

        // Create a test topic on the new active controller
        admin.createTopics(util.List.of(
          new NewTopic("test-topic2", 1, 1.toShort))).all().get()
        waitForTopicListing(admin, Seq("test-topic2"), Seq())
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  /**
   * Test that once a cluster is formatted, a bootstrap.metadata file that contains an unsupported
   * MetadataVersion is not a problem. This is a regression test for KAFKA-19192.
   */
  @Test
  def testOldBootstrapMetadataFile(): Unit = {
    val baseDirectory = TestUtils.tempDir().toPath()
    Using.resource(new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).
        setBaseDirectory(baseDirectory).
          build()).
      setDeleteOnClose(false).
        build()
    ) { cluster =>
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
    }
    val oldBootstrapMetadata = BootstrapMetadata.fromRecords(
      util.List.of(
        new ApiMessageAndVersion(
          new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(1),
          0.toShort)
      ),
      "oldBootstrapMetadata")
    // Re-create the cluster using the same directory structure as above.
    // Since we do not need to use the bootstrap metadata, the fact that
    // it specifies an obsolete metadata.version should not be a problem.
    Using.resource(new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).
        setBaseDirectory(baseDirectory).
        setBootstrapMetadata(oldBootstrapMetadata).
          build()).build()
    ) { cluster =>
      cluster.startup()
      cluster.waitForReadyBrokers()
    }
  }

  @Test
  def testIncreaseNumIoThreads(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumBrokerNodes(1).
        setNumControllerNodes(1).build()).
      setConfigProp(ServerConfigs.NUM_IO_THREADS_CONFIG, "4").
      build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val admin = cluster.admin()
      try {
        admin.incrementalAlterConfigs(
          util.Map.of(new ConfigResource(Type.BROKER, ""),
            util.List.of(new AlterConfigOp(
              new ConfigEntry(ServerConfigs.NUM_IO_THREADS_CONFIG, "8"), OpType.SET)))).all().get()
        val newTopic = util.List.of(new NewTopic("test-topic", 1, 1.toShort))
        val createTopicResult = admin.createTopics(newTopic)
        createTopicResult.all().get()
        waitForTopicListing(admin, Seq("test-topic"), Seq())
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }
}
