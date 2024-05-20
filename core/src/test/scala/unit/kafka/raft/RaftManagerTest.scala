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
package kafka.raft

import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.Properties
import java.util.concurrent.CompletableFuture
import kafka.log.LogManager
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.tools.TestRaftServer.ByteArraySerde
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ServerLogConfigs, ZkConfigs}
import org.apache.kafka.server.ProcessRole
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.apache.kafka.server.fault.FaultHandler
import org.mockito.Mockito._

import scala.util.Using


class RaftManagerTest {
  private def createZkBrokerConfig(
    migrationEnabled: Boolean,
    nodeId: Int,
    logDir: Seq[Path],
    metadataDir: Option[Path]
  ): KafkaConfig = {
    val props = new Properties
    logDir.foreach { value =>
      props.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, value.toString)
    }
    if (migrationEnabled) {
      metadataDir.foreach { value =>
        props.setProperty(KRaftConfigs.METADATA_LOG_DIR_CONFIG, value.toString)
      }
      props.setProperty(KRaftConfigs.MIGRATION_ENABLED_CONFIG, "true")
      props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9093")
      props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    }

    props.setProperty(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    props.setProperty(KafkaConfig.BrokerIdProp, nodeId.toString)
    new KafkaConfig(props)
  }

  private def createConfig(
    processRoles: Set[ProcessRole],
    nodeId: Int,
    logDir: Seq[Path],
    metadataDir: Option[Path]
  ): KafkaConfig = {
    val props = new Properties
    logDir.foreach { value =>
      props.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, value.toString)
    }
    metadataDir.foreach { value =>
      props.setProperty(KRaftConfigs.METADATA_LOG_DIR_CONFIG, value.toString)
    }
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, processRoles.mkString(","))
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    if (processRoles.contains(ProcessRole.BrokerRole)) {
      props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")
      if (processRoles.contains(ProcessRole.ControllerRole)) { // co-located
        props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
        props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9093")
      } else { // broker-only
        val voterId = nodeId + 1
        props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$voterId@localhost:9093")
      }
    } else if (processRoles.contains(ProcessRole.ControllerRole)) { // controller-only
      props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "SSL://localhost:9093")
      props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9093")
    }

    new KafkaConfig(props)
  }

  private def createRaftManager(
    topicPartition: TopicPartition,
    config: KafkaConfig
  ): KafkaRaftManager[Array[Byte]] = {
    val topicId = new Uuid(0L, 2L)

    new KafkaRaftManager[Array[Byte]](
      Uuid.randomUuid.toString,
      config,
      Uuid.randomUuid,
      new ByteArraySerde,
      topicPartition,
      topicId,
      Time.SYSTEM,
      new Metrics(Time.SYSTEM),
      Option.empty,
      CompletableFuture.completedFuture(QuorumConfig.parseVoterConnections(config.quorumVoters)),
      mock(classOf[FaultHandler])
    )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("broker", "controller", "broker,controller"))
  def testNodeIdPresent(processRoles: String): Unit = {
    var processRolesSet = Set.empty[ProcessRole]
    if (processRoles.contains("broker")) {
      processRolesSet = processRolesSet ++ Set(ProcessRole.BrokerRole)
    }
    if (processRoles.contains("controller")) {
      processRolesSet = processRolesSet ++ Set(ProcessRole.ControllerRole)
    }

    val logDir = TestUtils.tempDir()
    val nodeId = 1
    val raftManager = createRaftManager(
      new TopicPartition("__raft_id_test", 0),
      createConfig(
        processRolesSet,
        nodeId,
        Seq(logDir.toPath),
        None
      )
    )
    assertEquals(nodeId, raftManager.client.nodeId.getAsInt)
    raftManager.shutdown()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("metadata-only", "log-only", "both"))
  def testLogDirLockWhenControllerOnly(dirType: String): Unit = {
    val logDir = if (dirType.equals("metadata-only")) {
      Seq.empty
    } else {
      Seq(TestUtils.tempDir().toPath)
    }

    val metadataDir = if (dirType.equals("log-only")) {
      None
    } else {
      Some(TestUtils.tempDir().toPath)
    }

    val nodeId = 1
    val raftManager = createRaftManager(
      new TopicPartition("__raft_id_test", 0),
      createConfig(
        Set(ProcessRole.ControllerRole),
        nodeId,
        logDir,
        metadataDir
      )
    )

    val lockPath = metadataDir.getOrElse(logDir.head).resolve(LogManager.LockFileName)
    assertTrue(fileLocked(lockPath))

    raftManager.shutdown()

    assertFalse(fileLocked(lockPath))
  }

  @Test
  def testLogDirLockWhenBrokerOnlyWithSeparateMetadataDir(): Unit = {
    val logDir = Seq(TestUtils.tempDir().toPath)
    val metadataDir = Some(TestUtils.tempDir().toPath)

    val nodeId = 1
    val raftManager = createRaftManager(
      new TopicPartition("__raft_id_test", 0),
      createConfig(
        Set(ProcessRole.BrokerRole),
        nodeId,
        logDir,
        metadataDir
      )
    )

    val lockPath = metadataDir.getOrElse(logDir.head).resolve(LogManager.LockFileName)
    assertTrue(fileLocked(lockPath))

    raftManager.shutdown()

    assertFalse(fileLocked(lockPath))
  }

  def createMetadataLog(config: KafkaConfig): Unit = {
    val raftManager = createRaftManager(
      new TopicPartition("__cluster_metadata", 0),
      config
    )
    raftManager.shutdown()
  }

  def assertLogDirsExist(
    logDirs: Seq[Path],
    metadataLogDir: Option[Path],
    expectMetadataLog: Boolean
  ): Unit = {
    // In all cases, the log dir and metadata log dir themselves should be untouched
    assertTrue(Files.exists(metadataLogDir.get))
    logDirs.foreach { logDir =>
      assertTrue(Files.exists(logDir), "Should not delete log dir")
    }

    if (expectMetadataLog) {
      assertTrue(Files.exists(metadataLogDir.get.resolve("__cluster_metadata-0")))
    } else {
      assertFalse(Files.exists(metadataLogDir.get.resolve("__cluster_metadata-0")))
    }
  }

  @Test
  def testMigratingZkBrokerDeletesMetadataLog(): Unit = {
    val logDirs = Seq(TestUtils.tempDir().toPath)
    val metadataLogDir = Some(TestUtils.tempDir().toPath)
    val nodeId = 1
    val config = createZkBrokerConfig(migrationEnabled = true, nodeId, logDirs, metadataLogDir)
    createMetadataLog(config)

    KafkaRaftManager.maybeDeleteMetadataLogDir(config)
    assertLogDirsExist(logDirs, metadataLogDir, expectMetadataLog = false)
  }

  @Test
  def testNonMigratingZkBrokerDoesNotDeleteMetadataLog(): Unit = {
    val logDirs = Seq(TestUtils.tempDir().toPath)
    val metadataLogDir = Some(TestUtils.tempDir().toPath)
    val nodeId = 1

    val config = createZkBrokerConfig(migrationEnabled = false, nodeId, logDirs, metadataLogDir)

    // Create the metadata log dir directly as if the broker was previously in migration mode.
    // This simulates a misconfiguration after downgrade
    Files.createDirectory(metadataLogDir.get.resolve("__cluster_metadata-0"))

    val err = assertThrows(classOf[RuntimeException], () => KafkaRaftManager.maybeDeleteMetadataLogDir(config),
      "Should have not deleted the metadata log")
    assertEquals("Not deleting metadata log dir since migrations are not enabled.", err.getMessage)

    assertLogDirsExist(logDirs, metadataLogDir, expectMetadataLog = true)
  }

  @Test
  def testZkBrokerDoesNotDeleteSeparateLogDirs(): Unit = {
    val logDirs = Seq(TestUtils.tempDir().toPath, TestUtils.tempDir().toPath)
    val metadataLogDir = Some(TestUtils.tempDir().toPath)
    val nodeId = 1
    val config = createZkBrokerConfig(migrationEnabled = true, nodeId, logDirs, metadataLogDir)
    createMetadataLog(config)

    KafkaRaftManager.maybeDeleteMetadataLogDir(config)
    assertLogDirsExist(logDirs, metadataLogDir, expectMetadataLog = false)
  }

  @Test
  def testZkBrokerDoesNotDeleteSameLogDir(): Unit = {
    val logDirs = Seq(TestUtils.tempDir().toPath, TestUtils.tempDir().toPath)
    val metadataLogDir = logDirs.headOption
    val nodeId = 1
    val config = createZkBrokerConfig(migrationEnabled = true, nodeId, logDirs, metadataLogDir)
    createMetadataLog(config)

    KafkaRaftManager.maybeDeleteMetadataLogDir(config)
    assertLogDirsExist(logDirs, metadataLogDir, expectMetadataLog = false)
  }

  @Test
  def testKRaftBrokerDoesNotDeleteMetadataLog(): Unit = {
    val logDirs = Seq(TestUtils.tempDir().toPath)
    val metadataLogDir = Some(TestUtils.tempDir().toPath)
    val nodeId = 1
    val config = createConfig(
      Set(ProcessRole.BrokerRole),
      nodeId,
      logDirs,
      metadataLogDir
    )
    createMetadataLog(config)

    assertThrows(classOf[RuntimeException], () => KafkaRaftManager.maybeDeleteMetadataLogDir(config),
      "Should not have deleted metadata log")
    assertLogDirsExist(logDirs, metadataLogDir, expectMetadataLog = true)

  }

  private def fileLocked(path: Path): Boolean = {
    Using.resource(FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) { channel =>
      try {
        Option(channel.tryLock()).foreach(_.close())
        false
      } catch {
        case _: OverlappingFileLockException => true
      }
    }
  }
}
