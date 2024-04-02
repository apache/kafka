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
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.ProcessRole
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.apache.kafka.server.fault.FaultHandler
import org.mockito.Mockito._

class RaftManagerTest {
  private def createZkBrokerConfig(
    migrationEnabled: Boolean,
    nodeId: Int,
    logDir: Option[Path],
    metadataDir: Option[Path]
  ): KafkaConfig = {
    val props = new Properties
    logDir.foreach { value =>
      props.setProperty(KafkaConfig.LogDirProp, value.toString)
    }
    if (migrationEnabled) {
      metadataDir.foreach { value =>
        props.setProperty(KafkaConfig.MetadataLogDirProp, value.toString)
      }
      props.setProperty(KafkaConfig.MigrationEnabledProp, "true")
      props.setProperty(KafkaConfig.QuorumVotersProp, s"${nodeId}@localhost:9093")
      props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    }

    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")
    props.setProperty(KafkaConfig.BrokerIdProp, nodeId.toString)
    new KafkaConfig(props)
  }

  private def createConfig(
    processRoles: Set[ProcessRole],
    nodeId: Int,
    logDir: Option[Path],
    metadataDir: Option[Path]
  ): KafkaConfig = {
    val props = new Properties
    logDir.foreach { value =>
      props.setProperty(KafkaConfig.LogDirProp, value.toString)
    }
    metadataDir.foreach { value =>
      props.setProperty(KafkaConfig.MetadataLogDirProp, value.toString)
    }
    props.setProperty(KafkaConfig.ProcessRolesProp, processRoles.mkString(","))
    props.setProperty(KafkaConfig.NodeIdProp, nodeId.toString)
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    if (processRoles.contains(ProcessRole.BrokerRole)) {
      props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "PLAINTEXT")
      if (processRoles.contains(ProcessRole.ControllerRole)) { // co-located
        props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
        props.setProperty(KafkaConfig.QuorumVotersProp, s"${nodeId}@localhost:9093")
      } else { // broker-only
        val voterId = nodeId + 1
        props.setProperty(KafkaConfig.QuorumVotersProp, s"${voterId}@localhost:9093")
      }
    } else if (processRoles.contains(ProcessRole.ControllerRole)) { // controller-only
      props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9093")
      props.setProperty(KafkaConfig.QuorumVotersProp, s"${nodeId}@localhost:9093")
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
      new ByteArraySerde,
      topicPartition,
      topicId,
      Time.SYSTEM,
      new Metrics(Time.SYSTEM),
      Option.empty,
      CompletableFuture.completedFuture(RaftConfig.parseVoterConnections(config.quorumVoters)),
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
        Some(logDir.toPath),
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
      None
    } else {
      Some(TestUtils.tempDir().toPath)
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

    val lockPath = metadataDir.getOrElse(logDir.get).resolve(LogManager.LockFileName)
    assertTrue(fileLocked(lockPath))

    raftManager.shutdown()

    assertFalse(fileLocked(lockPath))
  }

  @Test
  def testLogDirLockWhenBrokerOnlyWithSeparateMetadataDir(): Unit = {
    val logDir = Some(TestUtils.tempDir().toPath)
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

    val lockPath = metadataDir.getOrElse(logDir.get).resolve(LogManager.LockFileName)
    assertTrue(fileLocked(lockPath))

    raftManager.shutdown()

    assertFalse(fileLocked(lockPath))
  }

  @Test
  def testMigratingZkBrokerDeletesMetadataLog(): Unit = {
    val logDir = Some(TestUtils.tempDir().toPath)
    val metadataDir = Some(TestUtils.tempDir().toPath)
    val nodeId = 1
    val config = createZkBrokerConfig(migrationEnabled = true, nodeId, logDir, metadataDir)
    val raftManager = createRaftManager(
      new TopicPartition("__cluster_metadata", 0),
      config
    )
    raftManager.shutdown()

    KafkaRaftManager.maybeDeleteMetadataLogDir(config) match {
      case Some(err) => fail("Failed to delete metadata log", err)
      case None => assertFalse(Files.exists(metadataDir.get))
    }
  }

  @Test
  def testNonMigratingZkBrokerDeletesMetadataLog(): Unit = {
    val logDir = Some(TestUtils.tempDir().toPath)
    val metadataDir = Some(TestUtils.tempDir().toPath)
    val nodeId = 1
    // Use this config to create the directory
    val config1 = createZkBrokerConfig(migrationEnabled = true, nodeId, logDir, metadataDir)
    val raftManager = createRaftManager(
      new TopicPartition("__cluster_metadata", 0),
      config1
    )
    raftManager.shutdown()

    val config2 = createZkBrokerConfig(migrationEnabled = false, nodeId, logDir, metadataDir)
    KafkaRaftManager.maybeDeleteMetadataLogDir(config2) match {
      case Some(err) => {
        assertEquals("Not deleting metadata log dir since migrations are not enabled.", err.getMessage)
        assertTrue(Files.exists(metadataDir.get))
      }
      case None => fail("Should have not deleted the metadata log")
    }
  }

  @Test
  def testKRaftBrokerDoesNotDeleteMetadataLog(): Unit = {
    val logDir = Some(TestUtils.tempDir().toPath)
    val metadataDir = Some(TestUtils.tempDir().toPath)
    val nodeId = 1
    val config = createConfig(
      Set(ProcessRole.BrokerRole),
      nodeId,
      logDir,
      metadataDir
    )
    val raftManager = createRaftManager(
      new TopicPartition("__cluster_metadata", 0),
      config
    )
    raftManager.shutdown()

    KafkaRaftManager.maybeDeleteMetadataLogDir(config) match {
      case Some(_) => assertTrue(Files.exists(metadataDir.get))
      case None => fail("Should not have deleted metadata log")
    }
  }

  private def fileLocked(path: Path): Boolean = {
    TestUtils.resource(FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) { channel =>
      try {
        Option(channel.tryLock()).foreach(_.close())
        false
      } catch {
        case _: OverlappingFileLockException => true
      }
    }
  }

}
