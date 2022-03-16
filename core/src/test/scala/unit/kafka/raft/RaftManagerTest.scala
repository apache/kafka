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

import java.util.concurrent.CompletableFuture
import java.util.Properties

import kafka.raft.KafkaRaftManager.RaftIoThread
import kafka.server.{KafkaConfig, MetaProperties}
import kafka.tools.TestRaftServer.ByteArraySerde
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft.KafkaRaftClient
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

import java.io.File

class RaftManagerTest {

  private def instantiateRaftManagerWithConfigs(topicPartition: TopicPartition, processRoles: String, nodeId: String) = {
    def configWithProcessRolesAndNodeId(processRoles: String, nodeId: String, logDir: File): KafkaConfig = {
      val props = new Properties
      props.setProperty(KafkaConfig.MetadataLogDirProp, logDir.getPath)
      props.setProperty(KafkaConfig.ProcessRolesProp, processRoles)
      props.setProperty(KafkaConfig.NodeIdProp, nodeId)
      props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
      if (processRoles.contains("broker")) {
        props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "PLAINTEXT")
        if (processRoles.contains("controller")) { // co-located
          props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
          props.setProperty(KafkaConfig.QuorumVotersProp, s"${nodeId}@localhost:9093")
        } else { // broker-only
          val voterId = (nodeId.toInt + 1)
          props.setProperty(KafkaConfig.QuorumVotersProp, s"${voterId}@localhost:9093")
        }
      } else if (processRoles.contains("controller")) { // controller-only
        props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9093")
        props.setProperty(KafkaConfig.QuorumVotersProp, s"${nodeId}@localhost:9093")
      }

      new KafkaConfig(props)
    }

    val logDir = TestUtils.tempDirectory()
    val config = configWithProcessRolesAndNodeId(processRoles, nodeId, logDir)
    val topicId = new Uuid(0L, 2L)
    val metaProperties = MetaProperties(
      clusterId = Uuid.randomUuid.toString,
      nodeId = config.nodeId
    )

    new KafkaRaftManager[Array[Byte]](
      metaProperties,
      config,
      new ByteArraySerde,
      topicPartition,
      topicId,
      Time.SYSTEM,
      new Metrics(Time.SYSTEM),
      Option.empty,
      CompletableFuture.completedFuture(RaftConfig.parseVoterConnections(config.quorumVoters))
    )
  }

  @Test
  def testSentinelNodeIdIfBrokerRoleOnly(): Unit = {
    val raftManager = instantiateRaftManagerWithConfigs(new TopicPartition("__raft_id_test", 0), "broker", "1")
    assertFalse(raftManager.client.nodeId.isPresent)
    raftManager.shutdown()
  }

  @Test
  def testNodeIdPresentIfControllerRoleOnly(): Unit = {
    val raftManager = instantiateRaftManagerWithConfigs(new TopicPartition("__raft_id_test", 0), "controller", "1")
    assertTrue(raftManager.client.nodeId.getAsInt == 1)
    raftManager.shutdown()
  }

  @Test
  def testNodeIdPresentIfColocated(): Unit = {
    val raftManager = instantiateRaftManagerWithConfigs(new TopicPartition("__raft_id_test", 0), "controller,broker", "1")
    assertTrue(raftManager.client.nodeId.getAsInt == 1)
    raftManager.shutdown()
  }

  @Test
  def testShutdownIoThread(): Unit = {
    val raftClient = mock(classOf[KafkaRaftClient[String]])
    val ioThread = new RaftIoThread(raftClient, threadNamePrefix = "test-raft")

    when(raftClient.isRunning).thenReturn(true)
    assertTrue(ioThread.isRunning)

    val shutdownFuture = new CompletableFuture[Void]
    when(raftClient.shutdown(5000)).thenReturn(shutdownFuture)

    ioThread.initiateShutdown()
    assertTrue(ioThread.isRunning)
    assertTrue(ioThread.isShutdownInitiated)
    verify(raftClient).shutdown(5000)

    shutdownFuture.complete(null)
    when(raftClient.isRunning).thenReturn(false)
    ioThread.run()
    assertFalse(ioThread.isRunning)
    assertTrue(ioThread.isShutdownComplete)
  }

  @Test
  def testUncaughtExceptionInIoThread(): Unit = {
    val raftClient = mock(classOf[KafkaRaftClient[String]])
    val ioThread = new RaftIoThread(raftClient, threadNamePrefix = "test-raft")

    when(raftClient.isRunning).thenReturn(true)
    assertTrue(ioThread.isRunning)

    when(raftClient.poll()).thenThrow(new RuntimeException)
    ioThread.run()

    assertTrue(ioThread.isShutdownComplete)
    assertTrue(ioThread.isThreadFailed)
    assertFalse(ioThread.isRunning)
  }

}
