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

import java.io.File
import java.nio.file.Files
import java.util.{Optional, Properties}
import kafka.common.{InconsistentBrokerMetadataException, InconsistentNodeIdException}
import kafka.log.UnifiedLog
import org.apache.kafka.common.{KafkaException, Uuid}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.bootstrap.{BootstrapDirectory, BootstrapMetadata}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class KafkaRaftServerTest {
  private val clusterIdBase64 = "H3KKO4NTRPaCWtEmm3vW7A"

  @Test
  def testSuccessfulLoadMetaProperties(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 0
    val metaProperties = MetaProperties(clusterId, nodeId)

    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker,controller")
    configProperties.put(KafkaConfig.NodeIdProp, nodeId.toString)
    configProperties.put(KafkaConfig.ListenersProp, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9093")
    configProperties.put(KafkaConfig.QuorumVotersProp, s"$nodeId@localhost:9093")
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")

    val (loadedMetaProperties, _, offlineDirs) =
      invokeLoadMetaProperties(metaProperties, configProperties)

    assertEquals(metaProperties, loadedMetaProperties)
    assertEquals(Seq.empty, offlineDirs)
  }

  @Test
  def testLoadMetaPropertiesWithInconsistentNodeId(): Unit = {
    val clusterId = clusterIdBase64
    val metaNodeId = 1
    val configNodeId = 0

    val metaProperties = MetaProperties(clusterId, metaNodeId)
    val configProperties = new Properties

    configProperties.put(KafkaConfig.ProcessRolesProp, "controller")
    configProperties.put(KafkaConfig.NodeIdProp, configNodeId.toString)
    configProperties.put(KafkaConfig.QuorumVotersProp, s"$configNodeId@localhost:9092")
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "PLAINTEXT")

    assertThrows(classOf[InconsistentNodeIdException], () =>
      invokeLoadMetaProperties(metaProperties, configProperties))
  }

  private def invokeLoadMetaProperties(
    metaProperties: MetaProperties,
    configProperties: Properties,
    metadataVersion: Option[MetadataVersion] = Some(MetadataVersion.latest())
  ): (MetaProperties, BootstrapMetadata, collection.Seq[String]) = {
    val tempLogDir = TestUtils.tempDirectory()
    try {
      writeMetaProperties(tempLogDir, metaProperties)
      metadataVersion.foreach(mv => writeBootstrapMetadata(tempLogDir, mv))
      configProperties.put(KafkaConfig.LogDirProp, tempLogDir.getAbsolutePath)
      val config = KafkaConfig.fromProps(configProperties)
      KafkaRaftServer.initializeLogDirs(config)
    } finally {
      Utils.delete(tempLogDir)
    }
  }

  private def writeMetaProperties(
    logDir: File,
    metaProperties: MetaProperties
  ): Unit = {
    val metaPropertiesFile = new File(logDir.getAbsolutePath, "meta.properties")
    val checkpoint = new BrokerMetadataCheckpoint(metaPropertiesFile)
    checkpoint.write(metaProperties.toProperties)
  }

  private def writeBootstrapMetadata(logDir: File, metadataVersion: MetadataVersion): Unit = {
    val bootstrapDirectory = new BootstrapDirectory(logDir.toString(), Optional.empty())
    bootstrapDirectory.writeBinaryFile(BootstrapMetadata.fromVersion(metadataVersion, "test"))
  }

  @Test
  def testStartupFailsIfMetaPropertiesMissingInSomeLogDir(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 1

    // One log dir is online and has properly formatted `meta.properties`.
    // The other is online, but has no `meta.properties`.
    val logDir1 = TestUtils.tempDirectory()
    val logDir2 = TestUtils.tempDirectory()
    writeMetaProperties(logDir1, MetaProperties(clusterId, nodeId))

    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker")
    configProperties.put(KafkaConfig.NodeIdProp, nodeId.toString)
    configProperties.put(KafkaConfig.QuorumVotersProp, s"${(nodeId + 1)}@localhost:9092")
    configProperties.put(KafkaConfig.LogDirProp, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    assertThrows(classOf[KafkaException], () => KafkaRaftServer.initializeLogDirs(config))
  }

  @Test
  def testStartupFailsIfMetaLogDirIsOffline(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 1

    // One log dir is online and has properly formatted `meta.properties`
    val validDir = TestUtils.tempDirectory()
    writeMetaProperties(validDir, MetaProperties(clusterId, nodeId))

    // Use a regular file as an invalid log dir to trigger an IO error
    val invalidDir = TestUtils.tempFile("blah")
    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker")
    configProperties.put(KafkaConfig.QuorumVotersProp, s"${(nodeId + 1)}@localhost:9092")
    configProperties.put(KafkaConfig.NodeIdProp, nodeId.toString)
    configProperties.put(KafkaConfig.MetadataLogDirProp, invalidDir.getAbsolutePath)
    configProperties.put(KafkaConfig.LogDirProp, validDir.getAbsolutePath)
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    assertThrows(classOf[KafkaException], () => KafkaRaftServer.initializeLogDirs(config))
  }

  @Test
  def testStartupDoesNotFailIfDataDirIsOffline(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 1

    // One log dir is online and has properly formatted `meta.properties`
    val validDir = TestUtils.tempDirectory()
    writeMetaProperties(validDir, MetaProperties(clusterId, nodeId))
    writeBootstrapMetadata(validDir, MetadataVersion.latest())

    // Use a regular file as an invalid log dir to trigger an IO error
    val invalidDir = TestUtils.tempFile("blah")
    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker")
    configProperties.put(KafkaConfig.NodeIdProp, nodeId.toString)
    configProperties.put(KafkaConfig.QuorumVotersProp, s"${(nodeId + 1)}@localhost:9092")
    configProperties.put(KafkaConfig.MetadataLogDirProp, validDir.getAbsolutePath)
    configProperties.put(KafkaConfig.LogDirProp, invalidDir.getAbsolutePath)
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    val (loadedProperties, _, offlineDirs) = KafkaRaftServer.initializeLogDirs(config)
    assertEquals(nodeId, loadedProperties.nodeId)
    assertEquals(Seq(invalidDir.getAbsolutePath), offlineDirs)
  }

  @Test
  def testStartupFailsIfUnexpectedMetadataDir(): Unit = {
    val nodeId = 1
    val clusterId = clusterIdBase64

    // Create two directories with valid `meta.properties`
    val metadataDir = TestUtils.tempDirectory()
    val dataDir = TestUtils.tempDirectory()

    Seq(metadataDir, dataDir).foreach { dir =>
      writeMetaProperties(dir, MetaProperties(clusterId, nodeId))
    }

    // Create the metadata dir in the data directory
    Files.createDirectory(new File(dataDir, UnifiedLog.logDirName(KafkaRaftServer.MetadataPartition)).toPath)

    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker")
    configProperties.put(KafkaConfig.NodeIdProp, nodeId.toString)
    configProperties.put(KafkaConfig.QuorumVotersProp, s"${(nodeId + 1)}@localhost:9092")
    configProperties.put(KafkaConfig.MetadataLogDirProp, metadataDir.getAbsolutePath)
    configProperties.put(KafkaConfig.LogDirProp, dataDir.getAbsolutePath)
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    assertThrows(classOf[KafkaException], () => KafkaRaftServer.initializeLogDirs(config))
  }

  @Test
  def testLoadPropertiesWithInconsistentClusterIds(): Unit = {
    val nodeId = 1
    val logDir1 = TestUtils.tempDirectory()
    val logDir2 = TestUtils.tempDirectory()

    // Create a random clusterId in each log dir
    Seq(logDir1, logDir2).foreach { dir =>
      writeMetaProperties(dir, MetaProperties(clusterId = Uuid.randomUuid().toString, nodeId))
    }

    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker")
    configProperties.put(KafkaConfig.QuorumVotersProp, s"${(nodeId + 1)}@localhost:9092")
    configProperties.put(KafkaConfig.NodeIdProp, nodeId.toString)
    configProperties.put(KafkaConfig.LogDirProp, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
    val config = KafkaConfig.fromProps(configProperties)

    assertThrows(classOf[InconsistentBrokerMetadataException],
      () => KafkaRaftServer.initializeLogDirs(config))
  }

  @Test
  def testKRaftUpdateWithIBP(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 0
    val metaProperties = MetaProperties(clusterId, nodeId)

    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker,controller")
    configProperties.put(KafkaConfig.NodeIdProp, nodeId.toString)
    configProperties.put(KafkaConfig.ListenersProp, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9093")
    configProperties.put(KafkaConfig.QuorumVotersProp, s"$nodeId@localhost:9093")
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
    configProperties.put(KafkaConfig.InterBrokerProtocolVersionProp, "3.3-IV1")

    val (loadedMetaProperties, bootstrapMetadata, offlineDirs) =
      invokeLoadMetaProperties(metaProperties, configProperties, None)

    assertEquals(metaProperties, loadedMetaProperties)
    assertEquals(Seq.empty, offlineDirs)
    assertEquals(bootstrapMetadata.metadataVersion(), MetadataVersion.IBP_3_3_IV1)
  }

  @Test
  def testKRaftUpdateWithoutIBP(): Unit = {
    val clusterId = clusterIdBase64
    val nodeId = 0
    val metaProperties = MetaProperties(clusterId, nodeId)

    val logDir = TestUtils.tempDirectory()
    writeMetaProperties(logDir, metaProperties)

    val configProperties = new Properties
    configProperties.put(KafkaConfig.ProcessRolesProp, "broker,controller")
    configProperties.put(KafkaConfig.NodeIdProp, nodeId.toString)
    configProperties.put(KafkaConfig.ListenersProp, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9093")
    configProperties.put(KafkaConfig.QuorumVotersProp, s"$nodeId@localhost:9093")
    configProperties.put(KafkaConfig.ControllerListenerNamesProp, "SSL")
    configProperties.put(KafkaConfig.LogDirProp, logDir.getAbsolutePath)

    val (loadedMetaProperties, bootstrapMetadata, offlineDirs) =
      invokeLoadMetaProperties(metaProperties, configProperties, None)

    assertEquals(metaProperties, loadedMetaProperties)
    assertEquals(Seq.empty, offlineDirs)
    assertEquals(bootstrapMetadata.metadataVersion(), MetadataVersion.latest())
  }
}
