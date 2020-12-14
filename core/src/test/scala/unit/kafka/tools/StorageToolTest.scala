/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util
import java.util.Properties

import kafka.server.{KafkaConfig, MetaProperties}
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Utils
import org.junit.Assert.assertEquals
import org.junit.Test

class StorageToolTest {
  private def newKip500Properties(): Properties = {
    val properties = new Properties()
    properties.setProperty(KafkaConfig.LogDirsProp, "/tmp/foo,/tmp/bar")
    properties.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    properties.setProperty(KafkaConfig.ControllerIdProp, "2")
    properties
  }

  @Test
  def testConfigToLogDirectories(): Unit = {
    val config = new KafkaConfig(newKip500Properties())
    assertEquals(Seq("/tmp/bar", "/tmp/foo"), StorageTool.configToLogDirectories(config))
  }

  @Test
  def testConfigToLogDirectoriesWithMetaLogDir(): Unit = {
    val properties = newKip500Properties()
    properties.setProperty(KafkaConfig.MetadataLogDirProp, "/tmp/baz")
    val config = new KafkaConfig(properties)
    assertEquals(Seq("/tmp/bar", "/tmp/baz", "/tmp/foo"),
      StorageTool.configToLogDirectories(config))
  }

  @Test
  def testInfoCommandOnEmptyDirectory(): Unit = {
    val stream = new ByteArrayOutputStream()
    val tempDir = TestUtils.tempDir()
    try {
      assertEquals(1, StorageTool.
        infoCommand(new PrintStream(stream), true, Seq(tempDir.toString)))
      assertEquals(s"""Found log directory:
  ${tempDir.toString}

Found problem:
  ${tempDir.toString} is not formatted.

""", stream.toString())
    } finally {
      Utils.delete(tempDir)
    }
  }

  @Test
  def testInfoCommandOnMissingDirectory(): Unit = {
    val stream = new ByteArrayOutputStream()
    val tempDir = TestUtils.tempDir()
    tempDir.delete()
    try {
      assertEquals(1, StorageTool.
        infoCommand(new PrintStream(stream), true, Seq(tempDir.toString)))
      assertEquals(s"""Found problem:
  ${tempDir.toString} does not exist

""", stream.toString())
    } finally {
      Utils.delete(tempDir)
    }
  }

  @Test
  def testInfoCommandOnDirectoryAsFile(): Unit = {
    val stream = new ByteArrayOutputStream()
    val tempFile = TestUtils.tempFile()
    try {
      assertEquals(1, StorageTool.
        infoCommand(new PrintStream(stream), true, Seq(tempFile.toString)))
      assertEquals(s"""Found problem:
  ${tempFile.toString} is not a directory

""", stream.toString())
    } finally {
      tempFile.delete()
    }
  }

  @Test
  def testInfoWithMismatchedLegacyKafkaConfig(): Unit = {
    val stream = new ByteArrayOutputStream()
    val tempDir = TestUtils.tempDir()
    try {
      Files.write(tempDir.toPath.resolve("meta.properties"),
        String.join("\n", util.Arrays.asList(
          "version=1",
          "cluster.id=26c36907-4158-4a35-919d-6534229f5241")).
            getBytes(StandardCharsets.UTF_8))
      assertEquals(1, StorageTool.
        infoCommand(new PrintStream(stream), false, Seq(tempDir.toString)))
      assertEquals(s"""Found log directory:
  ${tempDir.toString}

Found metadata: MetaProperties(clusterId=26c36907-4158-4a35-919d-6534229f5241)

Found problem:
  The kafka configuration file appears to be for a legacy cluster, but the directories are formatted for kip-500.

""", stream.toString())
    } finally {
      Utils.delete(tempDir)
    }
  }

  @Test
  def testInfoWithMismatchedKip500KafkaConfig(): Unit = {
    val stream = new ByteArrayOutputStream()
    val tempDir = TestUtils.tempDir()
    try {
      Files.write(tempDir.toPath.resolve("meta.properties"),
        String.join("\n", util.Arrays.asList(
          "version=0",
          "broker.id=1",
          "cluster.id=26c36907-4158-4a35-919d-6534229f5241")).
          getBytes(StandardCharsets.UTF_8))
      assertEquals(1, StorageTool.
        infoCommand(new PrintStream(stream), true, Seq(tempDir.toString)))
      assertEquals(s"""Found log directory:
  ${tempDir.toString}

Found metadata: LegacyMetaProperties(brokerId=1, clusterId=26c36907-4158-4a35-919d-6534229f5241)

Found problem:
  The kafka configuration file appears to be for a kip-500 cluster, but the directories are formatted for legacy mode.

""", stream.toString())
    } finally {
      Utils.delete(tempDir)
    }
  }

  @Test
  def testFormatEmptyDirectory(): Unit = {
    val tempDir = TestUtils.tempDir()
    val clusterId = "26c36907-4158-4a35-919d-6534229f5241"
    try {
      val metaProperties = MetaProperties(clusterId = Uuid.fromString(clusterId), brokerId = None, controllerId = None)
      val stream = new ByteArrayOutputStream()
      assertEquals(0, StorageTool.
        formatCommand(new PrintStream(stream), Seq(tempDir.toString), metaProperties, false))
      assertEquals("Formatting %s%n".format(tempDir), stream.toString())

      try {
        assertEquals(1, StorageTool.
          formatCommand(new PrintStream(new ByteArrayOutputStream()), Seq(tempDir.toString), metaProperties, false))
      } catch {
        case e: TerseFailure => assertEquals(s"Log directory ${tempDir} is already " +
          "formatted. Use --ignore-formatted to ignore this directory and format the " +
          "others.", e.getMessage)
      }

      try {
        assertEquals(1, StorageTool.
          formatCommand(new PrintStream(new ByteArrayOutputStream()), Seq(tempDir.toString), metaProperties, true))
      } catch {
        case e: TerseFailure => assertEquals("All of the log directories are already " +
          "formatted.", e.getMessage)
      }
    } finally {
      Utils.delete(tempDir)
    }
  }

  @Test
  def testFormatWithInvalidClusterId(): Unit = {
    val tempDir = TestUtils.tempDir()
    val clusterId = "invalid"
    try {
      val metaProperties = MetaProperties(clusterId = Uuid.fromString(clusterId), brokerId = None, controllerId = None)
      assertEquals(1, StorageTool.
        formatCommand(new PrintStream(new ByteArrayOutputStream()), Seq(tempDir.toString), metaProperties, false))
    } catch {
      case e: TerseFailure =>
        assertEquals("Cluster ID string invalid does not appear to be a valid UUID: " +
          "Invalid UUID string: invalid", e.getMessage)
    } finally {
      Utils.delete(tempDir)
    }
  }
}
