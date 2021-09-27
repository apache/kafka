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
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{Test, Timeout}


@Timeout(value = 40)
class StorageToolTest {
  private def newSelfManagedProperties() = {
    val properties = new Properties()
    properties.setProperty(KafkaConfig.LogDirsProp, "/tmp/foo,/tmp/bar")
    properties.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    properties.setProperty(KafkaConfig.NodeIdProp, "2")
    properties.setProperty(KafkaConfig.QuorumVotersProp, s"2@localhost:9092")
    properties.setProperty(KafkaConfig.ControllerListenerNamesProp, "PLAINTEXT")
    properties
  }

  @Test
  def testConfigToLogDirectories(): Unit = {
    val config = new KafkaConfig(newSelfManagedProperties())
    assertEquals(Seq("/tmp/bar", "/tmp/foo"), StorageTool.configToLogDirectories(config))
  }

  @Test
  def testConfigToLogDirectoriesWithMetaLogDir(): Unit = {
    val properties = newSelfManagedProperties()
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
    } finally Utils.delete(tempDir)
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
    } finally Utils.delete(tempDir)
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
    } finally tempFile.delete()
  }

  @Test
  def testInfoWithMismatchedLegacyKafkaConfig(): Unit = {
    val stream = new ByteArrayOutputStream()
    val tempDir = TestUtils.tempDir()
    try {
      Files.write(tempDir.toPath.resolve("meta.properties"),
        String.join("\n", util.Arrays.asList(
          "version=1",
          "cluster.id=XcZZOzUqS4yHOjhMQB6JLQ")).
            getBytes(StandardCharsets.UTF_8))
      assertEquals(1, StorageTool.
        infoCommand(new PrintStream(stream), false, Seq(tempDir.toString)))
      assertEquals(s"""Found log directory:
  ${tempDir.toString}

Found metadata: {cluster.id=XcZZOzUqS4yHOjhMQB6JLQ, version=1}

Found problem:
  The kafka configuration file appears to be for a legacy cluster, but the directories are formatted for a cluster in KRaft mode.

""", stream.toString())
    } finally Utils.delete(tempDir)
  }

  @Test
  def testInfoWithMismatchedSelfManagedKafkaConfig(): Unit = {
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

Found metadata: {broker.id=1, cluster.id=26c36907-4158-4a35-919d-6534229f5241, version=0}

Found problem:
  The kafka configuration file appears to be for a cluster in KRaft mode, but the directories are formatted for legacy mode.

""", stream.toString())
    } finally Utils.delete(tempDir)
  }

  @Test
  def testFormatEmptyDirectory(): Unit = {
    val tempDir = TestUtils.tempDir()
    try {
      val metaProperties = MetaProperties(
        clusterId = "XcZZOzUqS4yHOjhMQB6JLQ", nodeId = 2)
      val stream = new ByteArrayOutputStream()
      assertEquals(0, StorageTool.
        formatCommand(new PrintStream(stream), Seq(tempDir.toString), metaProperties, false))
      assertEquals("Formatting %s%n".format(tempDir), stream.toString())

      try assertEquals(1, StorageTool.
        formatCommand(new PrintStream(new ByteArrayOutputStream()), Seq(tempDir.toString), metaProperties, false)) catch {
        case e: TerseFailure => assertEquals(s"Log directory ${tempDir} is already " +
          "formatted. Use --ignore-formatted to ignore this directory and format the " +
          "others.", e.getMessage)
      }

      val stream2 = new ByteArrayOutputStream()
      assertEquals(0, StorageTool.
        formatCommand(new PrintStream(stream2), Seq(tempDir.toString), metaProperties, true))
      assertEquals("All of the log directories are already formatted.%n".format(), stream2.toString())
    } finally Utils.delete(tempDir)
  }

  @Test
  def testFormatWithInvalidClusterId(): Unit = {
    val config = new KafkaConfig(newSelfManagedProperties())
    assertEquals("Cluster ID string invalid does not appear to be a valid UUID: " +
      "Input string `invalid` decoded as 5 bytes, which is not equal to the expected " +
        "16 bytes of a base64-encoded UUID", assertThrows(classOf[TerseFailure],
          () => StorageTool.buildMetadataProperties("invalid", config)).getMessage)
  }
}
