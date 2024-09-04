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

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util
import java.util.Properties
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import net.sourceforge.argparse4j.inf.ArgumentParserException
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.common.{Features, MetadataVersion}
import org.apache.kafka.metadata.properties.{MetaPropertiesEnsemble, PropertiesUtils}
import org.apache.kafka.metadata.storage.FormatterException
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.config.{KRaftConfigs, ServerConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.mutable.ListBuffer

@Timeout(value = 40)
class StorageToolTest {

  private def newSelfManagedProperties() = {
    val properties = new Properties()
    properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/foo,/tmp/bar")
    properties.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    properties.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    properties.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, s"2@localhost:9092")
    properties.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "PLAINTEXT")
    properties
  }

  val allFeatures = Features.FEATURES.toList

  @Test
  def testConfigToLogDirectories(): Unit = {
    val config = new KafkaConfig(newSelfManagedProperties())
    assertEquals(Seq("/tmp/bar", "/tmp/foo"), StorageTool.configToLogDirectories(config))
  }

  @Test
  def testConfigToLogDirectoriesWithMetaLogDir(): Unit = {
    val properties = newSelfManagedProperties()
    properties.setProperty(KRaftConfigs.METADATA_LOG_DIR_CONFIG, "/tmp/baz")
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
      Files.write(tempDir.toPath.resolve(MetaPropertiesEnsemble.META_PROPERTIES_NAME),
        String.join("\n", util.Arrays.asList(
          "version=1",
          "node.id=1",
          "cluster.id=XcZZOzUqS4yHOjhMQB6JLQ")).
            getBytes(StandardCharsets.UTF_8))
      assertEquals(1, StorageTool.
        infoCommand(new PrintStream(stream), false, Seq(tempDir.toString)))
      assertEquals(s"""Found log directory:
  ${tempDir.toString}

Found metadata: {cluster.id=XcZZOzUqS4yHOjhMQB6JLQ, node.id=1, version=1}

Found problem:
  The kafka configuration file appears to be for a legacy cluster, but the directories are formatted for a cluster in KRaft mode.

""", stream.toString())
    } finally Utils.delete(tempDir)
  }

  @Test
  def testInfoWithMismatchedKRaftConfig(): Unit = {
    val stream = new ByteArrayOutputStream()
    val tempDir = TestUtils.tempDir()
    try {
      Files.write(tempDir.toPath.resolve(MetaPropertiesEnsemble.META_PROPERTIES_NAME),
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

  val defaultStaticQuorumProperties = new Properties()
  defaultStaticQuorumProperties.setProperty("process.roles", "broker")
  defaultStaticQuorumProperties.setProperty("node.id", "0")
  defaultStaticQuorumProperties.setProperty("controller.listener.names", "CONTROLLER")
  defaultStaticQuorumProperties.setProperty("controller.quorum.voters", "100@localhost:9093")

  val defaultDynamicQuorumProperties = new Properties()
  defaultDynamicQuorumProperties.setProperty("process.roles", "controller")
  defaultDynamicQuorumProperties.setProperty("node.id", "0")
  defaultDynamicQuorumProperties.setProperty("controller.listener.names", "CONTROLLER")
  defaultDynamicQuorumProperties.setProperty("controller.quorum.voters", "0@localhost:9093")
  defaultDynamicQuorumProperties.setProperty("listeners", "CONTROLLER://127.0.0.1:9093")
  defaultDynamicQuorumProperties.setProperty(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
  defaultDynamicQuorumProperties.setProperty(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG , "true")

  private def runFormatCommand(
    stream: ByteArrayOutputStream,
    properties: Properties,
    extraArguments: Seq[String] = Seq(),
    ignoreFormatted: Boolean = false
   ): Int = {
    val tempDir = TestUtils.tempDir()
    try {
      val configPathString = new File(tempDir.getAbsolutePath(), "format.props").toString
      PropertiesUtils.writePropertiesFile(properties, configPathString, true)
      val arguments = ListBuffer[String]("format",
        "--cluster-id", "XcZZOzUqS4yHOjhMQB6JLQ")
      if (ignoreFormatted) {
        arguments += "--ignore-formatted"
      }
      arguments += "--config"
      arguments += configPathString
      extraArguments.foreach(arguments += _)
      StorageTool.execute(arguments.toArray, new PrintStream(stream))
    } finally {
      Utils.delete(tempDir)
    }
  }

  @Test
  def testFormatSucceedsIfAllDirectoriesAreAvailable(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir(), TestUtils.tempDir(), TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties))

    assertTrue(stream.toString().
      contains("Formatting metadata directory %s".format(availableDirs.head)),
        "Failed to find content in output: " + stream.toString())
    availableDirs.tail.foreach {
      dir => assertTrue(
        stream.toString().contains("Formatting data directory %s".format(dir)),
          "Failed to find content in output: " + stream.toString())
    }
  }

  @Test
  def testFormatSucceedsIfAtLeastOneDirectoryIsAvailable(): Unit = {
    val availableDir1 = TestUtils.tempDir()
    val unavailableDir1 = TestUtils.tempFile()
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", s"${availableDir1},${unavailableDir1}")
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties))

    assertTrue(stream.toString().contains("Formatting metadata directory %s".format(availableDir1)),
      "Failed to find content in output: " + stream.toString())

    assertFalse(stream.toString().contains("Formatting log directory %s".format(unavailableDir1)),
      "Failed to find content in output: " + stream.toString())
    assertTrue(stream.toString().contains(
      "I/O error trying to read log directory %s. Ignoring...".format(unavailableDir1)),
        "Failed to find content in output: " + stream.toString())
  }

  @Test
  def testFormatFailsOnAlreadyFormatted(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir(), TestUtils.tempDir(), TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", s"${availableDirs(0)}")
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties))
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream2 = new ByteArrayOutputStream()
    assertTrue(assertThrows(classOf[FormatterException],
      () => runFormatCommand(stream2, properties)).getMessage.contains(
        "already formatted. Use --ignore-formatted to ignore this directory and format the others"))
  }

  @Test
  def testIgnoreFormatted(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir(), TestUtils.tempDir(), TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", s"${availableDirs(0)}")
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties))
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream2 = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream2, properties, Seq(), true))
  }

  @Test
  def testFormatFailsIfAllDirectoriesAreUnavailable(): Unit = {
    val unavailableDir1 = TestUtils.tempFile()
    val unavailableDir2 = TestUtils.tempFile()
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", s"${unavailableDir1},${unavailableDir2}")
    val stream = new ByteArrayOutputStream()
    assertEquals("No available log directories to format.", assertThrows(classOf[FormatterException],
      () => runFormatCommand(stream, properties)).getMessage)
    assertTrue(stream.toString().contains(
      "I/O error trying to read log directory %s. Ignoring...".format(unavailableDir1)),
        "Failed to find content in output: " + stream.toString())
    assertTrue(stream.toString().contains(
      "I/O error trying to read log directory %s. Ignoring...".format(unavailableDir2)),
        "Failed to find content in output: " + stream.toString())
  }

  @Test
  def testFormatFailsInZkMode(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    properties.setProperty("zookeeper.connect", "localhost:2181")
    val stream = new ByteArrayOutputStream()
    assertEquals("The kafka configuration file appears to be for a legacy cluster. " +
      "Formatting is only supported for clusters in KRaft mode.",
        assertThrows(classOf[TerseFailure],
          () => runFormatCommand(stream, properties)).getMessage)
  }

  @Test
  def testFormatWithReleaseVersion(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties, Seq("--release-version", "3.8-IV0")))
    assertTrue(stream.toString().contains("3.8-IV0"),
      "Failed to find content in output: " + stream.toString())
  }

  @Test
  def testFormatWithReleaseVersionAsFeature(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties, Seq("--feature", "metadata.version=20")))
    assertTrue(stream.toString().contains("4.0-IV0"),
      "Failed to find content in output: " + stream.toString())
  }

  @Test
  def testFormatWithReleaseVersionAndKRaftVersion(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties, Seq(
      "--release-version", "3.7-IV0",
      "--feature", "kraft.version=0")))
    assertTrue(stream.toString().contains("3.7-IV0"),
      "Failed to find content in output: " + stream.toString())
  }

  @Test
  def testFormatWithReleaseVersionDefault(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    properties.setProperty("inter.broker.protocol.version", "3.7")
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties))
    assertTrue(stream.toString().contains("3.7-IV4"),
      "Failed to find content in output: " + stream.toString())
  }

  @Test
  def testFormatWithReleaseVersionDefaultAndReleaseVersion(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    properties.setProperty("inter.broker.protocol.version", "3.7")
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, properties, Seq(
      "--release-version", "3.6-IV0",
      "--feature", "kraft.version=0")))
    assertTrue(stream.toString().contains("3.6-IV0"),
      "Failed to find content in output: " + stream.toString())
  }

  @Test
  def testFormatWithStandaloneFlagOnBrokerFails(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream = new ByteArrayOutputStream()
    val arguments = ListBuffer[String]("--release-version", "3.9-IV0", "--standalone")
    assertEquals("You cannot use --standalone on a broker node.",
      assertThrows(classOf[TerseFailure],
        () => runFormatCommand(stream, properties, arguments.toSeq)).getMessage)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testFormatWithStandaloneFlag(setKraftVersionFeature: Boolean): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultDynamicQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream = new ByteArrayOutputStream()
    val arguments = ListBuffer[String]("--release-version", "3.9-IV0", "--standalone")
    if (setKraftVersionFeature) {
      arguments += "--feature"
      arguments += "kraft.version=1"
    }
    assertEquals(0, runFormatCommand(stream, properties, arguments.toSeq))
    assertTrue(stream.toString().
      contains("Formatting dynamic metadata voter directory %s".format(availableDirs.head)),
        "Failed to find content in output: " + stream.toString())
  }

  @Test
  def testFormatWithStandaloneFlagAndInitialControllersFlagFails(): Unit = {
    val arguments = ListBuffer[String](
      "--release-version", "3.9-IV0",
      "--standalone", "--initial-controllers",
      "0@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ," +
      "1@localhost:8030:aUARLskQTCW4qCZDtS_cwA," +
      "2@localhost:8040:2ggvsS4kQb-fSJ_-zC_Ang")
    assertThrows(classOf[ArgumentParserException], () => StorageTool.parseArguments(arguments.toArray))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testFormatWithInitialControllersFlag(setKraftVersionFeature: Boolean): Unit = {
    val availableDirs = Seq(TestUtils.tempDir())
    val properties = new Properties()
    properties.putAll(defaultDynamicQuorumProperties)
    properties.setProperty("log.dirs", availableDirs.mkString(","))
    val stream = new ByteArrayOutputStream()
    val arguments = ListBuffer[String](
      "--release-version", "3.9-IV0",
      "--initial-controllers",
      "0@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ," +
        "1@localhost:8030:aUARLskQTCW4qCZDtS_cwA," +
        "2@localhost:8040:2ggvsS4kQb-fSJ_-zC_Ang")
    if (setKraftVersionFeature) {
      arguments += "--feature"
      arguments += "kraft.version=1"
    }
    assertEquals(0, runFormatCommand(stream, properties, arguments.toSeq))
    assertTrue(stream.toString().
      contains("Formatting dynamic metadata voter directory %s".format(availableDirs.head)),
      "Failed to find content in output: " + stream.toString())
  }

  private def runVersionMappingCommand(
    stream: ByteArrayOutputStream,
    releaseVersion: String
  ): Int = {
    val tempDir = TestUtils.tempDir()
    try {
      // Prepare the arguments list
      val arguments = ListBuffer[String]("version-mapping")

      // Add the release version argument
      if (releaseVersion != null) {
        arguments += "--release-version"
        arguments += releaseVersion
      }

      // Execute the StorageTool with the arguments
      StorageTool.execute(arguments.toArray, new PrintStream(stream))

    } finally {
      Utils.delete(tempDir)
    }
  }

  @Test
  def testVersionMappingWithValidReleaseVersion(): Unit = {
    val stream = new ByteArrayOutputStream()
    // Test with a valid release version
    assertEquals(0, runVersionMappingCommand(stream, "3.3-IV3"))

    val output = stream.toString()
    val metadataVersion = MetadataVersion.IBP_3_3_IV3
    // Check that the metadata version is correctly included in the output
    assertTrue(output.contains(s"metadata.version=${metadataVersion.featureLevel()} (${metadataVersion.version()})"),
      s"Output did not contain expected Metadata Version: $output"
    )

    for (feature <- Features.values()) {
      val featureLevel = feature.defaultValue(metadataVersion)
      assertTrue(output.contains(s"${feature.featureName()}=$featureLevel"),
        s"Output did not contain expected feature mapping: $output"
      )
    }
  }

  @Test
  def testVersionMappingWithNoReleaseVersion(): Unit = {
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)

    val stream = new ByteArrayOutputStream()
    assertEquals(0, runVersionMappingCommand(stream, null))

    val output = stream.toString
    val metadataVersion = MetadataVersion.latestProduction()
    // Check that the metadata version is correctly included in the output
    assertTrue(output.contains(s"metadata.version=${metadataVersion.featureLevel()} (${metadataVersion.version()})"),
      s"Output did not contain expected Metadata Version: $output"
    )

    for (feature <- Features.values()) {
      val featureLevel = feature.defaultValue(metadataVersion)
      assertTrue(output.contains(s"${feature.featureName()}=$featureLevel"),
        s"Output did not contain expected feature mapping: $output"
      )
    }
  }

  @Test
  def testVersionMappingWithInvalidReleaseVersion(): Unit = {
    val properties = new Properties()
    properties.putAll(defaultStaticQuorumProperties)

    val stream = new ByteArrayOutputStream()
    // Test with an invalid release version
    val exception = assertThrows(classOf[TerseFailure], () => {
      runVersionMappingCommand(stream, "2.9-IV2")
    })

    assertEquals("Unsupported release version '2.9-IV2'." +
      " Supported versions are: " + MetadataVersion.MINIMUM_BOOTSTRAP_VERSION.version +
      " to " + MetadataVersion.LATEST_PRODUCTION.version, exception.getMessage
    )

    val exception2 = assertThrows(classOf[TerseFailure], () => {
      runVersionMappingCommand(stream, "invalid")
    })

    assertEquals("Unsupported release version 'invalid'." +
      " Supported versions are: " + MetadataVersion.MINIMUM_BOOTSTRAP_VERSION.version +
      " to " + MetadataVersion.LATEST_PRODUCTION.version, exception2.getMessage
    )
  }
}
