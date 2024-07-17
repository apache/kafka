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
import java.nio.file.{Files, Paths}
import java.util
import java.util.{Collections, Properties}
import org.apache.kafka.common.{DirectoryId, KafkaException}
import kafka.server.KafkaConfig
import kafka.utils.Exit
import kafka.utils.TestUtils
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.commons.io.output.NullOutputStream
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.common.{ApiMessageAndVersion, Features, MetadataVersion, TestFeatureVersion}
import org.apache.kafka.common.metadata.{FeatureLevelRecord, UserScramCredentialRecord}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.Mockito

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

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
  def testInfoWithMismatchedSelfManagedKafkaConfig(): Unit = {
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

  @Test
  def testFormatEmptyDirectory(): Unit = {
    val tempDir = TestUtils.tempDir()
    try {
      val metaProperties = new MetaProperties.Builder().
        setVersion(MetaPropertiesVersion.V1).
        setClusterId("XcZZOzUqS4yHOjhMQB6JLQ").
        setNodeId(2).
        build()
      val stream = new ByteArrayOutputStream()
      val bootstrapMetadata = StorageTool.buildBootstrapMetadata(MetadataVersion.latestTesting(), None, "test format command")
      assertEquals(0, StorageTool.
        formatCommand(new PrintStream(stream), Seq(tempDir.toString), metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), ignoreFormatted = false))
      assertTrue(stream.toString().startsWith("Formatting %s".format(tempDir)))

      try assertEquals(1, StorageTool.
        formatCommand(new PrintStream(new ByteArrayOutputStream()), Seq(tempDir.toString), metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), ignoreFormatted = false)) catch {
        case e: TerseFailure => assertEquals(s"Log directory ${tempDir} is already " +
          "formatted. Use --ignore-formatted to ignore this directory and format the " +
          "others.", e.getMessage)
      }

      val stream2 = new ByteArrayOutputStream()
      assertEquals(0, StorageTool.
        formatCommand(new PrintStream(stream2), Seq(tempDir.toString), metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), ignoreFormatted = true))
      assertEquals("All of the log directories are already formatted.%n".format(), stream2.toString())
    } finally Utils.delete(tempDir)
  }

  private def runFormatCommand(stream: ByteArrayOutputStream, directories: Seq[String], ignoreFormatted: Boolean = false): Int = {
    val metaProperties = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId("XcZZOzUqS4yHOjhMQB6JLQ").
      setNodeId(2).
      build()
    val bootstrapMetadata = StorageTool.buildBootstrapMetadata(MetadataVersion.latestTesting(), None, "test format command")
    StorageTool.formatCommand(new PrintStream(stream), directories, metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), ignoreFormatted)
  }

  @Test
  def testFormatSucceedsIfAllDirectoriesAreAvailable(): Unit = {
    val availableDirs = Seq(TestUtils.tempDir(), TestUtils.tempDir(), TestUtils.tempDir()).map(dir => dir.toString)
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, availableDirs))
    val actual = stream.toString().split("\\r?\\n")
    val expect = availableDirs.map("Formatting %s".format(_))
    expect.foreach(dir => {
      assertEquals(1, actual.count(_.startsWith(dir)))
    })
  }

  @Test
  def testFormatSucceedsIfAtLeastOneDirectoryIsAvailable(): Unit = {
    val availableDir1 = TestUtils.tempDir()
    val unavailableDir1 = TestUtils.tempFile()
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, Seq(availableDir1.toString, unavailableDir1.toString)))
    assertTrue(stream.toString().contains("I/O error trying to read log directory %s. Ignoring...".format(unavailableDir1)))
    assertTrue(stream.toString().contains("Formatting %s".format(availableDir1)))
    assertFalse(stream.toString().contains("Formatting %s".format(unavailableDir1)))
  }

  @Test
  def testFormatFailsIfAllDirectoriesAreUnavailable(): Unit = {
    val unavailableDir1 = TestUtils.tempFile()
    val unavailableDir2 = TestUtils.tempFile()
    val stream = new ByteArrayOutputStream()
    assertEquals("No available log directories to format.", assertThrows(classOf[TerseFailure],
      () => runFormatCommand(stream, Seq(unavailableDir1.toString, unavailableDir2.toString))).getMessage)
    assertTrue(stream.toString().contains("I/O error trying to read log directory %s. Ignoring...".format(unavailableDir1)))
    assertTrue(stream.toString().contains("I/O error trying to read log directory %s. Ignoring...".format(unavailableDir2)))
  }

  @Test
  def testFormatSucceedsIfAtLeastOneFormattedDirectoryIsAvailable(): Unit = {
    val availableDir1 = TestUtils.tempDir()
    val stream = new ByteArrayOutputStream()
    assertEquals(0, runFormatCommand(stream, Seq(availableDir1.toString)))

    val stream2 = new ByteArrayOutputStream()
    val unavailableDir1 = TestUtils.tempFile()
    assertEquals(0, runFormatCommand(stream2, Seq(availableDir1.toString, unavailableDir1.toString), ignoreFormatted = true))
  }

  @Test
  def testFormatWithInvalidClusterId(): Unit = {
    val config = new KafkaConfig(newSelfManagedProperties())
    assertEquals("Cluster ID string invalid does not appear to be a valid UUID: " +
      "Input string `invalid` decoded as 5 bytes, which is not equal to the expected " +
        "16 bytes of a base64-encoded UUID", assertThrows(classOf[TerseFailure],
          () => StorageTool.buildMetadataProperties("invalid", config)).getMessage)
  }

  @Test
  def testDefaultMetadataVersion(): Unit = {
    val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"))
    val mv = StorageTool.getMetadataVersion(namespace, Map.empty, defaultVersionString = None)
    assertEquals(MetadataVersion.LATEST_PRODUCTION.featureLevel(), mv.featureLevel(),
      "Expected the default metadata.version to be the latest production version")
  }

  @Test
  def testConfiguredMetadataVersion(): Unit = {
    val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"))
    val mv = StorageTool.getMetadataVersion(namespace, Map.empty, defaultVersionString = Some(MetadataVersion.IBP_3_3_IV2.toString))
    assertEquals(MetadataVersion.IBP_3_3_IV2.featureLevel(), mv.featureLevel(),
      "Expected the default metadata.version to be 3.3-IV2")
  }

  @Test
  def testSettingFeatureAndReleaseVersionFails(): Unit = {
    val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ",
      "--release-version", "3.0-IV1", "--feature", "metadata.version=4"))
    assertThrows(classOf[IllegalArgumentException], () => StorageTool.getMetadataVersion(namespace, parseFeatures(namespace), defaultVersionString = None))
  }

  @Test
  def testParseFeatures(): Unit = {
    def parseAddFeatures(strings: String*): Map[String, java.lang.Short] = {
      var args = mutable.Seq("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ")
      args ++= strings
      val namespace = StorageTool.parseArguments(args.toArray)
      parseFeatures(namespace)
    }

    assertThrows(classOf[RuntimeException], () => parseAddFeatures("--feature", "blah"))
    assertThrows(classOf[RuntimeException], () => parseAddFeatures("--feature", "blah=blah"))

    // Test with no features
    assertEquals(Map(), parseAddFeatures())

    // Test with one feature
    val testFeatureLevel = 1
    val testArgument = TestFeatureVersion.FEATURE_NAME + "=" + testFeatureLevel.toString
    val expectedMap = Map(TestFeatureVersion.FEATURE_NAME -> testFeatureLevel.toShort)
    assertEquals(expectedMap, parseAddFeatures("--feature", testArgument))

    // Test with two features
    val metadataFeatureLevel = 5
    val metadataArgument = MetadataVersion.FEATURE_NAME + "=" + metadataFeatureLevel.toString
    val expectedMap2 = expectedMap ++ Map (MetadataVersion.FEATURE_NAME -> metadataFeatureLevel.toShort)
    assertEquals(expectedMap2, parseAddFeatures("--feature", testArgument, "--feature", metadataArgument))
  }

  private def parseFeatures(namespace: Namespace): Map[String, java.lang.Short] = {
    val specifiedFeatures: util.List[String] = namespace.getList("feature")
    StorageTool.featureNamesAndLevels(Option(specifiedFeatures).getOrElse(Collections.emptyList).asScala.toList)
  }

  @Test
  def testMetadataVersionFlags(): Unit = {
    def parseMetadataVersion(strings: String*): MetadataVersion = {
      var args = mutable.Seq("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ")
      args ++= strings
      val namespace = StorageTool.parseArguments(args.toArray)
      StorageTool.getMetadataVersion(namespace, Map.empty, defaultVersionString = None)
    }

    var mv = parseMetadataVersion("--release-version", "3.0")
    assertEquals("3.0", mv.shortVersion())

    mv = parseMetadataVersion("--release-version", "3.0-IV1")
    assertEquals(MetadataVersion.IBP_3_0_IV1, mv)

    assertThrows(classOf[IllegalArgumentException], () => parseMetadataVersion("--release-version", "0.0"))
  }

  private def generateRecord(featureName: String, level: Short): ApiMessageAndVersion = {
    new ApiMessageAndVersion(new FeatureLevelRecord().
      setName(featureName).
      setFeatureLevel(level), 0.toShort)
  }

  @ParameterizedTest
  @EnumSource(classOf[TestFeatureVersion])
  def testFeatureFlag(testFeatureVersion: TestFeatureVersion): Unit = {
    val featureLevel = testFeatureVersion.featureLevel
    if (featureLevel <= Features.TEST_VERSION.defaultValue(MetadataVersion.LATEST_PRODUCTION)) {
      val records = new ArrayBuffer[ApiMessageAndVersion]()
      StorageTool.generateFeatureRecords(
        records,
        MetadataVersion.LATEST_PRODUCTION,
        Map(TestFeatureVersion.FEATURE_NAME -> featureLevel),
        allFeatures,
        false,
        false
      )
      if (featureLevel > 0) {
        assertEquals(List(generateRecord(TestFeatureVersion.FEATURE_NAME, featureLevel)), records)
      }
    }
  }

  @ParameterizedTest
  @EnumSource(classOf[MetadataVersion])
  def testVersionDefault(metadataVersion: MetadataVersion): Unit = {
    val records = new ArrayBuffer[ApiMessageAndVersion]()
    StorageTool.generateFeatureRecords(
      records,
      metadataVersion,
      Map.empty,
      allFeatures,
      true,
      true
    )

    val featureLevel = Features.TEST_VERSION.defaultValue(metadataVersion)
    if (featureLevel > 0) {
      assertEquals(List(generateRecord(TestFeatureVersion.FEATURE_NAME, featureLevel)), records)
    }
  }
  @Test
  def testVersionDefaultNoArgs(): Unit = {
    val records = new ArrayBuffer[ApiMessageAndVersion]()
    StorageTool.generateFeatureRecords(
      records,
      MetadataVersion.LATEST_PRODUCTION,
      Map.empty,
      allFeatures,
      false,
      false
    )

    assertEquals(List(generateRecord(TestFeatureVersion.FEATURE_NAME, Features.TEST_VERSION.defaultValue(MetadataVersion.LATEST_PRODUCTION))), records)
  }


  @Test
  def testFeatureDependency(): Unit = {
    val featureLevel = 1.toShort
    assertThrows(classOf[TerseFailure], () => StorageTool.generateFeatureRecords(
      new ArrayBuffer[ApiMessageAndVersion](),
      MetadataVersion.IBP_2_8_IV1,
      Map(TestFeatureVersion.FEATURE_NAME -> featureLevel),
      allFeatures,
      false,
      false
    ))
  }

  @Test
  def testLatestFeaturesWithOldMetadataVersion(): Unit = {
    val records = new ArrayBuffer[ApiMessageAndVersion]()
    StorageTool.generateFeatureRecords(
      records,
      MetadataVersion.IBP_3_3_IV0,
      Map.empty,
      allFeatures,
      false,
      false
    )

    assertEquals(List(generateRecord(TestFeatureVersion.FEATURE_NAME, Features.TEST_VERSION.defaultValue(MetadataVersion.LATEST_PRODUCTION))), records)
  }

  @Test
  def testFeatureInvalidFlag(): Unit = {
    val featureLevel = 99.toShort
    assertThrows(classOf[IllegalArgumentException], () => StorageTool.generateFeatureRecords(
      new ArrayBuffer[ApiMessageAndVersion](),
      MetadataVersion.LATEST_PRODUCTION,
      Map(TestFeatureVersion.FEATURE_NAME -> featureLevel),
      allFeatures,
      false,
      false
    ))
  }

  @Test
  def testUnstableFeatureThrowsError(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => StorageTool.generateFeatureRecords(
      new ArrayBuffer[ApiMessageAndVersion](),
      MetadataVersion.LATEST_PRODUCTION,
      Map(TestFeatureVersion.FEATURE_NAME -> Features.TEST_VERSION.latestTesting),
      allFeatures,
      false,
      false
    ))
  }

  @Test
  def testAddScram():Unit = {
    def parseAddScram(strings: String*): Option[ArrayBuffer[UserScramCredentialRecord]] = {
      var args = mutable.Seq("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ")
      args ++= strings
      val namespace = StorageTool.parseArguments(args.toArray)
      StorageTool.getUserScramCredentialRecords(namespace)
    }

    var scramRecords = parseAddScram()
    assertEquals(None, scramRecords)

    // Validate we can add multiple SCRAM creds.
    scramRecords = parseAddScram("-S",
    "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]",
    "-S",
    "SCRAM-SHA-256=[name=george,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]")
    
    assertEquals(2, scramRecords.get.size)

    // Require name subfield.
    try assertEquals(1, parseAddScram("-S", 
      "SCRAM-SHA-256=[salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]")) catch {
      case e: TerseFailure => assertEquals(s"You must supply 'name' to add-scram", e.getMessage)
    }

    // Require password xor saltedpassword
    try assertEquals(1, parseAddScram("-S", 
      "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]"))
    catch {
      case e: TerseFailure => assertEquals(s"You must only supply one of 'password' or 'saltedpassword' to add-scram", e.getMessage)
    }

    try assertEquals(1, parseAddScram("-S", 
      "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",iterations=8192]"))
    catch {
      case e: TerseFailure => assertEquals(s"You must supply one of 'password' or 'saltedpassword' to add-scram", e.getMessage)
    }

    // Validate salt is required with saltedpassword
    try assertEquals(1, parseAddScram("-S", 
      "SCRAM-SHA-256=[name=alice,saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]"))
    catch {
      case e: TerseFailure => assertEquals(s"You must supply 'salt' with 'saltedpassword' to add-scram", e.getMessage)
    }

    // Validate salt is optional with password
    assertEquals(1, parseAddScram("-S", "SCRAM-SHA-256=[name=alice,password=alice,iterations=4096]").get.size)

    // Require 4096 <= iterations <= 16384
    try assertEquals(1, parseAddScram("-S", 
      "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=16385]"))
    catch {
      case e: TerseFailure => assertEquals(s"The 'iterations' value must be <= 16384 for add-scram", e.getMessage)
    }

    assertEquals(1, parseAddScram("-S",
      "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=16384]")
      .get.size)

    try assertEquals(1, parseAddScram("-S", 
      "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=4095]"))
    catch {
      case e: TerseFailure => assertEquals(s"The 'iterations' value must be >= 4096 for add-scram", e.getMessage)
    }

    assertEquals(1, parseAddScram("-S",
      "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=4096]")
      .get.size)

    // Validate iterations is optional
    assertEquals(1, parseAddScram("-S", "SCRAM-SHA-256=[name=alice,password=alice]") .get.size)
  }

  class StorageToolTestException(message: String)  extends KafkaException(message) {
  }

  @Test
  def testScramWithBadMetadataVersion(): Unit = {
    var exitString: String = ""
    def exitProcedure(exitStatus: Int, message: Option[String]) : Nothing = {
      exitString = message.getOrElse("")
      throw new StorageToolTestException(exitString)
    }
    Exit.setExitProcedure(exitProcedure)

    val properties = newSelfManagedProperties()
    val propsFile = TestUtils.tempFile()
    val propsStream = Files.newOutputStream(propsFile.toPath)
    properties.store(propsStream, "config.props")
    propsStream.close()

    val args = Array("format", "-c", s"${propsFile.toPath}", "-t", "XcZZOzUqS4yHOjhMQB6JLQ", "--release-version", "3.4", "-S", 
      "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=8192]")

    try {
      assertEquals(1, StorageTool.main(args))
    } catch {
      case e: StorageToolTestException => assertEquals(s"SCRAM is only supported in metadata.version ${MetadataVersion.IBP_3_5_IV2} or later.", exitString)
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test
  def testNoScramWithMetadataVersion(): Unit = {
    var exitString: String = ""
    var exitStatus: Int = 1
    def exitProcedure(status: Int, message: Option[String]) : Nothing = {
      exitStatus = status
      exitString = message.getOrElse("")
      throw new StorageToolTestException(exitString)
    }
    Exit.setExitProcedure(exitProcedure)

    val properties = newSelfManagedProperties()
    val propsFile = TestUtils.tempFile()
    val propsStream = Files.newOutputStream(propsFile.toPath)
    // This test does format the directory specified so use a tempdir
    properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, TestUtils.tempDir().toString)
    properties.store(propsStream, "config.props")
    propsStream.close()

    val args = Array("format", "-c", s"${propsFile.toPath}", "-t", "XcZZOzUqS4yHOjhMQB6JLQ", "--release-version", "3.4")

    try {
      StorageTool.main(args)
    } catch {
      case e: StorageToolTestException => assertEquals("", exitString)
      assertEquals(0, exitStatus)
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test
  def testDirUuidGeneration(): Unit = {
    val tempDir = TestUtils.tempDir()
    try {
      val metaProperties = new MetaProperties.Builder().
        setClusterId("XcZZOzUqS4yHOjhMQB6JLQ").
        setNodeId(2).
        build()
      val bootstrapMetadata = StorageTool.
        buildBootstrapMetadata(MetadataVersion.latestTesting(), None, "test format command")
      assertEquals(0, StorageTool.
        formatCommand(new PrintStream(NullOutputStream.NULL_OUTPUT_STREAM), Seq(tempDir.toString), metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), ignoreFormatted = false))

      val metaPropertiesFile = Paths.get(tempDir.toURI).resolve(MetaPropertiesEnsemble.META_PROPERTIES_NAME).toFile
      assertTrue(metaPropertiesFile.exists())
      val metaProps = new MetaProperties.Builder(
        PropertiesUtils.readPropertiesFile(metaPropertiesFile.getAbsolutePath())).
          build()
      assertTrue(metaProps.directoryId().isPresent())
      assertFalse(DirectoryId.reserved(metaProps.directoryId().get()))
    } finally Utils.delete(tempDir)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testFormattingUnstableMetadataVersionBlocked(enableUnstable: Boolean): Unit = {
    var exitString: String = ""
    var exitStatus: Int = 1
    def exitProcedure(status: Int, message: Option[String]) : Nothing = {
      exitStatus = status
      exitString = message.getOrElse("")
      throw new StorageToolTestException(exitString)
    }
    Exit.setExitProcedure(exitProcedure)
    val properties = newSelfManagedProperties()
    val propsFile = TestUtils.tempFile()
    val propsStream = Files.newOutputStream(propsFile.toPath)
    try {
      properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, TestUtils.tempDir().toString)
      properties.setProperty(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, enableUnstable.toString)
      properties.store(propsStream, "config.props")
    } finally {
      propsStream.close()
    }
    val args = Array("format", "-c", s"${propsFile.toPath}",
      "-t", "XcZZOzUqS4yHOjhMQB6JLQ",
      "--release-version", MetadataVersion.latestTesting().toString)
    try {
      StorageTool.main(args)
    } catch {
      case _: StorageToolTestException =>
    } finally {
      Exit.resetExitProcedure()
    }
    if (enableUnstable) {
      assertEquals("", exitString)
      assertEquals(0, exitStatus)
    } else {
      assertEquals(s"The metadata.version ${MetadataVersion.latestTesting().toString} is not ready for " +
        "production use yet.", exitString)
      assertEquals(1, exitStatus)
    }
  }

  @Test
  def testFormatValidatesConfigForMetadataVersion(): Unit = {
    val config = Mockito.spy(new KafkaConfig(TestUtils.createBrokerConfig(10, null)))
    val args = Array("format",
      "-c", "dummy.properties",
      "-t", "XcZZOzUqS4yHOjhMQB6JLQ",
      "--release-version", MetadataVersion.LATEST_PRODUCTION.toString)
    val exitCode = StorageTool.runFormatCommand(StorageTool.parseArguments(args), config)
    Mockito.verify(config, Mockito.times(1)).validateWithMetadataVersion(MetadataVersion.LATEST_PRODUCTION)
    assertEquals(0, exitCode)
  }

  @Test
  def testJbodSupportValidation(): Unit = {
    def formatWith(logDirCount: Int, metadataVersion: MetadataVersion): Integer = {
      val properties = TestUtils.createBrokerConfig(10, null, logDirCount = logDirCount)
      properties.remove(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG)
      val configFile = TestUtils.tempPropertiesFile(properties.asScala.toMap).toPath.toString
      StorageTool.execute(Array("format",
        "-c", configFile,
        "-t", "XcZZOzUqS4yHOjhMQB6JLQ",
        "--release-version", metadataVersion.toString))
    }

    assertEquals(0, formatWith(1, MetadataVersion.IBP_3_6_IV2))
    assertEquals("Invalid configuration for metadata version: " +
      "requirement failed: Multiple log directories (aka JBOD) are not supported in the current MetadataVersion 3.6-IV2. Need 3.7-IV2 or higher",
      assertThrows(classOf[TerseFailure], () => formatWith(2, MetadataVersion.IBP_3_6_IV2)).getMessage)
    assertEquals(0, formatWith(1, MetadataVersion.IBP_3_7_IV2))
    assertEquals(0, formatWith(2, MetadataVersion.IBP_3_7_IV2))
  }
}

