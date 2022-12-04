/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.tools.TerseFailure
import kafka.utils.{TestInfoUtils, TestUtils}
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType.{SAFE_DOWNGRADE, UNSAFE_DOWNGRADE}
import org.apache.kafka.clients.admin.MockAdminClient
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_3_3_IV0, IBP_3_3_IV1, IBP_3_3_IV2, IBP_3_3_IV3, IBP_3_4_IV1}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{ByteArrayOutputStream, PrintStream}
import java.{lang, util}
import java.util.Collections.{emptyMap, singletonMap}
import scala.jdk.CollectionConverters._

case class FeatureCommandTestEnv(admin: MockAdminClient = null) extends AutoCloseable {
  val stream = new ByteArrayOutputStream()
  val out = new PrintStream(stream)

  override def close(): Unit = {
    Utils.closeAll(stream, out)
    Utils.closeQuietly(admin, "admin")
  }

  def outputWithoutEpoch(): String = {
    val lines = stream.toString.split(String.format("%n"))
    lines.map { line =>
      val pos = line.indexOf("Epoch: ")
      if (pos > 0) {
        line.substring(0, pos)
      } else {
        line
      }
    }.mkString(String.format("%n"))
  }
}

class FeatureCommandTest extends IntegrationTestHarness {
  override def brokerCount: Int = 1

  override protected def metadataVersion: MetadataVersion = IBP_3_3_IV1

  serverConfig.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, metadataVersion.toString)

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testDescribeWithZk(quorum: String): Unit = {
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(0, FeatureCommand.mainNoExit(
        Array("--bootstrap-server", bootstrapServers(), "describe"), env.out))
      assertEquals("", env.outputWithoutEpoch())
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testDescribeWithKRaft(quorum: String): Unit = {
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(0, FeatureCommand.mainNoExit(
        Array("--bootstrap-server", bootstrapServers(), "describe"), env.out))
      assertEquals(String.format(
        "Feature: metadata.version\tSupportedMinVersion: 3.0-IV1\t" +
          "SupportedMaxVersion: 3.4-IV1\tFinalizedVersionLevel: 3.3-IV1\t"),
            env.outputWithoutEpoch())
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testUpgradeMetadataVersionWithZk(quorum: String): Unit = {
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(1, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "upgrade", "--metadata", "3.3-IV2"), env.out))
      assertEquals("Could not upgrade metadata.version to 6. Could not apply finalized feature " +
        "update because the provided feature is not supported.", env.outputWithoutEpoch())
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testUpgradeMetadataVersionWithKraft(quorum: String): Unit = {
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(0, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "upgrade", "--feature", "metadata.version=5"), env.out))
      assertEquals("metadata.version was upgraded to 5.", env.outputWithoutEpoch())
    }
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(0, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "upgrade", "--metadata", "3.3-IV2"), env.out))
      assertEquals("metadata.version was upgraded to 6.", env.outputWithoutEpoch())
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testDowngradeMetadataVersionWithZk(quorum: String): Unit = {
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(1, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "disable", "--feature", "metadata.version"), env.out))
      assertEquals("Could not disable metadata.version. Can not delete non-existing finalized feature.",
        env.outputWithoutEpoch())
    }
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(1, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "downgrade", "--metadata", "3.3-IV0"), env.out))
      assertEquals("Could not downgrade metadata.version to 4. Could not apply finalized feature " +
        "update because the provided feature is not supported.", env.outputWithoutEpoch())
    }
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(1, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "downgrade", "--unsafe", "--metadata", "3.3-IV0"), env.out))
      assertEquals("Could not downgrade metadata.version to 4. Could not apply finalized feature " +
        "update because the provided feature is not supported.", env.outputWithoutEpoch())
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testDowngradeMetadataVersionWithKRaft(quorum: String): Unit = {
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(1, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "disable", "--feature", "metadata.version"), env.out))
      assertEquals("Could not disable metadata.version. Invalid update version 0 for feature " +
        "metadata.version. Local controller 1000 only supports versions 1-9", env.outputWithoutEpoch())
    }
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(1, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "downgrade", "--metadata", "3.3-IV0"), env.out))
      assertEquals("Could not downgrade metadata.version to 4. Invalid metadata.version 4. " +
        "Refusing to perform the requested downgrade because it might delete metadata information. " +
        "Retry using UNSAFE_DOWNGRADE if you want to force the downgrade to proceed.", env.outputWithoutEpoch())
    }
    TestUtils.resource(FeatureCommandTestEnv()) { env =>
      assertEquals(1, FeatureCommand.mainNoExit(Array("--bootstrap-server", bootstrapServers(),
        "downgrade", "--unsafe", "--metadata", "3.3-IV0"), env.out))
      assertEquals("Could not downgrade metadata.version to 4. Invalid metadata.version 4. " +
        "Unsafe metadata downgrade is not supported in this version.", env.outputWithoutEpoch())
    }
  }
}

class FeatureCommandUnitTest {
  @Test
  def testLevelToString(): Unit = {
    assertEquals("5", FeatureCommand.levelToString("foo.bar", 5.toShort))
    assertEquals("3.3-IV0",
      FeatureCommand.levelToString(MetadataVersion.FEATURE_NAME, IBP_3_3_IV0.featureLevel()))
  }

  @Test
  def testMetadataVersionsToString(): Unit = {
    assertEquals("3.3-IV0, 3.3-IV1, 3.3-IV2, 3.3-IV3, 3.4-IV0, 3.4-IV1",
      FeatureCommand.metadataVersionsToString(IBP_3_3_IV0, IBP_3_4_IV1))
  }

  @Test
  def testdowngradeType(): Unit = {
    assertEquals(SAFE_DOWNGRADE, FeatureCommand.downgradeType(
      new Namespace(singletonMap("unsafe", java.lang.Boolean.valueOf(false)))))
    assertEquals(UNSAFE_DOWNGRADE, FeatureCommand.downgradeType(
      new Namespace(singletonMap("unsafe", java.lang.Boolean.valueOf(true)))))
    assertEquals(SAFE_DOWNGRADE, FeatureCommand.downgradeType(new Namespace(emptyMap())))
  }

  @Test
  def testParseNameAndLevel(): Unit = {
    assertEquals(("foo.bar", 5.toShort), FeatureCommand.parseNameAndLevel("foo.bar=5"))
    assertEquals(("quux", 0.toShort), FeatureCommand.parseNameAndLevel(" quux=0"))
    assertEquals("Can't parse feature=level string baaz: equals sign not found.",
      assertThrows(classOf[TerseFailure],
        () => FeatureCommand.parseNameAndLevel("baaz")).getMessage)
    assertEquals("Can't parse feature=level string w=tf: unable to parse tf as a short.",
      assertThrows(classOf[TerseFailure],
        () => FeatureCommand.parseNameAndLevel("w=tf")).getMessage)
  }

  def buildAdminClient1(): MockAdminClient = {
    new MockAdminClient.Builder().
      minSupportedFeatureLevels(Map(
        MetadataVersion.FEATURE_NAME -> lang.Short.valueOf(IBP_3_3_IV0.featureLevel()),
        "foo.bar" -> lang.Short.valueOf(0.toShort)
      ).asJava).
      featureLevels(Map(
        MetadataVersion.FEATURE_NAME -> lang.Short.valueOf(IBP_3_3_IV2.featureLevel()),
        "foo.bar" -> lang.Short.valueOf(5.toShort)
      ).asJava).
      maxSupportedFeatureLevels(Map(
        MetadataVersion.FEATURE_NAME -> lang.Short.valueOf(IBP_3_3_IV3.featureLevel()),
        "foo.bar" -> lang.Short.valueOf(10.toShort)
      ).asJava).
      build()
  }

  @Test
  def testHandleDescribe(): Unit = {
    TestUtils.resource(FeatureCommandTestEnv(buildAdminClient1())) { env =>
      FeatureCommand.handleDescribe(env.out, env.admin)
      assertEquals(String.format(
        "Feature: foo.bar\tSupportedMinVersion: 0\tSupportedMaxVersion: 10\tFinalizedVersionLevel: 5\tEpoch: 123%n" +
        "Feature: metadata.version\tSupportedMinVersion: 3.3-IV0\tSupportedMaxVersion: 3.3-IV3\tFinalizedVersionLevel: 3.3-IV2\tEpoch: 123%n"),
        env.stream.toString)
    }
  }

  @Test
  def testHandleUpgrade(): Unit = {
    TestUtils.resource(FeatureCommandTestEnv(buildAdminClient1())) { env =>
      assertEquals("1 out of 2 operation(s) failed.",
        assertThrows(classOf[TerseFailure], () =>
          FeatureCommand.handleUpgrade(env.out, new Namespace(Map(
            "metadata" -> "3.3-IV1",
            "feature" -> util.Arrays.asList("foo.bar=6")
          ).asJava), env.admin)).getMessage)
      assertEquals(String.format(
        "foo.bar was upgraded to 6.%n" +
        "Could not upgrade metadata.version to 5. Can't upgrade to lower version.%n"),
        env.stream.toString)
    }
  }

  @Test
  def testHandleUpgradeDryRun(): Unit = {
    TestUtils.resource(FeatureCommandTestEnv(buildAdminClient1())) { env =>
      assertEquals("1 out of 2 operation(s) failed.",
        assertThrows(classOf[TerseFailure], () =>
          FeatureCommand.handleUpgrade(env.out, new Namespace(Map(
            "metadata" -> "3.3-IV1",
            "feature" -> util.Arrays.asList("foo.bar=6"),
            "dry-run" -> java.lang.Boolean.valueOf(true)
          ).asJava), env.admin)).getMessage)
      assertEquals(String.format(
        "foo.bar can be upgraded to 6.%n" +
        "Can not upgrade metadata.version to 5. Can't upgrade to lower version.%n"),
        env.stream.toString)
    }
  }

  @Test
  def testHandleDowngrade(): Unit = {
    TestUtils.resource(FeatureCommandTestEnv(buildAdminClient1())) { env =>
      assertEquals("1 out of 2 operation(s) failed.",
        assertThrows(classOf[TerseFailure], () =>
          FeatureCommand.handleDowngrade(env.out, new Namespace(Map(
            "metadata" -> "3.3-IV3",
            "feature" -> util.Arrays.asList("foo.bar=1")
          ).asJava), env.admin)).getMessage)
      assertEquals(String.format(
        "foo.bar was downgraded to 1.%n" +
        "Could not downgrade metadata.version to 7. Can't downgrade to newer version.%n"),
        env.stream.toString)
    }
  }

  @Test
  def testHandleDowngradeDryRun(): Unit = {
    TestUtils.resource(FeatureCommandTestEnv(buildAdminClient1())) { env =>
      assertEquals("1 out of 2 operation(s) failed.",
        assertThrows(classOf[TerseFailure], () =>
          FeatureCommand.handleDowngrade(env.out, new Namespace(Map(
            "metadata" -> "3.3-IV3",
            "feature" -> util.Arrays.asList("foo.bar=1"),
            "dry-run" -> java.lang.Boolean.valueOf(true)
          ).asJava), env.admin)).getMessage)
      assertEquals(String.format(
        "foo.bar can be downgraded to 1.%n" +
        "Can not downgrade metadata.version to 7. Can't downgrade to newer version.%n"),
        env.stream.toString)
    }
  }

  @Test
  def testHandleDisable(): Unit = {
    TestUtils.resource(FeatureCommandTestEnv(buildAdminClient1())) { env =>
      assertEquals("1 out of 3 operation(s) failed.",
        assertThrows(classOf[TerseFailure], () =>
          FeatureCommand.handleDisable(env.out, new Namespace(Map[String, AnyRef](
            "feature" -> util.Arrays.asList("foo.bar", "metadata.version", "quux")
          ).asJava), env.admin)).getMessage)
      assertEquals(String.format(
        "foo.bar was disabled.%n" +
          "Could not disable metadata.version. Can't downgrade below 4%n" +
          "quux was disabled.%n"),
        env.stream.toString)
    }
  }

  @Test
  def testHandleDisableDryRun(): Unit = {
    TestUtils.resource(FeatureCommandTestEnv(buildAdminClient1())) { env =>
      assertEquals("1 out of 3 operation(s) failed.",
        assertThrows(classOf[TerseFailure], () =>
          FeatureCommand.handleDisable(env.out, new Namespace(Map[String, AnyRef](
            "feature" -> util.Arrays.asList("foo.bar", "metadata.version", "quux"),
            "dry-run" -> java.lang.Boolean.valueOf(true)
          ).asJava), env.admin)).getMessage)
      assertEquals(String.format(
        "foo.bar can be disabled.%n" +
          "Can not disable metadata.version. Can't downgrade below 4%n" +
          "quux can be disabled.%n"),
        env.stream.toString)
    }
  }
}
