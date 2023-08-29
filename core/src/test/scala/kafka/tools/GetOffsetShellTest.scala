/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.util.Properties
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Exit, Logging, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.network.Mode
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}

class GetOffsetShellTest extends KafkaServerTestHarness with Logging {
  private val topicCount = 4
  private val offsetTopicPartitionCount = 4
  protected var admin: Admin = _

  override def generateConfigs: collection.Seq[KafkaConfig] = TestUtils.createBrokerConfigs(1, zkConnectOrNull)
    .map { p =>
      p.put(KafkaConfig.OffsetsTopicPartitionsProp, Int.box(offsetTopicPartitionCount))
      p
    }.map(KafkaConfig.fromProps)

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    Range(1, topicCount + 1).foreach(i => createTopic(topicName(i), i))

    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    // Send X messages to each partition of topicX
    val producer = new KafkaProducer[String, String](props)
    Range(1, topicCount + 1).foreach(i => Range(0, i*i)
      .foreach(msgCount => producer.send(new ProducerRecord[String, String](topicName(i), msgCount % i, null, "val" + msgCount))))
    producer.close()

    admin = TestUtils.createAdminClient(brokers, listenerName,
      TestUtils.securityConfigs(Mode.CLIENT,
        securityProtocol,
        trustStoreFile,
        "adminClient",
        TestUtils.SslCertificateCn,
        clientSaslProperties))

    //TestUtils.createOffsetsTopic(zkClient, servers)
    TestUtils.createOffsetsTopicWithAdmin(admin, brokers)
  }

  @AfterEach
  override def tearDown(): Unit = {
    admin.close()

    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testNoFilterOptions(quorum: String): Unit = {
    val offsets = executeAndParse(Array())
    assertEquals(expectedOffsetsWithInternal(), offsets)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInternalExcluded(quorum: String): Unit = {
    val offsets = executeAndParse(Array("--exclude-internal-topics"))
    assertEquals(expectedTestTopicOffsets(), offsets)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicNameArg(quorum: String): Unit = {
    Range(1, topicCount + 1).foreach(i => {
      val offsets = executeAndParse(Array("--topic", topicName(i)))
      assertEquals(expectedOffsetsForTopic(i), offsets, () => "Offset output did not match for " + topicName(i))
    })
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPatternArg(quorum: String): Unit = {
    val offsets = executeAndParse(Array("--topic", "topic.*"))
    assertEquals(expectedTestTopicOffsets(), offsets)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testPartitionsArg(quorum: String): Unit = {
    val offsets = executeAndParse(Array("--partitions", "0,1"))
    assertEquals(expectedOffsetsWithInternal().filter { case (_, partition, _) => partition <= 1 }, offsets)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPatternArgWithPartitionsArg(quorum: String): Unit = {
    val offsets = executeAndParse(Array("--topic", "topic.*", "--partitions", "0,1"))
    assertEquals(expectedTestTopicOffsets().filter { case (_, partition, _) => partition <= 1 }, offsets)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPartitionsArg(quorum: String): Unit = {
    val offsets = executeAndParse(Array("--topic-partitions", "topic1:0,topic2:1,topic(3|4):2,__.*:3"))
    assertEquals(
      List(
        ("__consumer_offsets", 3, Some(0)),
        ("topic1", 0, Some(1)),
        ("topic2", 1, Some(2)),
        ("topic3", 2, Some(3)),
        ("topic4", 2, Some(4))
      ),
      offsets
    )
  }

  @ParameterizedTest
  @CsvSource(value = Array("-1,zk", "-1,kraft", "latest,zk", "latest,kraft"))
  def testGetLatestOffsets(time: String, quorum: String): Unit = {
    val offsets = executeAndParse(Array("--topic-partitions", "topic.*:0", "--time", time))
    assertEquals(
      List(
        ("topic1", 0, Some(1)),
        ("topic2", 0, Some(2)),
        ("topic3", 0, Some(3)),
        ("topic4", 0, Some(4))
      ),
      offsets
    )
  }

  @ParameterizedTest
  @CsvSource(value = Array("-2,zk", "-2,kraft", "earliest,zk", "earliest,kraft"))
  def testGetEarliestOffsets(time: String, quorum: String): Unit = {
    val offsets = executeAndParse(Array("--topic-partitions", "topic.*:0", "--time", time))
    assertEquals(
      List(
        ("topic1", 0, Some(0)),
        ("topic2", 0, Some(0)),
        ("topic3", 0, Some(0)),
        ("topic4", 0, Some(0))
      ),
      offsets
    )
  }

  @ParameterizedTest
  @CsvSource(value = Array("-3,zk", "-3,kraft", "max-timestamp,zk", "max-timestamp,kraft"))
  def testGetOffsetsByMaxTimestamp(time: String, quorum: String): Unit = {
    val offsets = executeAndParse(Array("--topic-partitions", "topic.*", "--time", time))
    offsets.foreach { case (topic, _, timestampOpt) =>
      // We can't know the exact offsets with max timestamp
      assertTrue(timestampOpt.get >= 0 && timestampOpt.get <= topic.replace("topic", "").toInt)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testGetOffsetsByTimestamp(quorum: String): Unit = {
    val time = (System.currentTimeMillis() / 2).toString
    val offsets = executeAndParse(Array("--topic-partitions", "topic.*:0", "--time", time))
    assertEquals(
      List(
        ("topic1", 0, Some(0)),
        ("topic2", 0, Some(0)),
        ("topic3", 0, Some(0)),
        ("topic4", 0, Some(0))
      ),
      offsets
    )
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testNoOffsetIfTimestampGreaterThanLatestRecord(quorum: String): Unit = {
    val time = (System.currentTimeMillis() * 2).toString
    val offsets = executeAndParse(Array("--topic-partitions", "topic.*", "--time", time))
    assertEquals(List.empty, offsets)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPartitionsArgWithInternalExcluded(quorum: String): Unit = {
    val offsets = executeAndParse(Array("--topic-partitions",
      "topic1:0,topic2:1,topic(3|4):2,__.*:3", "--exclude-internal-topics"))
    assertEquals(
      List(
        ("topic1", 0, Some(1)),
        ("topic2", 1, Some(2)),
        ("topic3", 2, Some(3)),
        ("topic4", 2, Some(4))
      ),
      offsets
    )
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPartitionsArgWithInternalIncluded(quorum: String): Unit = {
    val offsets = executeAndParse(Array("--topic-partitions", "__.*:0"))
    assertEquals(List(("__consumer_offsets", 0, Some(0))), offsets)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPartitionsNotFoundForNonExistentTopic(quorum: String): Unit = {
    assertExitCodeIsOne(Array("--topic", "some_nonexistent_topic"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPartitionsNotFoundForExcludedInternalTopic(quorum: String): Unit = {
    assertExitCodeIsOne(Array("--topic", "some_nonexistent_topic:*"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPartitionsNotFoundForNonMatchingTopicPartitionPattern(quorum: String): Unit = {
    assertExitCodeIsOne(Array("--topic-partitions", "__consumer_offsets", "--exclude-internal-topics"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPartitionsFlagWithTopicFlagCauseExit(quorum: String): Unit = {
    assertExitCodeIsOne(Array("--topic-partitions", "__consumer_offsets", "--topic", "topic1"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicPartitionsFlagWithPartitionsFlagCauseExit(quorum: String): Unit = {
    assertExitCodeIsOne(Array("--topic-partitions", "__consumer_offsets", "--partitions", "0"))
  }

  private def expectedOffsetsWithInternal(): List[(String, Int, Option[Long])] = {
    Range(0, offsetTopicPartitionCount).map(i => ("__consumer_offsets", i, Some(0L))).toList ++ expectedTestTopicOffsets()
  }

  private def expectedTestTopicOffsets(): List[(String, Int, Option[Long])] = {
    Range(1, topicCount + 1).flatMap(i => expectedOffsetsForTopic(i)).toList
  }

  private def expectedOffsetsForTopic(i: Int): List[(String, Int, Option[Long])] = {
    val name = topicName(i)
    Range(0, i).map(p => (name, p, Some(i.toLong))).toList
  }

  private def topicName(i: Int): String = "topic" + i

  private def assertExitCodeIsOne(args: Array[String]): Unit = {
    var exitStatus: Option[Int] = None
    Exit.setExitProcedure { (status, _) =>
      exitStatus = Some(status)
      throw new RuntimeException
    }

    try {
      GetOffsetShell.main(addBootstrapServer(args))
    } catch {
      case e: RuntimeException =>
    } finally {
      Exit.resetExitProcedure()
    }

    assertEquals(Some(1), exitStatus)
  }

  private def executeAndParse(args: Array[String]): List[(String, Int, Option[Long])] = {
    val output = executeAndGrabOutput(args)
    output.split(System.lineSeparator())
      .map(_.split(":"))
      .filter(_.length >= 2)
      .map { line =>
        val topic = line(0)
        val partition = line(1).toInt
        val timestamp = if (line.length == 2 || line(2).isEmpty) None else Some(line(2).toLong)
        (topic, partition, timestamp)
      }
      .toList
  }

  private def executeAndGrabOutput(args: Array[String]): String = {
    TestUtils.grabConsoleOutput(GetOffsetShell.main(addBootstrapServer(args)))
  }

  private def addBootstrapServer(args: Array[String]): Array[String] = {
    args ++ Array("--bootstrap-server", bootstrapServers())
  }
}


