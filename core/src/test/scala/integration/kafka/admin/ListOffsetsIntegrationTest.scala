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

package kafka.admin

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class ListOffsetsIntegrationTest extends KafkaServerTestHarness {

  val topicName = "foo"
  var adminClient: Admin = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(topicName, 1, 1.toShort)
    produceMessages()
    adminClient = Admin.create(Map[String, Object](
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers()
    ).asJava)
  }

  @AfterEach
  override def tearDown(): Unit = {
    Utils.closeQuietly(adminClient, "ListOffsetsAdminClient")
    super.tearDown()
  }

  @Test
  def testEarliestOffset(): Unit = {
    val earliestOffset = runFetchOffsets(adminClient, OffsetSpec.earliest())
    assertEquals(0, earliestOffset.offset())
  }

  @Test
  def testLatestOffset(): Unit = {
    val latestOffset = runFetchOffsets(adminClient, OffsetSpec.latest())
    assertEquals(3, latestOffset.offset())
  }

  @Test
  def testMaxTimestampOffset(): Unit = {
    val maxTimestampOffset = runFetchOffsets(adminClient, OffsetSpec.maxTimestamp())
    assertEquals(1, maxTimestampOffset.offset())
  }

  private def runFetchOffsets(adminClient: Admin,
                              offsetSpec: OffsetSpec): ListOffsetsResult.ListOffsetsResultInfo = {
    val tp = new TopicPartition(topicName, 0)
    adminClient.listOffsets(Map(
      tp -> offsetSpec
    ).asJava, new ListOffsetsOptions()).all().get().get(tp)
  }

  def produceMessages(): Unit = {
    val records = Seq(
      new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 100L,
        null, new Array[Byte](10000)),
      new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 999L,
        null, new Array[Byte](10000)),
      new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, 200L,
        null, new Array[Byte](10000)),
    )
    TestUtils.produceMessages(servers, records, -1)
  }

  def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(1, zkConnect).map(KafkaConfig.fromProps)
}

