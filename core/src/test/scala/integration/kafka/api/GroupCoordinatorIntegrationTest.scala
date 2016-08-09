/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package integration.kafka.api

import kafka.common.{Topic, TopicAndPartition}
import kafka.integration.KafkaServerTestHarness
import kafka.log.Log
import kafka.message.GZIPCompressionCodec
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._
import java.util.Properties

class GroupCoordinatorIntegrationTest extends KafkaServerTestHarness {
  val offsetsTopicCompressionCodec = GZIPCompressionCodec
  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  overridingProps.put(KafkaConfig.OffsetsTopicCompressionCodecProp, offsetsTopicCompressionCodec.codec.toString)

  override def generateConfigs = TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map {
    KafkaConfig.fromProps(_, overridingProps)
  }

  @Test
  def testGroupCoordinatorPropagatesOfffsetsTopicCompressionCodec() {
    val consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers),
                                               securityProtocol = SecurityProtocol.PLAINTEXT)
    val offsetMap = Map(
      new TopicPartition(Topic.GroupMetadataTopicName, 0) -> new OffsetAndMetadata(10, "")
    ).asJava
    consumer.commitSync(offsetMap)
    val logManager = servers.head.getLogManager

    def getGroupMetadataLogOpt: Option[Log] =
      logManager.getLog(TopicAndPartition(Topic.GroupMetadataTopicName, 0))

    TestUtils.waitUntilTrue(() => getGroupMetadataLogOpt.exists(_.logSegments.exists(_.log.nonEmpty)),
                            "Commit message not appended in time")

    val logSegments = getGroupMetadataLogOpt.get.logSegments
    val incorrectCompressionCodecs = logSegments.flatMap(_.log.map(_.message.compressionCodec)).filter(_ != offsetsTopicCompressionCodec)
    assertEquals("Incorrect compression codecs should be empty", Seq.empty, incorrectCompressionCodecs)

    consumer.close()
  }
}
