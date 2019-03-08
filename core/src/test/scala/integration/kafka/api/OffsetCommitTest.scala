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

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.Test

import kafka.api.{ApiVersion, KAFKA_1_1_IV0}

import scala.collection.JavaConverters._

class OffsetCommitTest extends ZooKeeperTestHarness {
  var server: KafkaServer = _
  val group = "test-group"
  val retentionCheckInterval: Long = 100L

  def StartServer(apiVersion: ApiVersion = ApiVersion.latestVersion): Unit = {
    super.setUp()
    val config = TestUtils.createBrokerConfigs(1, zkConnect).head
    config.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    config.setProperty(KafkaConfig.OffsetsRetentionCheckIntervalMsProp, retentionCheckInterval.toString)
    config.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, apiVersion.toString)
    server = TestUtils.createServer(KafkaConfig.fromProps(config), Time.SYSTEM)
  }

  @Test
  def testFetchCommittedOffsetAfterBrokerBounce() {
    StartServer(KAFKA_1_1_IV0)

    // Create the topic and produce some data
    val topic = "test-topic"
    val tp = new TopicPartition(topic, 0)
    TestUtils.createTopic(zkClient, topic, 1, 1, Seq(server))
    TestUtils.produceMessage(Seq(server), topic, "test1")
    TestUtils.produceMessage(Seq(server), topic, "test2")
    TestUtils.produceMessage(Seq(server), topic, "test3")

    // Create the consumer
    var consumer = TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(Seq(server)))

    // Commit offset
    val offsetMap = Map(tp -> new OffsetAndMetadata(2L, "")).asJava
    consumer.commitSync(offsetMap)
    assertEquals(2L, consumer.committed(tp).offset())
    consumer.close()

    // Bounce the server
    server.shutdown()
    server.startup()

    // Wait until the offset topic retention check passes
    Thread.sleep(retentionCheckInterval * 2)

    // Fetch committed offset again
    consumer = TestUtils.createConsumer(TestUtils.getBrokerListStrFromServers(Seq(server)))
    assertEquals(2L, consumer.committed(tp).offset())

    consumer.close()
    server.shutdown()
  }
}
