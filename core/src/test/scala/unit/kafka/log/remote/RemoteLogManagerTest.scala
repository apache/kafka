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
package kafka.log.remote

import kafka.server._
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.MockTime
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.{AfterEach, Test}

import java.nio.file.{Files, Paths}
import java.util.Properties
import scala.collection.Seq

class RemoteLogManagerTest {

  val clusterId = "test-cluster-id"
  val brokerId = 0
  val topicPartition = new TopicPartition("test-topic", 0)
  val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val logsDir: String = Files.createTempDirectory("kafka-").toString
  val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
    private var epochs: Seq[EpochEntry] = Seq()
    override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
    override def read(): Seq[EpochEntry] = this.epochs
  }
  val cache = new LeaderEpochFileCache(topicPartition, checkpoint)
  val rlmConfig: RemoteLogManagerConfig = createRLMConfig()

  @AfterEach
  def afterEach(): Unit = {
    Utils.delete(Paths.get(logsDir).toFile)
  }

  @Test
  def testRLMConfig(): Unit = {
    val key = "hello"
    val rlmmConfigPrefix = "rlmm.config."
    val props: Properties = new Properties()
    props.put(rlmmConfigPrefix + key, "world")
    props.put("remote.log.metadata.y", "z")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, rlmmConfigPrefix)
    val rlmConfig = createRLMConfig(props)
    val rlmmConfig = rlmConfig.remoteLogMetadataManagerProps()
    assertEquals(props.get(rlmmConfigPrefix + key), rlmmConfig.get(key))
    assertFalse(rlmmConfig.containsKey("remote.log.metadata.y"))
  }

  private def createRLMConfig(props: Properties = new Properties): RemoteLogManagerConfig = {
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    val config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props)
    new RemoteLogManagerConfig(config)
  }
}