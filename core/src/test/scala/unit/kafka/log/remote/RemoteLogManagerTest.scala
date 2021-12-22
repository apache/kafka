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

import kafka.server.checkpoints.{LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.server.log.remote.storage._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.util.Properties
import scala.collection.Seq

class RemoteLogManagerTest {

  val topicPartition = new TopicPartition("test-topic", 0)
  val logsDir: String = Files.createTempDirectory("kafka-").toString
  val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
    private var epochs: Seq[EpochEntry] = Seq()
    override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
    override def read(): Seq[EpochEntry] = this.epochs
  }
  val cache = new LeaderEpochFileCache(topicPartition, checkpoint)

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

  @Test
  def testGetLeaderEpochCheckpoint(): Unit = {
    val epochs = Seq(EpochEntry(0, 33), EpochEntry(1, 43), EpochEntry(2, 99), EpochEntry(3, 105))
    epochs.foreach(epochEntry => cache.assign(epochEntry.epoch, epochEntry.startOffset))

    val remoteLogManager = new RemoteLogManager(createRLMConfig(), brokerId = 1, logsDir)

    val maybeCache = Some(cache)
    var actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = -1, endOffset = 200).read()
    assertEquals(epochs.take(4), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = -1, endOffset = 105).read()
    assertEquals(epochs.take(3), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = -1, endOffset = 100).read()
    assertEquals(epochs.take(3), actual)

    actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = 34, endOffset = 200).read()
    assertEquals(Seq(EpochEntry(0, 34)) ++ epochs.slice(1, 4), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = 43, endOffset = 200).read()
    assertEquals(epochs.slice(1, 4), actual)

    actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = 34, endOffset = 100).read()
    assertEquals(Seq(EpochEntry(0, 34)) ++ epochs.slice(1, 3), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = 34, endOffset = 30).read()
    assertTrue(actual.isEmpty)
    actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = 101, endOffset = 101).read()
    assertTrue(actual.isEmpty)
    actual = remoteLogManager.getLeaderEpochCheckpoint(maybeCache, startOffset = 101, endOffset = 102).read()
    assertEquals(Seq(EpochEntry(2, 101)), actual)
  }

  @Test
  def testGetLeaderEpochCheckpointEncoding(): Unit = {
    val epochs = Seq(EpochEntry(0, 33), EpochEntry(1, 43), EpochEntry(2, 99), EpochEntry(3, 105))
    epochs.foreach(epochEntry => cache.assign(epochEntry.epoch, epochEntry.startOffset))

    val remoteLogManager = new RemoteLogManager(createRLMConfig(), brokerId = 1, logsDir)

    val tmpFile = TestUtils.tempFile()
    val checkpoint = remoteLogManager.getLeaderEpochCheckpoint(Some(cache), startOffset = -1, endOffset = 200)
    assertEquals(epochs, checkpoint.read())

    Files.write(tmpFile.toPath, checkpoint.readAsByteBuffer().array())
    assertEquals(epochs, new LeaderEpochCheckpointFile(tmpFile).read())
  }

  private def createRLMConfig(props: Properties = new Properties): RemoteLogManagerConfig = {
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    val config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props)
    new RemoteLogManagerConfig(config)
  }

}