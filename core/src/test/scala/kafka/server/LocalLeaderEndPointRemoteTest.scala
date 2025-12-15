/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.log.LogManager
import kafka.utils.TestUtils
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.storage.internals.log.LogConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.io.File
import java.util.Properties
import scala.collection.Map
import scala.jdk.CollectionConverters._

class LocalLeaderEndPointRemoteTest extends LocalLeaderEndPointTestBase {

  override def createLogManager(config: KafkaConfig): LogManager = {
    val logProps = new Properties()
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    // Keep cleanup.policy=delete (default), not compact, so remote storage is allowed
    val defaultLogConfig = LogConfig.fromProps(Map.empty[String, Object].asJava, logProps)

    TestUtils.createLogManager(
      config.logDirs.asScala.map(new File(_)),
      defaultConfig = defaultLogConfig,
      remoteStorageSystemEnable = true
    )
  }

  @Test
  def testEarliestPendingUploadOffsetWhenNoSegmentsUploaded(): Unit = {
    // Append some records; no remote upload happened yet
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    val expected = endPoint.fetchEarliestOffset(topicPartition, 0)
    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(expected, result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenLocalStartGreaterThanStart(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    // Bump epoch and advance local log start offset without changing log start offset
    bumpLeaderEpoch()
    replicaManager.logManager.getLog(topicPartition).foreach(_.updateLocalLogStartOffset(3))

    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 1)
    assertEquals(new OffsetAndEpoch(-1L, -1), result)
  }

  @Test
  def testEarliestPendingUploadOffsetWhenHighestRemoteOffsetKnown(): Unit = {
    appendRecords(replicaManager, topicIdPartition, records)
      .onFire(response => assertEquals(Errors.NONE, response.error))

    // Highest remote is 1 => earliest pending should be max(1+1, logStart)
    val log = replicaManager.getPartitionOrException(topicPartition).localLogOrException
    log.updateHighestOffsetInRemoteStorage(1)

    val expectedOffset = Math.max(2L, log.logStartOffset())
    val epoch = log.leaderEpochCache().epochForOffset(expectedOffset).orElse(0)

    val result = endPoint.fetchEarliestPendingUploadOffset(topicPartition, 0)
    assertEquals(new OffsetAndEpoch(expectedOffset, epoch), result)
  }
}
