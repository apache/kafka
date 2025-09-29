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

package kafka.server

import java.io.File
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse}
import org.apache.kafka.server.config.ServerLogConfigs
import org.apache.kafka.storage.internals.log.LogFileUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.util.Random

class AlterReplicaLogDirsRequestTest extends BaseRequestTest {
  override val logDirCount = 5
  override val brokerCount = 1

  val topic = "topic"

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, "0")
    properties.put(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, "1000")
  }

  private def findErrorForPartition(response: AlterReplicaLogDirsResponse, tp: TopicPartition): Errors = {
    Errors.forCode(response.data.results.asScala
      .find(x => x.topicName == tp.topic).get.partitions.asScala
      .find(p => p.partitionIndex == tp.partition).get.errorCode)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAlterReplicaLogDirsRequest(quorum: String): Unit = {
    val partitionNum = 5

    // Alter replica dir before topic creation
    val logDir1 = new File(brokers.head.config.logDirs(Random.nextInt(logDirCount))).getAbsolutePath
    val partitionDirs1 = (0 until partitionNum).map(partition => new TopicPartition(topic, partition) -> logDir1).toMap
    val alterReplicaLogDirsResponse1 = sendAlterReplicaLogDirsRequest(partitionDirs1)

    // The response should show error UNKNOWN_TOPIC_OR_PARTITION for all partitions
    (0 until partitionNum).foreach { partition =>
      val tp = new TopicPartition(topic, partition)
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, findErrorForPartition(alterReplicaLogDirsResponse1, tp))
      assertTrue(brokers.head.logManager.getLog(tp).isEmpty)
    }

    createTopic(topic, partitionNum)
    (0 until partitionNum).foreach { partition =>
      assertEquals(logDir1, brokers.head.logManager.getLog(new TopicPartition(topic, partition)).get.dir.getParent)
    }

    // Alter replica dir again after topic creation
    val logDir2 = new File(brokers.head.config.logDirs(Random.nextInt(logDirCount))).getAbsolutePath
    val partitionDirs2 = (0 until partitionNum).map(partition => new TopicPartition(topic, partition) -> logDir2).toMap
    val alterReplicaLogDirsResponse2 = sendAlterReplicaLogDirsRequest(partitionDirs2)
    // The response should succeed for all partitions
    (0 until partitionNum).foreach { partition =>
      val tp = new TopicPartition(topic, partition)
      assertEquals(Errors.NONE, findErrorForPartition(alterReplicaLogDirsResponse2, tp))
      TestUtils.waitUntilTrue(() => {
        logDir2 == brokers.head.logManager.getLog(new TopicPartition(topic, partition)).get.dir.getParent
      }, "timed out waiting for replica movement")
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAlterReplicaLogDirsRequestErrorCode(quorum: String): Unit = {
    val offlineDir = new File(brokers.head.config.logDirs.tail.head).getAbsolutePath
    val validDir1 = new File(brokers.head.config.logDirs(1)).getAbsolutePath
    val validDir2 = new File(brokers.head.config.logDirs(2)).getAbsolutePath
    val validDir3 = new File(brokers.head.config.logDirs(3)).getAbsolutePath

    // Test AlterReplicaDirRequest before topic creation
    val partitionDirs1 = mutable.Map.empty[TopicPartition, String]
    partitionDirs1.put(new TopicPartition(topic, 0), "invalidDir")
    partitionDirs1.put(new TopicPartition(topic, 1), validDir1)
    val alterReplicaDirResponse1 = sendAlterReplicaLogDirsRequest(partitionDirs1.toMap)
    assertEquals(Errors.LOG_DIR_NOT_FOUND, findErrorForPartition(alterReplicaDirResponse1, new TopicPartition(topic, 0)))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, findErrorForPartition(alterReplicaDirResponse1, new TopicPartition(topic, 1)))

    createTopic(topic, 3)

    // Test AlterReplicaDirRequest after topic creation
    val partitionDirs2 = mutable.Map.empty[TopicPartition, String]
    partitionDirs2.put(new TopicPartition(topic, 0), "invalidDir")
    partitionDirs2.put(new TopicPartition(topic, 1), validDir2)
    val alterReplicaDirResponse2 = sendAlterReplicaLogDirsRequest(partitionDirs2.toMap)
    assertEquals(Errors.LOG_DIR_NOT_FOUND, findErrorForPartition(alterReplicaDirResponse2, new TopicPartition(topic, 0)))
    assertEquals(Errors.NONE, findErrorForPartition(alterReplicaDirResponse2, new TopicPartition(topic, 1)))

    // Test AlterReplicaDirRequest after topic creation and log directory failure
    brokers.head.logDirFailureChannel.maybeAddOfflineLogDir(offlineDir, "", new java.io.IOException())
    TestUtils.waitUntilTrue(() => !brokers.head.logManager.isLogDirOnline(offlineDir), s"timed out waiting for $offlineDir to be offline", 3000)
    val partitionDirs3 = mutable.Map.empty[TopicPartition, String]
    partitionDirs3.put(new TopicPartition(topic, 0), "invalidDir")
    partitionDirs3.put(new TopicPartition(topic, 1), validDir3)
    partitionDirs3.put(new TopicPartition(topic, 2), offlineDir)
    val alterReplicaDirResponse3 = sendAlterReplicaLogDirsRequest(partitionDirs3.toMap)
    assertEquals(Errors.LOG_DIR_NOT_FOUND, findErrorForPartition(alterReplicaDirResponse3, new TopicPartition(topic, 0)))
    assertEquals(Errors.KAFKA_STORAGE_ERROR, findErrorForPartition(alterReplicaDirResponse3, new TopicPartition(topic, 1)))
    assertEquals(Errors.KAFKA_STORAGE_ERROR, findErrorForPartition(alterReplicaDirResponse3, new TopicPartition(topic, 2)))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAlterReplicaLogDirsRequestWithRetention(quorum: String): Unit = {
    val partitionNum = 1

    // Alter replica dir before topic creation
    val logDir1 = new File(brokers.head.config.logDirs(1)).getAbsolutePath
    val partitionDirs1 = (0 until partitionNum).map(partition => new TopicPartition(topic, partition) -> logDir1).toMap
    val alterReplicaLogDirsResponse1 = sendAlterReplicaLogDirsRequest(partitionDirs1)

    // The response should show error UNKNOWN_TOPIC_OR_PARTITION for all partitions
    val tp = new TopicPartition(topic, 0)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, findErrorForPartition(alterReplicaLogDirsResponse1, tp))
    assertTrue(brokers.head.logManager.getLog(tp).isEmpty)

    val topicProperties = new Properties()
    topicProperties.put(TopicConfig.RETENTION_BYTES_CONFIG, "1024")
    // This test needs enough time to wait for dir movement happened.
    // We don't want files with `.deleted` suffix are removed too fast,
    // so we can validate there will be orphan files and orphan files will be removed eventually.
    topicProperties.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "10000")
    topicProperties.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1024")

    createTopic(topic, partitionNum, 1, topicProperties)
    assertEquals(logDir1, brokers.head.logManager.getLog(tp).get.dir.getParent)

    // send enough records to trigger log rolling
    (0 until 20).foreach { _ =>
      TestUtils.generateAndProduceMessages(brokers, topic, 10, 1)
    }
    TestUtils.waitUntilTrue(() => brokers.head.logManager.getLog(new TopicPartition(topic, 0)).get.numberOfSegments > 1,
      "timed out waiting for log segment to roll")

    // Wait for log segment retention in original dir.
    TestUtils.waitUntilTrue(() => {
      new File(logDir1, tp.toString).listFiles().count(_.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) > 0
    }, "timed out waiting for log segment to retention")

    // Alter replica dir again after topic creation
    val logDir2 = new File(brokers.head.config.logDirs(2)).getAbsolutePath
    val alterReplicaLogDirsResponse2 = sendAlterReplicaLogDirsRequest(Map(tp -> logDir2))
    // The response should succeed for all partitions
    assertEquals(Errors.NONE, findErrorForPartition(alterReplicaLogDirsResponse2, tp))
    TestUtils.waitUntilTrue(() => {
      logDir2 == brokers.head.logManager.getLog(tp).get.dir.getParent
    }, "timed out waiting for replica movement")

    // Make sure the deleted log segment is removed
    TestUtils.waitUntilTrue(() => {
      new File(logDir2, tp.toString).listFiles().count(_.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) == 0
    }, "timed out waiting for removing deleted log segment")
  }

  private def sendAlterReplicaLogDirsRequest(partitionDirs: Map[TopicPartition, String]): AlterReplicaLogDirsResponse = {
    val logDirs = partitionDirs.groupBy{case (_, dir) => dir}.map{ case(dir, tps) =>
      new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
        .setPath(dir)
        .setTopics(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopicCollection(
          tps.groupBy { case (tp, _) => tp.topic }
            .map { case (topic, tpPartitions) =>
              new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic()
                .setName(topic)
                .setPartitions(tpPartitions.map{case (tp, _) => tp.partition.asInstanceOf[Integer]}.toList.asJava)
        }.toList.asJava.iterator))
    }
    val data = new AlterReplicaLogDirsRequestData()
        .setDirs(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirCollection(logDirs.asJava.iterator))
    val request = new AlterReplicaLogDirsRequest.Builder(data).build()
    connectAndReceive[AlterReplicaLogDirsResponse](request, anySocketServer)
  }
}
