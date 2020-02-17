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

import java.nio.file.Files
import java.util
import java.util.UUID

import kafka.api.IntegrationTestHarness
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{RLMMWithTopicStorage, RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata}
import org.junit.{Assert, Test}

class RLMMWithTopicStorageTest extends IntegrationTestHarness {

  override def brokerCount: Int = 3

  @Test
  @throws[Exception]
  def testPutAndGetRemoteLogMetadata(): Unit = {
    val tmpLogDirPath = Files.createTempDirectory("kafka-").toString

    createTopic(RLMMWithTopicStorage.REMOTE_LOG_METADATA_TOPIC, 5, 2)

    // user topic partitions
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 1)

    val rlmmWithTopicStorage = createRLMMWithTopicStorage(tmpLogDirPath, 1)

    val rlSegIdTp0_0 = new RemoteLogSegmentId(tp0, UUID.randomUUID)
    val rlSegMetTp0_0 = new RemoteLogSegmentMetadata(rlSegIdTp0_0, 0L,
      100L, 1, tp0.toString.getBytes)

    val rlSegIdTp0_101 = new RemoteLogSegmentId(tp0, UUID.randomUUID)
    val rlSegMetTp0_101 = new RemoteLogSegmentMetadata(rlSegIdTp0_101, 101L,
      200L, 1, tp0.toString.getBytes)

    val rlSegIdTp1_101 = new RemoteLogSegmentId(tp1, UUID.randomUUID)
    val rlSegMetTp1_101 = new RemoteLogSegmentMetadata(rlSegIdTp1_101, 101L,
      200L, 1, tp1.toString.getBytes)

    val rlSegIdTp2_401 = new RemoteLogSegmentId(tp2, UUID.randomUUID)
    val rlSegMetTp2_401 = new RemoteLogSegmentMetadata(rlSegIdTp2_401, 401L,
      700L, 1, tp1.toString.getBytes)

    try {

      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegIdTp0_0, rlSegMetTp0_0)
      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegIdTp0_101, rlSegMetTp0_101)
      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegIdTp1_101, rlSegMetTp1_101)
      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegIdTp2_401, rlSegMetTp2_401)

      val rlSegIdTp1_150 = rlmmWithTopicStorage.getRemoteLogSegmentId(tp1, 150)
      Assert.assertEquals(rlSegIdTp1_101, rlSegIdTp1_150)

      // this should return the RemoteLogSegmentId with highest offset as the target offset is beyond the highest.
      val rlSegIdTp0_300 = rlmmWithTopicStorage.getRemoteLogSegmentId(tp0, 300)
      Assert.assertEquals(rlSegIdTp0_101, rlSegIdTp0_300)

    } finally {
      rlmmWithTopicStorage.close()
    }

    // reload RLMM by reading from the data and committed offsets file.
    val rlmmWithTopicStorageReloaded = createRLMMWithTopicStorage(tmpLogDirPath)
    try {
      val remoteLogSegmentId170 = rlmmWithTopicStorageReloaded.getRemoteLogSegmentId(tp0, 170)
      Assert.assertEquals(remoteLogSegmentId170, rlSegIdTp0_101)
    } finally {
      rlmmWithTopicStorageReloaded.close()
    }
  }

  private def createRLMMWithTopicStorage(tmpLogDirPath: String, brokerId: Int = 1): RLMMWithTopicStorage = {
    val rlmmWithTopicStorage = new RLMMWithTopicStorage
    val configs = new util.HashMap[String, Any]
    configs.put("log.dir", tmpLogDirPath)
    configs.put(RemoteLogMetadataManager.BROKER_ID_CONFIG, brokerId)
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    rlmmWithTopicStorage.configure(configs)

    rlmmWithTopicStorage
  }

  @Test
  @throws[Exception]
  def testMultipleRLMMInstanceWorkflow(): Unit = {
    //todo-tier
    val tp = Range.inclusive(0, 2).map(x => new TopicPartition("foo", x))

  }
}
