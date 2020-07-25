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

import java.io.File
import java.nio.file.{Files, Path}
import java.util
import java.util.{Collections, UUID}

import kafka.api.IntegrationTestHarness
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata.remoteLogSegmentId
import org.apache.kafka.common.log.remote.storage.{RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata}
import org.junit.{Assert, Before, Test}

import scala.jdk.CollectionConverters._

/**
 * This test is added here as it needs the brokers to be run to test the `RemoteLogMetadataManager` implementation
 * based on topic storage.
 */
class RLMMWithTopicStorageTest extends IntegrationTestHarness {

  override def brokerCount: Int = 3

  // user topic partitions
  val tp0 = new TopicPartition("foo", 0)
  val tp1 = new TopicPartition("foo", 1)
  val tp2 = new TopicPartition("bar", 0)
  val tp3 = new TopicPartition("bar", 1)

  val allTopicPartitions: util.Set[TopicPartition] = Set(tp0, tp1, tp2, tp3).asJava

  val segSize: Long = 1024 * 1024

  val rlSegIdTp0_0_100 = new RemoteLogSegmentId(tp0, UUID.randomUUID)
  val rlSegMetTp0_0_100 = new RemoteLogSegmentMetadata(rlSegIdTp0_0_100, 0L, 100L, -1L, 1, segSize)

  val rlSegIdTp0_101_200 = new RemoteLogSegmentId(tp0, UUID.randomUUID)
  val rlSegMetTp0_101_200 = new RemoteLogSegmentMetadata(rlSegIdTp0_101_200, 101L, 200L, -1L, 1, segSize)

  val rlSegIdTp1_101_300 = new RemoteLogSegmentId(tp1, UUID.randomUUID)
  val rlSegMetTp1_101_300 = new RemoteLogSegmentMetadata(rlSegIdTp1_101_300, 101L, 300L, -1L, 1, segSize)

  val rlSegIdTp2_150_400 = new RemoteLogSegmentId(tp2, UUID.randomUUID)
  val rlSegMetTp2_150_400 = new RemoteLogSegmentMetadata(rlSegIdTp2_150_400, 150L, 400L, -1L, 1, segSize)

  val rlSegIdTp2_401_700 = new RemoteLogSegmentId(tp2, UUID.randomUUID)
  val rlSegMetTp2_401_700 = new RemoteLogSegmentMetadata(rlSegIdTp2_401_700, 401L, 700L, -1L, 1, segSize)

  val rlSegIdTp2_501_1000 = new RemoteLogSegmentId(tp2, UUID.randomUUID)
  val rlSegMetTp2_501_1000 = new RemoteLogSegmentMetadata(rlSegIdTp2_501_1000, 501L, 1000L, -1L, 1, segSize)

  val rlSegIdTp3_101_700 = new RemoteLogSegmentId(tp3, UUID.randomUUID)
  val rlSegMetTp3_101 = new RemoteLogSegmentMetadata(rlSegIdTp3_101_700, 101L, 700L, -1L, 1, segSize)

  val rlSegIdTp3_701_1900 = new RemoteLogSegmentId(tp3, UUID.randomUUID)
  val rlSegMetTp3_701_1900 = new RemoteLogSegmentMetadata(rlSegIdTp3_701_1900, 701L, 1900L, -1L, 1, segSize)

  def tmpLogDirPathAsStr: String = tmpLogDirPath.toString

  var tmpLogDirPath: Path = _

  @Before
  override def setUp(): Unit = {
    super.setUp()
    tmpLogDirPath = Files.createTempDirectory("kafka-")

  }

  @Test
  @throws[Exception]
  def testPutAndGetRemoteLogMetadata(): Unit = {

    var mayBeRlmmWithTopicStorage: Option[RLMMWithTopicStorage] = None

    try {
      mayBeRlmmWithTopicStorage = Some(createRLMMWithTopicStorage(tmpLogDirPathAsStr, 1))
      val rlmmWithTopicStorage = mayBeRlmmWithTopicStorage.get
      rlmmWithTopicStorage.onPartitionLeadershipChanges(allTopicPartitions, Set.empty[TopicPartition].asJava)

      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegMetTp0_0_100)
      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegMetTp0_101_200)
      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegMetTp1_101_300)
      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegMetTp2_401_700)

      val rlSegIdTp1_150 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp1, 150))
      Assert.assertEquals(rlSegIdTp1_101_300, rlSegIdTp1_150)

      // this should return the RemoteLogSegmentId with offset containing 0, including startoffset and the first entry
      val rlSegIdTp0_0 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 0))
      Assert.assertEquals(rlSegIdTp0_0_100, rlSegIdTp0_0)

      // this should return the RemoteLogSegmentId with offset containing 100, as last offset of the first entry
      val rlSegIdTp0_100 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 100))
      Assert.assertEquals(rlSegIdTp0_0_100, rlSegIdTp0_100)

      // this should return the RemoteLogSegmentId with offset containing 101, including startoffset
      val rlSegIdTp0_101 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 101))
      Assert.assertEquals(rlSegIdTp0_101_200, rlSegIdTp0_101)

      // this should return the RemoteLogSegmentId with offset containing 200, including endoffset and last entry.
      val rlSegIdTp0_200 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 200))
      Assert.assertEquals(rlSegIdTp0_101_200, rlSegIdTp0_200)

      // this should return the RemoteLogSegmentId with highest offset as the target offset is beyond the highest.
      val rlSegIdTp0_300 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 300))
      Assert.assertEquals(rlSegIdTp0_101_200, rlSegIdTp0_300)
    } finally {
      mayBeRlmmWithTopicStorage.foreach(x => x.close())
    }

    // reload RLMM by reading from the data and committed offsets file.
    val rlmmWithTopicStorageReloaded = createRLMMWithTopicStorage(tmpLogDirPathAsStr)
    try {
      val remoteLogSegmentId170 = remoteLogSegmentId(rlmmWithTopicStorageReloaded.remoteLogSegmentMetadata(tp0, 170))
      Assert.assertEquals(rlSegIdTp0_101_200, remoteLogSegmentId170)
    } finally {
      rlmmWithTopicStorageReloaded.close()
    }
  }

  @Test
  @throws[Exception]
  def testNonExistingOffsets(): Unit = {

    val rlSegIdTp0_10 = new RemoteLogSegmentId(tp0, UUID.randomUUID)
    val rlSegMetTp0_10 = new RemoteLogSegmentMetadata(rlSegIdTp0_10, 10L, 100L, -1L, 1, segSize)
    var mayBeRlmmWithTopicStorage: Option[RLMMWithTopicStorage] = None
    try {
      val rlmmWithTopicStorage = createRLMMWithTopicStorage(tmpLogDirPathAsStr, 1)
      mayBeRlmmWithTopicStorage = Some(rlmmWithTopicStorage)
      rlmmWithTopicStorage.onPartitionLeadershipChanges(allTopicPartitions, Set.empty[TopicPartition].asJava)
      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegMetTp0_10)

      // get the non existing offset, below base offset
      val rlSegIdTp0_2 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 2L))
      Assert.assertNull(rlSegIdTp0_2)

      // get the non existing offset, above end offset. This should return the immediate floor entry.
      val rlSegIdTp0_200 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 200L))
      Assert.assertEquals(rlSegIdTp0_10, rlSegIdTp0_200)
    } finally {
      mayBeRlmmWithTopicStorage.foreach(x => x.close())
    }
  }

  @Test
  @throws[Exception]
  def testDeleteRemoteLogSegment(): Unit = {

    var mayBeRlmmWithTopicStorage: Option[RLMMWithTopicStorage] = None

    try {
      val rlmmWithTopicStorage = createRLMMWithTopicStorage(tmpLogDirPathAsStr, 1)
      mayBeRlmmWithTopicStorage = Some(rlmmWithTopicStorage)
      rlmmWithTopicStorage.onPartitionLeadershipChanges(allTopicPartitions, Set.empty[TopicPartition].asJava)
      rlmmWithTopicStorage.putRemoteLogSegmentData(rlSegMetTp0_0_100)

      // get the non existing offset, below base offset
      val rlSegMetTp0_15 = remoteLogSegmentId(rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 15L))
      Assert.assertEquals(rlSegIdTp0_0_100, rlSegMetTp0_15)

      // delete the segment
      rlmmWithTopicStorage.deleteRemoteLogSegmentMetadata(rlSegIdTp0_0_100)

      // there should not be any entry as it is already deleted.
      val rlSegMetTp0_15_2 = rlmmWithTopicStorage.remoteLogSegmentMetadata(tp0, 15L)
      Assert.assertNull(rlSegMetTp0_15_2)

    } finally {
      mayBeRlmmWithTopicStorage.foreach(x => x.close())
    }
  }

  private def createRLMMWithTopicStorage(tmpLogDirPath: String, brokerId: Int = 1): RLMMWithTopicStorage = {
    val rlmmWithTopicStorage = new RLMMWithTopicStorage
    configureRLMM(tmpLogDirPath, brokerId, rlmmWithTopicStorage)
    rlmmWithTopicStorage
  }

  private def configureRLMM(tmpLogDirPath: String, brokerId: Int,
                            rlmmWithTopicStorage: RLMMWithTopicStorage) = {
    val configs = new util.HashMap[String, Any]
    val logDir = new File(tmpLogDirPath, 1.toString)
    logDir.mkdirs()
    configs.put("log.dir", logDir.toString)
    configs.put(RemoteLogMetadataManager.BROKER_ID, brokerId)
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    rlmmWithTopicStorage.configure(configs)
    rlmmWithTopicStorage.onServerStarted(null)
  }

  def waitTillReceiveExpected(fn: () => RemoteLogSegmentId, expected: RemoteLogSegmentId, waitTimeInMillis: Long = 30000): Boolean = {
    var checkAgain = true
    val startTime = System.currentTimeMillis()
    while (checkAgain) {
      val result = fn()
      if (!expected.equals(result)) {
        if (System.currentTimeMillis() - startTime < waitTimeInMillis) {
          info("Sleeping for 100 msecs to retry again")
          Thread.sleep(100)
          checkAgain = true
        } else {
          return false
        }
      } else {
        return true
      }
    }

    false
  }

  @Test
  @throws[Exception]
  def testMultipleRLMMInstanceWorkflow(): Unit = {
    // Multiple RLMM instances publishes events and they should receive events from each other.

    // this is a leader and publishes remote log segment events.
    val brokerId1 = 1
    var mayBeRlmm1: Option[RLMMWithTopicStorage] = None

    // this is a follower and never publishes events but consumes events from the topic.
    val brokerId2 = 2
    var mayBeRlmm2: Option[RLMMWithTopicStorage] = None

    try {
      val rlmm1 = createRLMMWithTopicStorage(tmpLogDirPathAsStr, brokerId1)
      mayBeRlmm1 = Some(rlmm1)

      val rlmm2 = createRLMMWithTopicStorage(tmpLogDirPathAsStr, brokerId2)
      mayBeRlmm2 = Some(rlmm2);

      // make tp0 and tp3 as leader for rlmm1 and follower for rlmm2
      val partitions = new util.HashSet[TopicPartition]()
      partitions.add(tp0)
      partitions.add(tp2)
      rlmm1.onPartitionLeadershipChanges(partitions, Collections.emptySet());

      rlmm2.onPartitionLeadershipChanges(Collections.emptySet(), partitions);

      // publishes events to rlmm1 for tp0 and tp2
      rlmm1.putRemoteLogSegmentData(rlSegMetTp0_0_100);
      rlmm1.putRemoteLogSegmentData(rlSegMetTp0_101_200);
      rlmm1.putRemoteLogSegmentData(rlSegMetTp2_501_1000);

      // check whether the published events from rlmm1 are available.
      val rlSegMetTp0_0_1 = rlmm1.remoteLogSegmentMetadata(tp0, 10);
      Assert.assertEquals(rlSegIdTp0_0_100, rlSegMetTp0_0_1);
      val rlSegIdTp0_101_1 = rlmm1.remoteLogSegmentMetadata(tp0, 190);
      Assert.assertEquals(rlSegIdTp0_101_200, rlSegIdTp0_101_1);

      // check whether these events are received in rlmm2 as it is a follower.
      Assert.assertTrue(waitTillReceiveExpected(() => remoteLogSegmentId(rlmm2.remoteLogSegmentMetadata(tp0, 10)), rlSegIdTp0_0_100))
      Assert.assertTrue(waitTillReceiveExpected(() => remoteLogSegmentId(rlmm2.remoteLogSegmentMetadata(tp0, 190)), rlSegIdTp0_101_200))

    } finally {
      mayBeRlmm1.foreach(x => x.close())
      mayBeRlmm2.foreach(x => x.close())
    }
  }

  @Test
  def testLeaderFollowerFailover(): Unit = {

    val brokerId1 = 1
    var mayBeRlmm1: Option[RLMMWithTopicStorage] = None

    val brokerId2 = 2
    var mayBeRlmm2: Option[RLMMWithTopicStorage] = None

    // tp3 remote log segment metadata notifications always go to partition 1 and all other notifications go to
    //partition 0.
    class RLMMWithTopicStorageWithCustomPartitioner extends RLMMWithTopicStorage {
      override def metadataPartitionFor(tp: TopicPartition): Int = {
        if (tp.equals(tp3)) 1 else 0
      }
    }

    try {
      val rlmm1 = new RLMMWithTopicStorageWithCustomPartitioner()

      mayBeRlmm1 = Some(rlmm1)
      configureRLMM(tmpLogDirPathAsStr, brokerId1, rlmm1)

      // make tp0 and tp3 as leader for rlmm1
      val leaderSet1 = new util.HashSet[TopicPartition]()
      leaderSet1.add(tp0)
      leaderSet1.add(tp3)
      rlmm1.onPartitionLeadershipChanges(leaderSet1, Collections.emptySet())

      // publish segment metadata
      rlmm1.putRemoteLogSegmentData(rlSegMetTp0_0_100)
      rlmm1.putRemoteLogSegmentData(rlSegMetTp0_101_200)
      rlmm1.putRemoteLogSegmentData(rlSegMetTp3_101)

      val rlSegMetTp0_10 = rlmm1.remoteLogSegmentMetadata(tp0, 10);
      Assert.assertEquals(rlSegIdTp0_0_100, rlSegMetTp0_10);

      val rlSegMetTp3_140 = rlmm1.remoteLogSegmentMetadata(tp3, 140);
      Assert.assertEquals(rlSegIdTp3_101_700, rlSegMetTp3_140);

      val rlmm2 = new RLMMWithTopicStorageWithCustomPartitioner()
      mayBeRlmm2 = Some(rlmm2)
      configureRLMM(tmpLogDirPathAsStr, brokerId2, rlmm2)

      // make tp1 and tp2 as leaders for rlmm2
      val leaderSet2 = new util.HashSet[TopicPartition]()
      leaderSet2.add(tp1)
      leaderSet2.add(tp2)
      rlmm2.onPartitionLeadershipChanges(leaderSet2, Collections.emptySet())

      // publish segment metadata
      rlmm2.putRemoteLogSegmentData(rlSegMetTp1_101_300)
      rlmm2.putRemoteLogSegmentData(rlSegMetTp2_150_400)

      // check for a few messages for tp1 and tp2 but not for tp0
      val rlSegIdTp1_180 = remoteLogSegmentId(rlmm2.remoteLogSegmentMetadata(tp1, 180))
      Assert.assertEquals(rlSegIdTp1_101_300, rlSegIdTp1_180)

      val rlSegIdTp2_300 = remoteLogSegmentId(rlmm2.remoteLogSegmentMetadata(tp2, 300))
      Assert.assertEquals(rlSegIdTp2_150_400, rlSegIdTp2_300)

      // check for tp3 messages in rlmm2, but it should not have received
      Assert.assertFalse(waitTillReceiveExpected(() => remoteLogSegmentId(rlmm2.remoteLogSegmentMetadata(tp3, 170)), rlSegIdTp3_101_700, 2000L));

      // reassign tp3 from rlmm1 to rlmm2. rlmm1 should not receive any updates of tp3 as it should have been
      // unsubscribed fro remote log metadata partition 1. Because only tp3 notifications go to partition 1.

      val movedPartitions = Collections.singleton(tp3)
      rlmm1.onStopPartitions(movedPartitions)
      rlmm2.onPartitionLeadershipChanges(movedPartitions, Collections.emptySet())

      // rlmm2 should receive all notifications for tp3 as it is subscribed for.
      Assert.assertTrue(waitTillReceiveExpected(() => remoteLogSegmentId(rlmm2.remoteLogSegmentMetadata(tp3, 170)), rlSegIdTp3_101_700));

      // add a new segment notification for tp3
      rlmm2.putRemoteLogSegmentData(rlSegMetTp3_701_1900)
      Assert.assertTrue(waitTillReceiveExpected(() => remoteLogSegmentId(rlmm2.remoteLogSegmentMetadata(tp3, 720)), rlSegIdTp3_701_1900));

      // rlmm1 should not receive latest tp3 segment notifications as it is not assigned for.
      Assert.assertFalse(waitTillReceiveExpected(() => remoteLogSegmentId(rlmm1.remoteLogSegmentMetadata(tp3, 720)), rlSegIdTp3_701_1900, 2000L));

      // add rlmm1 as follower for tp3 and it should receive the latest tp3 segment notification.
      rlmm1.onPartitionLeadershipChanges(Collections.emptySet(), movedPartitions)
      Assert.assertTrue(waitTillReceiveExpected(() => remoteLogSegmentId(rlmm1.remoteLogSegmentMetadata(tp3, 720)), rlSegIdTp3_701_1900));

    } finally {
      mayBeRlmm1.foreach(x => x.close())
      mayBeRlmm2.foreach(x => x.close())
    }
  }

}
