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

import kafka.log._
import java.io.File

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock
import org.junit._
import org.junit.Assert._
import kafka.utils.{KafkaScheduler, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Partition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.SimpleRecord

class HighwatermarkPersistenceTest {

  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps)
  val topic = "foo"
  val zkClient: KafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
  val logManagers = configs map { config =>
    TestUtils.createLogManager(
      logDirs = config.logDirs.map(new File(_)),
      cleanerConfig = CleanerConfig())
  }

  val logDirFailureChannels = configs map { config =>
    new LogDirFailureChannel(config.logDirs.size)
  }

  val alterIsrManager = TestUtils.createAlterIsrManager()

  @After
  def teardown(): Unit = {
    for (manager <- logManagers; dir <- manager.liveLogDirs)
      Utils.delete(dir)
  }

  @Test
  def testHighWatermarkPersistenceSinglePartition(): Unit = {
    // mock zkclient
    EasyMock.replay(zkClient)

    // create kafka scheduler
    val scheduler = new KafkaScheduler(2)
    scheduler.startup()
    val metrics = new Metrics
    val time = new MockTime
    val quotaManager = QuotaFactory.instantiate(configs.head, metrics, time, "")
    // create replica manager
    val replicaManager = new ReplicaManager(configs.head, metrics, time, zkClient, scheduler,
      logManagers.head, new AtomicBoolean(false), quotaManager,
      new BrokerTopicStats, new MetadataCache(configs.head.brokerId), logDirFailureChannels.head, alterIsrManager)
    replicaManager.startup()
    try {
      replicaManager.checkpointHighWatermarks()
      var fooPartition0Hw = hwmFor(replicaManager, topic, 0)
      assertEquals(0L, fooPartition0Hw)
      val tp0 = new TopicPartition(topic, 0)
      val partition0 = replicaManager.createPartition(tp0)
      // create leader and follower replicas
      val log0 = logManagers.head.getOrCreateLog(new TopicPartition(topic, 0), () => LogConfig())
      partition0.setLog(log0, isFutureLog = false)

      partition0.updateAssignmentAndIsr(
        assignment = Seq(configs.head.brokerId, configs.last.brokerId),
        isr = Set(configs.head.brokerId),
        addingReplicas = Seq.empty,
        removingReplicas = Seq.empty
      )

      replicaManager.checkpointHighWatermarks()
      fooPartition0Hw = hwmFor(replicaManager, topic, 0)
      assertEquals(log0.highWatermark, fooPartition0Hw)
      // set the high watermark for local replica
      partition0.localLogOrException.updateHighWatermark(5L)
      replicaManager.checkpointHighWatermarks()
      fooPartition0Hw = hwmFor(replicaManager, topic, 0)
      assertEquals(log0.highWatermark, fooPartition0Hw)
      EasyMock.verify(zkClient)
    } finally {
      // shutdown the replica manager upon test completion
      replicaManager.shutdown(false)
      quotaManager.shutdown()
      metrics.close()
      scheduler.shutdown()
    }
  }

  @Test
  def testHighWatermarkPersistenceMultiplePartitions(): Unit = {
    val topic1 = "foo1"
    val topic2 = "foo2"
    // mock zkclient
    EasyMock.replay(zkClient)
    // create kafka scheduler
    val scheduler = new KafkaScheduler(2)
    scheduler.startup()
    val metrics = new Metrics
    val time = new MockTime
    val quotaManager = QuotaFactory.instantiate(configs.head, metrics, time, "")
    // create replica manager
    val replicaManager = new ReplicaManager(configs.head, metrics, time, zkClient,
      scheduler, logManagers.head, new AtomicBoolean(false), quotaManager,
      new BrokerTopicStats, new MetadataCache(configs.head.brokerId), logDirFailureChannels.head, alterIsrManager)
    replicaManager.startup()
    try {
      replicaManager.checkpointHighWatermarks()
      var topic1Partition0Hw = hwmFor(replicaManager, topic1, 0)
      assertEquals(0L, topic1Partition0Hw)
      val t1p0 = new TopicPartition(topic1, 0)
      val topic1Partition0 = replicaManager.createPartition(t1p0)
      // create leader log
      val topic1Log0 = logManagers.head.getOrCreateLog(t1p0, () => LogConfig())
      // create a local replica for topic1
      topic1Partition0.setLog(topic1Log0, isFutureLog = false)
      replicaManager.checkpointHighWatermarks()
      topic1Partition0Hw = hwmFor(replicaManager, topic1, 0)
      assertEquals(topic1Log0.highWatermark, topic1Partition0Hw)
      // set the high watermark for local replica
      append(topic1Partition0, count = 5)
      topic1Partition0.localLogOrException.updateHighWatermark(5L)
      replicaManager.checkpointHighWatermarks()
      topic1Partition0Hw = hwmFor(replicaManager, topic1, 0)
      assertEquals(5L, topic1Log0.highWatermark)
      assertEquals(5L, topic1Partition0Hw)
      // add another partition and set highwatermark
      val t2p0 = new TopicPartition(topic2, 0)
      val topic2Partition0 = replicaManager.createPartition(t2p0)
      // create leader log
      val topic2Log0 = logManagers.head.getOrCreateLog(t2p0, () => LogConfig())
      // create a local replica for topic2
      topic2Partition0.setLog(topic2Log0, isFutureLog = false)
      replicaManager.checkpointHighWatermarks()
      var topic2Partition0Hw = hwmFor(replicaManager, topic2, 0)
      assertEquals(topic2Log0.highWatermark, topic2Partition0Hw)
      // set the highwatermark for local replica
      append(topic2Partition0, count = 15)
      topic2Partition0.localLogOrException.updateHighWatermark(15L)
      assertEquals(15L, topic2Log0.highWatermark)
      // change the highwatermark for topic1
      append(topic1Partition0, count = 5)
      topic1Partition0.localLogOrException.updateHighWatermark(10L)
      assertEquals(10L, topic1Log0.highWatermark)
      replicaManager.checkpointHighWatermarks()
      // verify checkpointed hw for topic 2
      topic2Partition0Hw = hwmFor(replicaManager, topic2, 0)
      assertEquals(15L, topic2Partition0Hw)
      // verify checkpointed hw for topic 1
      topic1Partition0Hw = hwmFor(replicaManager, topic1, 0)
      assertEquals(10L, topic1Partition0Hw)
      EasyMock.verify(zkClient)
    } finally {
      // shutdown the replica manager upon test completion
      replicaManager.shutdown(false)
      quotaManager.shutdown()
      metrics.close()
      scheduler.shutdown()
    }
  }

  private def append(partition: Partition, count: Int): Unit = {
    val records = TestUtils.records((0 to count).map(i => new SimpleRecord(s"$i".getBytes)))
    partition.localLogOrException.appendAsLeader(records, leaderEpoch = 0)
  }

  private def hwmFor(replicaManager: ReplicaManager, topic: String, partition: Int): Long = {
    replicaManager.highWatermarkCheckpoints(new File(replicaManager.config.logDirs.head).getAbsolutePath).read().getOrElse(
      new TopicPartition(topic, partition), 0L)
  }
}
