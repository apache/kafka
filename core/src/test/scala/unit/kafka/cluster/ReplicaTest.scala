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
package kafka.cluster

import java.util.Properties

import kafka.log.{Log, LogConfig, LogManager}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.utils.Utils
import org.junit.{After, Before, Test}
import org.junit.Assert._

class ReplicaTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  var log: Log = _
  var replica: Replica = _

  @Before
  def setup(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    val config = LogConfig(logProps)
    log = Log(logDir,
      config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))

    replica = new Replica(brokerId = 0,
      topicPartition = new TopicPartition("foo", 0),
      time = time,
      log = Some(log))
  }

  @After
  def tearDown(): Unit = {
    log.close()
    brokerTopicStats.close()
    Utils.delete(tmpDir)
  }

  @Test(expected = classOf[OffsetOutOfRangeException])
  def testCannotIncrementLogStartOffsetPastHighWatermark(): Unit = {
    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes)
      log.appendAsLeader(records, leaderEpoch = 0)
    }

    replica.highWatermark = new LogOffsetMetadata(25L)
    replica.maybeIncrementLogStartOffset(26L)
  }

}
