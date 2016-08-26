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

package unit.kafka.server

import java.util.Properties

import kafka.admin.AdminUtils._
import kafka.common._
import kafka.log.LogConfig._
import kafka.server.KafkaConfig.fromProps
import kafka.server.QuotaType.{LeaderReplication, FollowerReplication}
import kafka.server._
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.MetricName
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class ReplicationQuotaTest extends ZooKeeperTestHarness {
  def TenPercentError(value: Int): Int = Math.round(value * 0.1).toInt

  def FifteenPercentError(value: Int): Int = Math.round(value * 0.15).toInt

  val msg100KB = new Array[Byte](100000)
  var brokers: Seq[KafkaServer] = null
  var leader: KafkaServer = null
  var follower: KafkaServer = null
  val topic1 = "topic1"
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  var leaderByteRateMetricName: MetricName = null
  var followerByteRateMetricName: MetricName = null

  @Before
  override def setUp() {
    super.setUp()
    //Create two brokers with one partition then figure out who the leader is.
    brokers = createBrokerConfigs(2, zkConnect).map(fromProps).map(TestUtils.createServer(_))
    val leaders = TestUtils.createTopic(zkUtils, topic1, numPartitions = 1, replicationFactor = 2, servers = brokers)
    leader = if (leaders(0).get == brokers.head.config.brokerId) brokers.head else brokers(1)
    follower = if (leaders(0).get == brokers.head.config.brokerId) brokers(1) else brokers.head
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5, acks = 0)
    leaderByteRateMetricName = leader.metrics.metricName("byte-rate", LeaderReplication.toString, "Tracking byte-rate for " + LeaderReplication)
    followerByteRateMetricName = leader.metrics.metricName("byte-rate", FollowerReplication.toString, "Tracking byte-rate for" + FollowerReplication)
  }

  @After
  override def tearDown() {
    brokers.foreach(_.shutdown())
    producer.close()
    super.tearDown()
  }

  //TODO do a test that mimics a bootstrapping broker
  //TODO tests show the producer timing out. This is us throttling an ISR partition. Should reconsider this issue.
  //TODO speed up by altering quota.window.num & quota.window.size.seconds and reducing throttle

  @Test //make this test faster by reducing the window lengths
  def shouldThrottleToDesiredRateOnLeaderOverTime(): Unit = {
    shouldThrottleToDesiredRateOverTime(leader,  leaderByteRateMetricName)
  }

  @Test //make this test faster by reducing the window lengths
  def shouldThrottleToDesiredRateOFollowerOverTime(): Unit = {
    shouldThrottleToDesiredRateOverTime(follower, followerByteRateMetricName)
  }

  //TODO need to work on the temporal comparisons prior to merge
  def shouldThrottleToDesiredRateOverTime(brokerUnderTest: KafkaServer, metricName: MetricName) {

    /**
      * This test will fail if the rate is < 1MB/s as 1MB is replica.fetch.max.bytes.
      * So with a throttle of 100KB/s 1 fetch of 1 partition would fill 10s of quota.
      * In doing so sending the quota way over, then blocking for ten seconds. Thus
      * it is likely the test will fail, depending on when in the cycle it completes.
      *
      * Keep the throttle value > 1MB/s to get stability.
      *
      */

    //Given
    val msg = msg100KB
    val throttle: Int = 10 * msg.length
    val msgCount: Int = 100

    //Propagate throttle value and list of throttled partitions
    changeBrokerConfig(zkUtils, (0 until brokers.length), property(KafkaConfig.ThrottledReplicationRateLimitProp, throttle.toString))
    changeTopicConfig(zkUtils, topic1, property(ThrottledReplicasListProp, "*"))

    val start = System.currentTimeMillis()

    //When we load with data (acks = 0)
    for (x <- 0 until msgCount)
      producer.send(new ProducerRecord(topic1, msg))

    //Wait for replication to complete
    def logsMatchAtOffset() = waitForOffset(tp(topic1, 0), msgCount)
    waitUntilTrue(logsMatchAtOffset, "Broker logs should be identical and have offset " + msgCount, 100000)
    val took = System.currentTimeMillis() - start

    //Then the recorded rate should match the quota we defined
    val throttledRateFromLeader: Double = brokerUnderTest.metrics.metrics.asScala(metricName).value()
    assertEquals(throttle, throttledRateFromLeader, TenPercentError(throttle))

    //Then also check it took the expected amount of time (don't merge this as is)
    val expectedDuration = msgCount / (throttle / msg.length) * 1000
    info(s"Expected:$expectedDuration, Took:$took")
    assertEquals(expectedDuration, took, expectedDuration * 0.2)
  }

  @Test
  def shouldReplicateThrottledAndNonThrottledPartitionsConcurrentlyWhenLeaderThrottleEngaged() {
    shouldReplicateThrottledAndNonThrottledPartitionsConcurrently(LeaderReplication)
  }

  @Test
  def shouldReplicateThrottledAndNonThrottledPartitionsConcurrentlyWhenFollowerThrottleEngaged() {
    shouldReplicateThrottledAndNonThrottledPartitionsConcurrently(FollowerReplication)
  }

  //TODO need to work on the temporal comparisons prior to merge
  def shouldReplicateThrottledAndNonThrottledPartitionsConcurrently(throttleSide: QuotaType): Unit = {
    val topic = "specific-replicas"

    //Given 4 partitions, all lead on server 0, we'll throttle two of them
    TestUtils.createTopic(zkUtils, topic, Map(0 -> Seq(0, 1), 1 -> Seq(0, 1), 2 -> Seq(0, 1), 3 -> Seq(0, 1)), brokers)

    //Define test settings
    val msg = msg100KB
    val throttle: Int = 10 * msg.length
    val msgCount: Int = 50

    //Set the throttle config and replicas list so partition 0 & 2, only, are throttled
    changeBrokerConfig(zkUtils, (0 until brokers.length), property(KafkaConfig.ThrottledReplicationRateLimitProp, throttle.toString))
    val side: Int = if (throttleSide == FollowerReplication) 1 else 0
    changeTopicConfig(zkUtils, topic, property(ThrottledReplicasListProp, s"0-$side:2-$side")) //partition 0 & 2 are throttled

    //Useful functions
    def configPropagatedCondition(): Boolean = brokers(side).quotaManagers.leaderReplication.isThrottled(new TopicAndPartition(topic, 0)) //doesn't matter if we check leader or follower replication
    def logsMatchRegular() = waitForOffset(tp(topic, 1), msgCount) && waitForOffset(tp(topic, 3), msgCount)
    def logsMatchThrottled() = waitForOffset(tp(topic, 0), msgCount) && waitForOffset(tp(topic, 2), msgCount) //the throttled ones

    //Wait for config to propagate
    waitUntilTrue(configPropagatedCondition, "Throttled partition config should have propagated.")

    val start: Long = System.currentTimeMillis()

    //Write a message to each partition individually to get an even spread
    for (x <- 0 until msgCount)
      for (partition <- (0 to 3))
        producer.send(new ProducerRecord(topic, partition, null, msg))

    waitUntilTrue(logsMatchRegular, "Partition 1 or 3's logs didn't match", 30000)

    var took = System.currentTimeMillis() - start
    assertTrue("Partition 1 & 3 should have replicated quickly: " + took, took < 2000)

    waitUntilTrue(logsMatchThrottled, "Throttled partitions (0,2) logs didn't match")

    val expectedDuration = msgCount / (throttle / msg.length) * 1000 * 2 // i.e 2 throttled partitions
    took = System.currentTimeMillis() - start
    info(s"expected: $expectedDuration, was: $took")
    assertEquals(s"Throttled partitions should have been slow. Was $took ms", expectedDuration, took, FifteenPercentError(expectedDuration))
  }

  def tp(topic: String, partition: Int): TopicAndPartition = new TopicAndPartition(topic, partition)

  def logsMatch(): Boolean = logsMatch(TopicAndPartition(topic1, 0))

  def logsMatch(topicAndPart: TopicAndPartition): Boolean = {
    var result = true
    val expectedOffset = brokers.head.getLogManager().getLog(topicAndPart).get.logEndOffset
    result = result && expectedOffset > 0 && brokers.forall { item =>
      expectedOffset == item.getLogManager().getLog(topicAndPart).get.logEndOffset
    }
    if (result) info("final offset was " + expectedOffset + " for partition " + topicAndPart)
    result
  }

  def waitForOffset(topicAndPart: TopicAndPartition, offset: Int, servers: Seq[KafkaServer] = brokers): Boolean = {
    var result = true
    result = result && servers.forall { item =>
      offset == (if (item.getLogManager().getLog(topicAndPart) == None) 0 else item.getLogManager().getLog(topicAndPart).get.logEndOffset)
    }
    result
  }

  def property(key: String, value: String) = {
    val props = new Properties()
    props.put(key, value)
    props
  }
}
