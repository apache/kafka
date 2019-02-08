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

package kafka.server

import java.util.Properties

import kafka.log.LogConfig._
import kafka.server.KafkaConfig.fromProps
import kafka.server.QuotaType._
import kafka.utils.TestUtils._
import kafka.utils.CoreUtils._
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Test}

import scala.collection.JavaConverters._

/**
  * This is the main test which ensure Replication Quotas work correctly.
  *
  * The test will fail if the quota is < 1MB/s as 1MB is the default for replica.fetch.max.bytes.
  * So with a throttle of 100KB/s, 1 fetch of 1 partition would fill 10s of quota. In turn causing
  * the throttled broker to pause for > 10s
  *
  * Anything over 100MB/s tends to fail as this is the non-throttled replication rate
  */
class ReplicationQuotasTest extends ZooKeeperTestHarness {
  def percentError(percent: Int, value: Long): Long = Math.round(value * percent / 100)

  val msg100KB = new Array[Byte](100000)
  var brokers: Seq[KafkaServer] = null
  val topic = "topic1"
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null

  @After
  override def tearDown() {
    producer.close()
    shutdownServers(brokers)
    super.tearDown()
  }

  @Test
  def shouldBootstrapTwoBrokersWithLeaderThrottle(): Unit = {
    shouldMatchQuotaReplicatingThroughAnAsymmetricTopology(true)
  }

  @Test
  def shouldBootstrapTwoBrokersWithFollowerThrottle(): Unit = {
    shouldMatchQuotaReplicatingThroughAnAsymmetricTopology(false)
  }

  def shouldMatchQuotaReplicatingThroughAnAsymmetricTopology(leaderThrottle: Boolean): Unit = {
    /**
      * In short we have 8 brokers, 2 are not-started. We assign replicas for the two non-started
      * brokers, so when we start them we can monitor replication from the 6 to the 2.
      *
      * We also have two non-throttled partitions on two of the 6 brokers, just to make sure
      * regular replication works as expected.
      */

    brokers = (100 to 105).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }

    //Given six partitions, led on nodes 0,1,2,3,4,5 but with followers on node 6,7 (not started yet)
    //And two extra partitions 6,7, which we don't intend on throttling.
    val assignment = Map(
      0 -> Seq(100, 106), //Throttled
      1 -> Seq(101, 106), //Throttled
      2 -> Seq(102, 106), //Throttled
      3 -> Seq(103, 107), //Throttled
      4 -> Seq(104, 107), //Throttled
      5 -> Seq(105, 107), //Throttled
      6 -> Seq(100, 106), //Not Throttled
      7 -> Seq(101, 107) //Not Throttled
    )
    TestUtils.createTopic(zkClient, topic, assignment, brokers)

    val msg = msg100KB
    val msgCount = 100
    val expectedDuration = 10 //Keep the test to N seconds
    var throttle: Long = msgCount * msg.length / expectedDuration
    if (!leaderThrottle) throttle = throttle * 3 //Follower throttle needs to replicate 3x as fast to get the same duration as there are three replicas to replicate for each of the two follower brokers

    //Set the throttle limit on all 8 brokers, but only assign throttled replicas to the six leaders, or two followers
    (100 to 107).foreach { brokerId =>
      adminZkClient.changeBrokerConfig(Seq(brokerId),
        propsWith(
          (DynamicConfig.Broker.LeaderReplicationThrottledRateProp, throttle.toString),
          (DynamicConfig.Broker.FollowerReplicationThrottledRateProp, throttle.toString)
        ))
    }

    //Either throttle the six leaders or the two followers
    if (leaderThrottle)
      adminZkClient.changeTopicConfig(topic, propsWith(LeaderReplicationThrottledReplicasProp, "0:100,1:101,2:102,3:103,4:104,5:105" ))
    else
      adminZkClient.changeTopicConfig(topic, propsWith(FollowerReplicationThrottledReplicasProp, "0:106,1:106,2:106,3:107,4:107,5:107"))

    //Add data equally to each partition
    producer = createProducer(getBrokerListStrFromServers(brokers), acks = 1)
    (0 until msgCount).foreach { _ =>
      (0 to 7).foreach { partition =>
        producer.send(new ProducerRecord(topic, partition, null, msg))
      }
    }

    //Ensure data is fully written: broker 1 has partition 1, broker 2 has partition 2 etc
    (0 to 5).foreach { id => waitForOffsetsToMatch(msgCount, id, 100 + id) }
    //Check the non-throttled partitions too
    waitForOffsetsToMatch(msgCount, 6, 100)
    waitForOffsetsToMatch(msgCount, 7, 101)

    val start = System.currentTimeMillis()

    //When we create the 2 new, empty brokers
    createBrokers(106 to 107)

    //Check that throttled config correctly migrated to the new brokers
    (106 to 107).foreach { brokerId =>
      assertEquals(throttle, brokerFor(brokerId).quotaManagers.follower.upperBound())
    }
    if (!leaderThrottle) {
      (0 to 2).foreach { partition => assertTrue(brokerFor(106).quotaManagers.follower.isThrottled(tp(partition))) }
      (3 to 5).foreach { partition => assertTrue(brokerFor(107).quotaManagers.follower.isThrottled(tp(partition))) }
    }

    //Wait for non-throttled partitions to replicate first
    (6 to 7).foreach { id => waitForOffsetsToMatch(msgCount, id, 100 + id) }
    val unthrottledTook = System.currentTimeMillis() - start

    //Wait for replicas 0,1,2,3,4,5 to fully replicated to broker 106,107
    (0 to 2).foreach { id => waitForOffsetsToMatch(msgCount, id, 106) }
    (3 to 5).foreach { id => waitForOffsetsToMatch(msgCount, id, 107) }

    val throttledTook = System.currentTimeMillis() - start

    //Check the times for throttled/unthrottled are each side of what we expect
    val throttledLowerBound = expectedDuration * 1000 * 0.9
    val throttledUpperBound = expectedDuration * 1000 * 3
    assertTrue(s"Expected $unthrottledTook < $throttledLowerBound", unthrottledTook < throttledLowerBound)
    assertTrue(s"Expected $throttledTook > $throttledLowerBound", throttledTook > throttledLowerBound)
    assertTrue(s"Expected $throttledTook < $throttledUpperBound", throttledTook < throttledUpperBound)

    // Check the rate metric matches what we expect.
    // In a short test the brokers can be read unfairly, so assert against the average
    val rateUpperBound = throttle * 1.1
    val rateLowerBound = throttle * 0.5
    val rate = if (leaderThrottle) avRate(LeaderReplication, 100 to 105) else avRate(FollowerReplication, 106 to 107)
    assertTrue(s"Expected ${rate} < $rateUpperBound", rate < rateUpperBound)
    assertTrue(s"Expected ${rate} > $rateLowerBound", rate > rateLowerBound)
  }

  def tp(partition: Int): TopicPartition = new TopicPartition(topic, partition)

  @Test
  def shouldThrottleOldSegments(): Unit = {
    /**
      * Simple test which ensures throttled replication works when the dataset spans many segments
      */

    //2 brokers with 1MB Segment Size & 1 partition
    val config: Properties = createBrokerConfig(100, zkConnect)
    config.put("log.segment.bytes", (1024 * 1024).toString)
    brokers = Seq(createServer(fromProps(config)))
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers)

    //Write 20MBs and throttle at 5MB/s
    val msg = msg100KB
    val msgCount: Int = 200
    val expectedDuration = 4
    val throttle: Long = msg.length * msgCount / expectedDuration

    //Set the throttle to only limit leader
    adminZkClient.changeBrokerConfig(Seq(100), propsWith(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, throttle.toString))
    adminZkClient.changeTopicConfig(topic, propsWith(LeaderReplicationThrottledReplicasProp, "0:100"))

    //Add data
    addData(msgCount, msg)

    //Start the new broker (and hence start replicating)
    debug("Starting new broker")
    brokers = brokers :+ createServer(fromProps(createBrokerConfig(101, zkConnect)))
    val start = System.currentTimeMillis()

    waitForOffsetsToMatch(msgCount, 0, 101)

    val throttledTook = System.currentTimeMillis() - start

    assertTrue(s"Throttled replication of ${throttledTook}ms should be > ${expectedDuration * 1000 * 0.9}ms",
      throttledTook > expectedDuration * 1000 * 0.9)
    assertTrue(s"Throttled replication of ${throttledTook}ms should be < ${expectedDuration * 1500}ms",
      throttledTook < expectedDuration * 1000 * 1.5)
  }

  def addData(msgCount: Int, msg: Array[Byte]): Unit = {
    producer = createProducer(getBrokerListStrFromServers(brokers), acks = 0)
    (0 until msgCount).map(_ => producer.send(new ProducerRecord(topic, msg))).foreach(_.get)
    waitForOffsetsToMatch(msgCount, 0, 100)
  }

  private def waitForOffsetsToMatch(offset: Int, partitionId: Int, brokerId: Int): Unit = {
    waitUntilTrue(() => {
      offset == brokerFor(brokerId).getLogManager.getLog(new TopicPartition(topic, partitionId))
        .map(_.logEndOffset).getOrElse(0)
    }, s"Offsets did not match for partition $partitionId on broker $brokerId", 60000)
  }

  private def brokerFor(id: Int): KafkaServer = brokers.filter(_.config.brokerId == id).head

  def createBrokers(brokerIds: Seq[Int]): Unit = {
    brokerIds.foreach { id =>
      brokers = brokers :+ createServer(fromProps(createBrokerConfig(id, zkConnect)))
    }
  }

  private def avRate(replicationType: QuotaType, brokers: Seq[Int]): Double = {
    brokers.map(brokerFor).map(measuredRate(_, replicationType)).sum / brokers.length
  }

  private def measuredRate(broker: KafkaServer, repType: QuotaType): Double = {
    val metricName = broker.metrics.metricName("byte-rate", repType.toString)
    broker.metrics.metrics.asScala(metricName).metricValue.asInstanceOf[Double]
  }
}
