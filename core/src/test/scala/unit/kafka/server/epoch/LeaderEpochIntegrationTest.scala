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
package kafka.server.epoch

import java.util.{Map => JMap}

import kafka.server.KafkaConfig._
import kafka.server.{BlockingSend, KafkaServer, ReplicaFetcherBlockingSend}
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.{LogContext, SystemTime}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys

import org.junit.Assert._
import org.junit.{After, Test}
import org.apache.kafka.common.requests.{EpochEndOffset, OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse}

import scala.collection.JavaConverters._
import scala.collection.Map

class LeaderEpochIntegrationTest extends ZooKeeperTestHarness with Logging {
  var brokers: Seq[KafkaServer] = null
  val topic1 = "foo"
  val topic2 = "bar"
  val t1p0 = new TopicPartition(topic1, 0)
  val t1p1 = new TopicPartition(topic1, 1)
  val t1p2 = new TopicPartition(topic1, 2)
  val t2p0 = new TopicPartition(topic2, 0)
  val t2p2 = new TopicPartition(topic2, 2)
  val tp = t1p0
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null

  @After
  override def tearDown() {
    if (producer != null)
      producer.close()
    TestUtils.shutdownServers(brokers)
    super.tearDown()
  }

  @Test
  def shouldAddCurrentLeaderEpochToMessagesAsTheyAreWrittenToLeader() {
    brokers = (0 to 1).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }

    // Given two topics with replication of a single partition
    for (topic <- List(topic1, topic2)) {
      createTopic(zkClient, topic, Map(0 -> Seq(0, 1)), servers = brokers)
    }

    // When we send four messages
    sendFourMessagesToEachTopic()

    //Then they should be stamped with Leader Epoch 0
    var expectedLeaderEpoch = 0
    waitUntilTrue(() => messagesHaveLeaderEpoch(brokers(0), expectedLeaderEpoch, 0), "Leader epoch should be 0")

    //Given we then bounce the leader
    brokers(0).shutdown()
    brokers(0).startup()

    //Then LeaderEpoch should now have changed from 0 -> 1
    expectedLeaderEpoch = 1
    waitForEpochChangeTo(topic1, 0, expectedLeaderEpoch)
    waitForEpochChangeTo(topic2, 0, expectedLeaderEpoch)

    //Given we now send messages
    sendFourMessagesToEachTopic()

    //The new messages should be stamped with LeaderEpoch = 1
    waitUntilTrue(() => messagesHaveLeaderEpoch(brokers(0), expectedLeaderEpoch, 4), "Leader epoch should be 1")
  }

  @Test
  def shouldSendLeaderEpochRequestAndGetAResponse(): Unit = {

    //3 brokers, put partition on 100/101 and then pretend to be 102
    brokers = (100 to 102).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    adminZkClient.createOrUpdateTopicPartitionAssignmentPathInZK(topic1, Map(
      0 -> Seq(100),
      1 -> Seq(101)
    ))
    adminZkClient.createOrUpdateTopicPartitionAssignmentPathInZK(topic2, Map(
      0 -> Seq(100)
    ))

    //Send messages equally to the two partitions, then half as many to a third
    producer = createProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1)
    (0 until 10).foreach { _ =>
      producer.send(new ProducerRecord(topic1, 0, null, "IHeartLogs".getBytes))
    }
    (0 until 20).foreach { _ =>
      producer.send(new ProducerRecord(topic1, 1, null, "OhAreThey".getBytes))
    }
    (0 until 30).foreach { _ =>
      producer.send(new ProducerRecord(topic2, 0, null, "IReallyDo".getBytes))
    }
    producer.flush()

    val fetcher0 = new TestFetcherThread(sender(from = brokers(2), to = brokers(0)))
    val epochsRequested = Map(t1p0 -> 0, t1p1 -> 0, t2p0 -> 0, t2p2 -> 0)

    //When
    val offsetsForEpochs = fetcher0.leaderOffsetsFor(epochsRequested)

    //Then end offset should be correct
    assertEquals(10, offsetsForEpochs(t1p0).endOffset)
    assertEquals(30, offsetsForEpochs(t2p0).endOffset)

    //And should get no leader for partition error from t1p1 (as it's not on broker 0)
    assertTrue(offsetsForEpochs(t1p1).hasError)
    assertEquals(UNKNOWN_TOPIC_OR_PARTITION, offsetsForEpochs(t1p1).error)
    assertEquals(UNDEFINED_EPOCH_OFFSET, offsetsForEpochs(t1p1).endOffset)

    //Repointing to broker 1 we should get the correct offset for t1p1
    val fetcher1 = new TestFetcherThread(sender(from = brokers(2), to = brokers(1)))
    val offsetsForEpochs1 = fetcher1.leaderOffsetsFor(epochsRequested)
    assertEquals(20, offsetsForEpochs1(t1p1).endOffset)
  }

  @Test
  def shouldIncreaseLeaderEpochBetweenLeaderRestarts(): Unit = {

    //Setup: we are only interested in the single partition on broker 101
    brokers = Seq(100, 101).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    def leo() = brokers(1).replicaManager.getReplica(tp).get.logEndOffset.messageOffset
    adminZkClient.createOrUpdateTopicPartitionAssignmentPathInZK(tp.topic, Map(tp.partition -> Seq(101)))
    producer = createProducer(getBrokerListStrFromServers(brokers), retries = 10, acks = -1)

    //1. Given a single message
    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    var fetcher = new TestFetcherThread(sender(brokers(0), brokers(1)))

    //Then epoch should be 0 and leo: 1
    var offset = fetcher.leaderOffsetsFor(Map(tp -> 0))(tp).endOffset()
    assertEquals(1, offset)
    assertEquals(leo(), offset)


    //2. When broker is bounced
    brokers(1).shutdown()
    brokers(1).startup()

    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    fetcher = new TestFetcherThread(sender(brokers(0), brokers(1)))


    //Then epoch 0 should still be the start offset of epoch 1
    offset = fetcher.leaderOffsetsFor(Map(tp -> 0))(tp).endOffset()
    assertEquals(1, offset)

    //Then epoch 2 should be the leo (NB: The leader epoch goes up in factors of 2 - This is because we have to first change leader to -1 and then change it again to the live replica)
    assertEquals(2, fetcher.leaderOffsetsFor(Map(tp -> 2))(tp).endOffset())
    assertEquals(leo(), fetcher.leaderOffsetsFor(Map(tp -> 2))(tp).endOffset())


    //3. When broker is bounced again
    brokers(1).shutdown()
    brokers(1).startup()

    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    fetcher = new TestFetcherThread(sender(brokers(0), brokers(1)))


    //Then Epoch 0 should still map to offset 1
    assertEquals(1, fetcher.leaderOffsetsFor(Map(tp -> 0))(tp).endOffset())

    //Then Epoch 2 should still map to offset 2
    assertEquals(2, fetcher.leaderOffsetsFor(Map(tp -> 2))(tp).endOffset())

    //Then Epoch 4 should still map to offset 2
    assertEquals(3, fetcher.leaderOffsetsFor(Map(tp -> 4))(tp).endOffset())
    assertEquals(leo(), fetcher.leaderOffsetsFor(Map(tp -> 4))(tp).endOffset())

    //Adding some extra assertions here to save test setup.
    shouldSupportRequestsForEpochsNotOnTheLeader(fetcher)
  }

  //Appended onto the previous test to save on setup cost.
  def shouldSupportRequestsForEpochsNotOnTheLeader(fetcher: TestFetcherThread): Unit = {
    /**
      * Asking for an epoch not present on the leader should return the
      * next matching epoch, unless there isn't any, which should return
      * undefined.
      */

    val epoch1 = Map(t1p0 -> 1)
    assertEquals(1, fetcher.leaderOffsetsFor(epoch1)(t1p0).endOffset())

    val epoch3 = Map(t1p0 -> 3)
    assertEquals(2, fetcher.leaderOffsetsFor(epoch3)(t1p0).endOffset())

    val epoch5 = Map(t1p0 -> 5)
    assertEquals(-1, fetcher.leaderOffsetsFor(epoch5)(t1p0).endOffset())
  }

  private def sender(from: KafkaServer, to: KafkaServer): BlockingSend = {
    val endPoint = from.metadataCache.getAliveBrokers.find(_.id == to.config.brokerId).get.brokerEndPoint(from.config.interBrokerListenerName)
    new ReplicaFetcherBlockingSend(endPoint, from.config, new Metrics(), new SystemTime(), 42, "TestFetcher", new LogContext())
  }

  private def waitForEpochChangeTo(topic: String, partition: Int, epoch: Int): Unit = {
    TestUtils.waitUntilTrue(() => {
      brokers(0).metadataCache.getPartitionInfo(topic, partition) match {
        case Some(m) => m.basePartitionState.leaderEpoch == epoch
        case None => false
      }
    }, "Epoch didn't change")
  }

  private  def messagesHaveLeaderEpoch(broker: KafkaServer, expectedLeaderEpoch: Int, minOffset: Int): Boolean = {
    var result = true
    for (topic <- List(topic1, topic2)) {
      val tp = new TopicPartition(topic, 0)
      val leo = broker.getLogManager().getLog(tp).get.logEndOffset
      result = result && leo > 0 && brokers.forall { broker =>
        broker.getLogManager().getLog(tp).get.logSegments.iterator.forall { segment =>
          if (segment.read(minOffset, None, Integer.MAX_VALUE) == null) {
            false
          } else {
            segment.read(minOffset, None, Integer.MAX_VALUE)
              .records.batches().iterator().asScala.forall(
              expectedLeaderEpoch == _.partitionLeaderEpoch()
            )
          }
        }
      }
    }
    result
  }

  private def sendFourMessagesToEachTopic() = {
    val testMessageList1 = List("test1", "test2", "test3", "test4")
    val testMessageList2 = List("test5", "test6", "test7", "test8")
    val producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5, keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
    val records =
      testMessageList1.map(m => new ProducerRecord(topic1, m, m)) ++
        testMessageList2.map(m => new ProducerRecord(topic2, m, m))
    records.map(producer.send).foreach(_.get)
    producer.close()
  }

  /**
    * Simulates how the Replica Fetcher Thread requests leader offsets for epochs
    */
  private[epoch] class TestFetcherThread(sender: BlockingSend) extends Logging {

    def leaderOffsetsFor(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset] = {
      val request = new OffsetsForLeaderEpochRequest.Builder(ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(), toJavaFormat(partitions))
      val response = sender.sendRequest(request)
      response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse].responses.asScala
    }

    def toJavaFormat(partitions: Map[TopicPartition, Int]): JMap[TopicPartition, Integer] = {
      partitions.map { case (tp, epoch) => tp -> epoch.asInstanceOf[Integer] }.toMap.asJava
    }
  }
}
