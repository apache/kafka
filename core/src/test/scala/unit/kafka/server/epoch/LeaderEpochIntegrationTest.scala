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

import kafka.cluster.BrokerEndPoint
import kafka.server.KafkaConfig._
import kafka.server.{BlockingSend, BrokerBlockingSender, KafkaBroker, QuorumTestHarness}
import kafka.utils.Implicits._
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.{LogContext, SystemTime}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderPartition, OffsetForLeaderTopic, OffsetForLeaderTopicCollection}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse}
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq}
import scala.collection.mutable.ListBuffer

class LeaderEpochIntegrationTest extends QuorumTestHarness with Logging {
  var brokers: ListBuffer[KafkaBroker] = ListBuffer()
  val topic1 = "foo"
  val topic2 = "bar"
  val t1p0 = new TopicPartition(topic1, 0)
  val t1p1 = new TopicPartition(topic1, 1)
  val t1p2 = new TopicPartition(topic1, 2)
  val t2p0 = new TopicPartition(topic2, 0)
  val t2p2 = new TopicPartition(topic2, 2)
  val tp = t1p0
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  @AfterEach
  override def tearDown(): Unit = {
    if (producer != null)
      producer.close()
    TestUtils.shutdownServers(brokers)
    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def shouldAddCurrentLeaderEpochToMessagesAsTheyAreWrittenToLeader(quorum: String): Unit = {
    brokers ++= (0 to 1).map { id => createBroker(fromProps(createBrokerConfig(id, zkConnectOrNull))) }

    // Given two topics with replication of a single partition
    for (topic <- List(topic1, topic2)) {
      createTopic(topic, Map(0 -> Seq(0, 1)))
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def shouldSendLeaderEpochRequestAndGetAResponse(quorum: String): Unit = {

    //3 brokers, put partition on 100/101 and then pretend to be 102
    brokers ++= (100 to 102).map { id => createBroker(fromProps(createBrokerConfig(id, zkConnectOrNull))) }

    val assignment1 = Map(0 -> Seq(100), 1 -> Seq(101))
    createTopic(topic1, assignment1)

    val assignment2 = Map(0 -> Seq(100))
    createTopic(topic2, assignment2)

    //Send messages equally to the two partitions, then half as many to a third
    producer = createProducer(plaintextBootstrapServers(brokers), acks = -1)
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
    fetcher0.close()

    //Then end offset should be correct
    assertEquals(10, offsetsForEpochs(t1p0).endOffset)
    assertEquals(30, offsetsForEpochs(t2p0).endOffset)

    //And should get no leader for partition error from t1p1 (as it's not on broker 0)
    assertEquals(NOT_LEADER_OR_FOLLOWER.code, offsetsForEpochs(t1p1).errorCode)
    assertEquals(UNDEFINED_EPOCH_OFFSET, offsetsForEpochs(t1p1).endOffset)

    //Repointing to broker 1 we should get the correct offset for t1p1
    val fetcher1 = new TestFetcherThread(sender(from = brokers(2), to = brokers(1)))
    val offsetsForEpochs1 = fetcher1.leaderOffsetsFor(epochsRequested)
    assertEquals(20, offsetsForEpochs1(t1p1).endOffset)
    fetcher1.close()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def shouldIncreaseLeaderEpochBetweenLeaderRestarts(quorum: String): Unit = {
    //Setup: we are only interested in the single partition on broker 101
    brokers += createBroker(fromProps(createBrokerConfig(100, zkConnectOrNull)))
    if (isKRaftTest()) {
      assertEquals(controllerServer.config.nodeId, TestUtils.waitUntilQuorumLeaderElected(controllerServer))
    } else {
      assertEquals(100, TestUtils.waitUntilControllerElected(zkClient))
    }

    brokers += createBroker(fromProps(createBrokerConfig(101, zkConnectOrNull)))

    def leo() = brokers(1).replicaManager.localLog(tp).get.logEndOffset

    createTopic(tp.topic, Map(tp.partition -> Seq(101)))
    producer = createProducer(plaintextBootstrapServers(brokers), acks = -1)

    //1. Given a single message
    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    var fetcher = new TestFetcherThread(sender(brokers(0), brokers(1)))

    //Then epoch should be 0 and leo: 1
    var epochEndOffset = fetcher.leaderOffsetsFor(Map(tp -> 0))(tp)
    assertEquals(0, epochEndOffset.leaderEpoch)
    assertEquals(1, epochEndOffset.endOffset)
    assertEquals(1, leo())

    //2. When broker is bounced
    brokers(1).shutdown()
    brokers(1).startup()

    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    fetcher.close()
    fetcher = new TestFetcherThread(sender(brokers(0), brokers(1)))

    //Then epoch 0 should still be the start offset of epoch 1
    epochEndOffset = fetcher.leaderOffsetsFor(Map(tp -> 0))(tp)
    assertEquals(1, epochEndOffset.endOffset)
    assertEquals(0, epochEndOffset.leaderEpoch)

    //No data written in epoch 1
    epochEndOffset = fetcher.leaderOffsetsFor(Map(tp -> 1))(tp)
    assertEquals(0, epochEndOffset.leaderEpoch)
    assertEquals(1, epochEndOffset.endOffset)

    //Then epoch 2 should be the leo (NB: The leader epoch goes up in factors of 2 -
    //This is because we have to first change leader to -1 and then change it again to the live replica)
    //Note that the expected leader changes depend on the controller being on broker 100, which is not restarted
    epochEndOffset = fetcher.leaderOffsetsFor(Map(tp -> 2))(tp)
    assertEquals(2, epochEndOffset.leaderEpoch)
    assertEquals(2, epochEndOffset.endOffset)
    assertEquals(2, leo())

    //3. When broker is bounced again
    brokers(1).shutdown()
    brokers(1).startup()

    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    fetcher.close()
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
    fetcher.close()
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

  private def sender(from: KafkaBroker, to: KafkaBroker): BlockingSend = {
    val node = from.metadataCache.getAliveBrokerNode(to.config.brokerId,
      from.config.interBrokerListenerName).get
    val endPoint = new BrokerEndPoint(node.id(), node.host(), node.port())
    new BrokerBlockingSender(endPoint, from.config, new Metrics(), new SystemTime(), 42, "TestFetcher", new LogContext())
  }

  private def waitForEpochChangeTo(topic: String, partition: Int, epoch: Int): Unit = {
    TestUtils.waitUntilTrue(() => {
      brokers(0).metadataCache.getPartitionInfo(topic, partition).exists(_.leaderEpoch == epoch)
    }, "Epoch didn't change")
  }

  private  def messagesHaveLeaderEpoch(broker: KafkaBroker, expectedLeaderEpoch: Int, minOffset: Int): Boolean = {
    var result = true
    for (topic <- List(topic1, topic2)) {
      val tp = new TopicPartition(topic, 0)
      val leo = broker.logManager.getLog(tp).get.logEndOffset
      result = result && leo > 0 && brokers.forall { broker =>
        broker.logManager.getLog(tp).get.logSegments.stream.allMatch { segment =>
          if (segment.read(minOffset, Integer.MAX_VALUE) == null) {
            false
          } else {
            segment.read(minOffset, Integer.MAX_VALUE).records.batches().iterator().asScala.forall(
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
    val producer = TestUtils.createProducer(plaintextBootstrapServers(brokers),
      keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
    val records =
      testMessageList1.map(m => new ProducerRecord(topic1, m, m)) ++
        testMessageList2.map(m => new ProducerRecord(topic2, m, m))
    records.map(producer.send).foreach(_.get)
    producer.close()
  }

  private def createTopic(topic: String, partitionReplicaAssignment: collection.Map[Int, Seq[Int]]): Unit = {
    resource(createAdminClient(brokers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))) { admin =>
      TestUtils.createTopicWithAdmin(
        admin = admin,
        topic = topic,
        replicaAssignment = partitionReplicaAssignment,
        brokers = brokers,
        controllers = controllerServers
      )
    }
  }

  /**
    * Simulates how the Replica Fetcher Thread requests leader offsets for epochs
    */
  private[epoch] class TestFetcherThread(sender: BlockingSend) extends Logging {

    def leaderOffsetsFor(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset] = {
      val topics = new OffsetForLeaderTopicCollection(partitions.size)
      partitions.forKeyValue { (topicPartition, leaderEpoch) =>
        var topic = topics.find(topicPartition.topic)
        if (topic == null) {
          topic = new OffsetForLeaderTopic().setTopic(topicPartition.topic)
          topics.add(topic)
        }
        topic.partitions.add(new OffsetForLeaderPartition()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(leaderEpoch))
      }

      val request = OffsetsForLeaderEpochRequest.Builder.forFollower(
        ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion, topics, 1)
      val response = sender.sendRequest(request)
      response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse].data.topics.asScala.flatMap { topic =>
        topic.partitions.asScala.map { partition =>
          new TopicPartition(topic.topic, partition.partition) -> partition
        }
      }.toMap
    }

    def close(): Unit = sender.close()
  }
}
