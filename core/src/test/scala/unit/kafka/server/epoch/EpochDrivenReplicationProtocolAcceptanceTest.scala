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

import java.io.{File, RandomAccessFile}
import java.util.Properties
import kafka.api.ApiVersion
import kafka.log.{UnifiedLog, LogLoader}
import kafka.server.KafkaConfig._
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.tools.DumpLogSegments
import kafka.utils.{CoreUtils, Logging, TestUtils}
import kafka.utils.TestUtils._
import kafka.server.QuorumTestHarness
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{ListBuffer => Buffer}
import scala.collection.Seq

/**
  * These tests were written to assert the addition of leader epochs to the replication protocol fix the problems
  * described in KIP-101.
  *
  * https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation
  *
  * A test which validates the end to end workflow is also included.
  */
class EpochDrivenReplicationProtocolAcceptanceTest extends QuorumTestHarness with Logging {

  // Set this to KAFKA_0_11_0_IV1 to demonstrate the tests failing in the pre-KIP-101 case
  val apiVersion = ApiVersion.latestVersion
  val topic = "topic1"
  val msg = new Array[Byte](1000)
  val msgBigger = new Array[Byte](10000)
  var brokers: Seq[KafkaServer] = null
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    producer.close()
    TestUtils.shutdownServers(brokers)
    super.tearDown()
  }

  @Test
  def shouldFollowLeaderEpochBasicWorkflow(): Unit = {

    //Given 2 brokers
    brokers = (100 to 101).map(createBrokerForId(_))

    //A single partition topic with 2 replicas
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers)
    producer = createProducer
    val tp = new TopicPartition(topic, 0)

    //When one record is written to the leader
    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    //The message should have epoch 0 stamped onto it in both leader and follower
    assertEquals(0, latestRecord(leader).partitionLeaderEpoch)
    assertEquals(0, latestRecord(follower).partitionLeaderEpoch)

    //Both leader and follower should have recorded Epoch 0 at Offset 0
    assertEquals(Buffer(EpochEntry(0, 0)), epochCache(leader).epochEntries)
    assertEquals(Buffer(EpochEntry(0, 0)), epochCache(follower).epochEntries)

    //Bounce the follower
    bounce(follower)
    awaitISR(tp)

    //Nothing happens yet as we haven't sent any new messages.
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(leader).epochEntries)
    assertEquals(Buffer(EpochEntry(0, 0)), epochCache(follower).epochEntries)

    //Send a message
    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    //Epoch1 should now propagate to the follower with the written message
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(leader).epochEntries)
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(follower).epochEntries)

    //The new message should have epoch 1 stamped
    assertEquals(1, latestRecord(leader).partitionLeaderEpoch())
    assertEquals(1, latestRecord(follower).partitionLeaderEpoch())

    //Bounce the leader. Epoch -> 2
    bounce(leader)
    awaitISR(tp)

    //Epochs 2 should be added to the leader, but not on the follower (yet), as there has been no replication.
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(leader).epochEntries)
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(follower).epochEntries)

    //Send a message
    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    //This should case epoch 2 to propagate to the follower
    assertEquals(2, latestRecord(leader).partitionLeaderEpoch())
    assertEquals(2, latestRecord(follower).partitionLeaderEpoch())

    //The leader epoch files should now match on leader and follower
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(leader).epochEntries)
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(follower).epochEntries)
  }

  @Test
  def shouldNotAllowDivergentLogs(): Unit = {
    //Given two brokers
    brokers = (100 to 101).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    val broker100 = brokers(0)
    val broker101 = brokers(1)

    //A single partition topic with 2 replicas
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers)
    producer = createProducer

    //Write 10 messages (ensure they are not batched so we can truncate in the middle below)
    (0 until 10).foreach { i =>
      producer.send(new ProducerRecord(topic, 0, s"$i".getBytes, msg)).get()
    }

    //Stop the brokers (broker 101 first so that 100 is the leader)
    broker101.shutdown()
    broker100.shutdown()

    //Delete the clean shutdown file to simulate crash
    new File(broker100.config.logDirs.head, LogLoader.CleanShutdownFile).delete()

    //Delete 5 messages from the leader's log on 100
    deleteMessagesFromLogFile(5 * msg.length, broker100, 0)

    //Restart broker 100
    broker100.startup()

    //Bounce the producer (this is required since the broker uses a random port)
    producer.close()
    producer = createProducer

    //Write ten additional messages
    (11 until 20).map { i =>
      producer.send(new ProducerRecord(topic, 0, s"$i".getBytes, msg))
    }.foreach(_.get())

    //Start broker 101 (we expect it to truncate to match broker 100's log)
    broker101.startup()

    //Wait for replication to resync
    waitForLogsToMatch(broker100, broker101)

    assertEquals(getLogFile(brokers(0), 0).length, getLogFile(brokers(1), 0).length, "Log files should match Broker0 vs Broker 1")
  }

  //We can reproduce the pre-KIP-101 failure of this test by setting KafkaConfig.InterBrokerProtocolVersionProp = KAFKA_0_11_0_IV1
  @Test
  def offsetsShouldNotGoBackwards(): Unit = {

    //Given two brokers
    brokers = (100 to 101).map(createBrokerForId(_))

    //A single partition topic with 2 replicas
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers)
    producer = createBufferingProducer

    //Write 100 messages
    (0 until 100).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
      producer.flush()
    }

    //Stop the brokers
    brokers.foreach { b => b.shutdown() }

    //Delete the clean shutdown file to simulate crash
    new File(brokers(0).config.logDirs(0), LogLoader.CleanShutdownFile).delete()

    //Delete half the messages from the log file
    deleteMessagesFromLogFile(getLogFile(brokers(0), 0).length() / 2, brokers(0), 0)

    //Start broker 100 again
    brokers(0).startup()

    //Bounce the producer (this is required, although I'm unsure as to why?)
    producer.close()
    producer = createBufferingProducer

    //Write two large batches of messages. This will ensure that the LeO of the follower's log aligns with the middle
    //of the a compressed message set in the leader (which, when forwarded, will result in offsets going backwards)
    (0 until 77).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
    }
    producer.flush()
    (0 until 77).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
    }
    producer.flush()

    printSegments()

    //Start broker 101. When it comes up it should read a whole batch of messages from the leader.
    //As the chronology is lost we would end up with non-monotonic offsets (pre kip-101)
    brokers(1).startup()

    //Wait for replication to resync
    waitForLogsToMatch(brokers(0), brokers(1))

    printSegments()

    //Shut down broker 100, so we read from broker 101 which should have corrupted
    brokers(0).shutdown()

    //Search to see if we have non-monotonic offsets in the log
    startConsumer()
    val records = TestUtils.pollUntilAtLeastNumRecords(consumer, 100)
    var prevOffset = -1L
    records.foreach { r =>
      assertTrue(r.offset > prevOffset, s"Offset $prevOffset came before ${r.offset} ")
      prevOffset = r.offset
    }

    //Are the files identical?
    assertEquals(getLogFile(brokers(0), 0).length, getLogFile(brokers(1), 0).length, "Log files should match Broker0 vs Broker 1")
  }

  /**
    * Unlike the tests above, this test doesn't fail prior to the Leader Epoch Change. I was unable to find a deterministic
    * method for recreating the fast leader change bug.
    */
  @Test
  def shouldSurviveFastLeaderChange(): Unit = {
    val tp = new TopicPartition(topic, 0)

    //Given 2 brokers
    brokers = (100 to 101).map(createBrokerForId(_))

    //A single partition topic with 2 replicas
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers)
    producer = createProducer

    //Kick off with a single record
    producer.send(new ProducerRecord(topic, 0, null, msg)).get
    var messagesWritten = 1

    //Now invoke the fast leader change bug
    (0 until 5).foreach { i =>
      val leaderId = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
      val leader = brokers.filter(_.config.brokerId == leaderId)(0)
      val follower = brokers.filter(_.config.brokerId != leaderId)(0)

      producer.send(new ProducerRecord(topic, 0, null, msg)).get
      messagesWritten += 1

      //As soon as it replicates, bounce the follower
      bounce(follower)

      log(leader, follower)
      awaitISR(tp)

      //Then bounce the leader
      bounce(leader)

      log(leader, follower)
      awaitISR(tp)

      //Ensure no data was lost
      assertTrue(brokers.forall { broker => getLog(broker, 0).logEndOffset == messagesWritten })
    }
  }

  @Test
  def logsShouldNotDivergeOnUncleanLeaderElections(): Unit = {

    // Given two brokers, unclean leader election is enabled
    brokers = (100 to 101).map(createBrokerForId(_, enableUncleanLeaderElection = true))

    // A single partition topic with 2 replicas, min.isr = 1
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(100, 101)), brokers,
      CoreUtils.propsWith((KafkaConfig.MinInSyncReplicasProp, "1")))

    producer = TestUtils.createProducer(plaintextBootstrapServers(brokers), acks = 1)

    // Write one message while both brokers are up
    (0 until 1).foreach { i =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
      producer.flush()}

    // Since we use producer with acks = 1, make sure that logs match for the first epoch
    waitForLogsToMatch(brokers(0), brokers(1))

    // shutdown broker 100
    brokers(0).shutdown()

    //Write 1 message
    (0 until 1).foreach { i =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
      producer.flush()}

    brokers(1).shutdown()
    brokers(0).startup()

    //Bounce the producer (this is required, probably because the broker port changes on restart?)
    producer.close()
    producer = TestUtils.createProducer(plaintextBootstrapServers(brokers), acks = 1)

    //Write 3 messages
    (0 until 3).foreach { i =>
      producer.send(new ProducerRecord(topic, 0, null, msgBigger))
      producer.flush()}

    brokers(0).shutdown()
    brokers(1).startup()

    //Bounce the producer (this is required, probably because the broker port changes on restart?)
    producer.close()
    producer = TestUtils.createProducer(plaintextBootstrapServers(brokers), acks = 1)

    //Write 1 message
    (0 until 1).foreach { i =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
      producer.flush()}

    brokers(1).shutdown()
    brokers(0).startup()

    //Bounce the producer (this is required, probably because the broker port changes on restart?)
    producer.close()
    producer = TestUtils.createProducer(plaintextBootstrapServers(brokers), acks = 1)

    //Write 2 messages
    (0 until 2).foreach { i =>
      producer.send(new ProducerRecord(topic, 0, null, msgBigger))
      producer.flush()}

    printSegments()

    brokers(1).startup()

    waitForLogsToMatch(brokers(0), brokers(1))
    printSegments()

    def crcSeq(broker: KafkaServer, partition: Int = 0): Seq[Long] = {
      val batches = getLog(broker, partition).activeSegment.read(0, Integer.MAX_VALUE)
        .records.batches().asScala.toSeq
      batches.map(_.checksum)
    }
    assertTrue(crcSeq(brokers(0)) == crcSeq(brokers(1)),
               s"Logs on Broker 100 and Broker 101 should match")
  }

  private def log(leader: KafkaServer, follower: KafkaServer): Unit = {
    info(s"Bounce complete for follower ${follower.config.brokerId}")
    info(s"Leader: leo${leader.config.brokerId}: " + getLog(leader, 0).logEndOffset + " cache: " + epochCache(leader).epochEntries)
    info(s"Follower: leo${follower.config.brokerId}: " + getLog(follower, 0).logEndOffset + " cache: " + epochCache(follower).epochEntries)
  }

  private def waitForLogsToMatch(b1: KafkaServer, b2: KafkaServer, partition: Int = 0): Unit = {
    TestUtils.waitUntilTrue(() => {getLog(b1, partition).logEndOffset == getLog(b2, partition).logEndOffset}, "Logs didn't match.")
  }

  private def printSegments(): Unit = {
    info("Broker0:")
    DumpLogSegments.main(Seq("--files", getLogFile(brokers(0), 0).getCanonicalPath).toArray)
    info("Broker1:")
    DumpLogSegments.main(Seq("--files", getLogFile(brokers(1), 0).getCanonicalPath).toArray)
  }

  private def startConsumer(): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val consumerConfig = new Properties()
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, plaintextBootstrapServers(brokers))
    consumerConfig.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(getLogFile(brokers(1), 0).length() * 2))
    consumerConfig.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(getLogFile(brokers(1), 0).length() * 2))
    consumer = new KafkaConsumer(consumerConfig, new ByteArrayDeserializer, new ByteArrayDeserializer)
    consumer.assign(List(new TopicPartition(topic, 0)).asJava)
    consumer.seek(new TopicPartition(topic, 0), 0)
    consumer
  }

  private def deleteMessagesFromLogFile(bytes: Long, broker: KafkaServer, partitionId: Int): Unit = {
    val logFile = getLogFile(broker, partitionId)
    val writable = new RandomAccessFile(logFile, "rwd")
    writable.setLength(logFile.length() - bytes)
    writable.close()
  }

  private def createBufferingProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    TestUtils.createProducer(plaintextBootstrapServers(brokers),
      acks = -1,
      lingerMs = 10000,
      batchSize = msg.length * 1000,
      compressionType = "snappy")
  }

  private def getLogFile(broker: KafkaServer, partition: Int): File = {
    val log: UnifiedLog = getLog(broker, partition)
    log.flush(false)
    log.dir.listFiles.filter(_.getName.endsWith(".log"))(0)
  }

  private def getLog(broker: KafkaServer, partition: Int): UnifiedLog = {
    broker.logManager.getLog(new TopicPartition(topic, partition)).orNull
  }

  private def bounce(follower: KafkaServer): Unit = {
    follower.shutdown()
    follower.startup()
    producer.close()
    producer = createProducer //TODO not sure why we need to recreate the producer, but it doesn't reconnect if we don't
  }

  private def epochCache(broker: KafkaServer): LeaderEpochFileCache = getLog(broker, 0).leaderEpochCache.get

  private def latestRecord(leader: KafkaServer, offset: Int = -1, partition: Int = 0): RecordBatch = {
    getLog(leader, partition).activeSegment.read(0, Integer.MAX_VALUE)
      .records.batches().asScala.toSeq.last
  }

  private def awaitISR(tp: TopicPartition): Unit = {
    TestUtils.waitUntilTrue(() => {
      leader.replicaManager.onlinePartition(tp).get.inSyncReplicaIds.size == 2
    }, "Timed out waiting for replicas to join ISR")
  }

  private def createProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    TestUtils.createProducer(plaintextBootstrapServers(brokers), acks = -1)
  }

  private def leader: KafkaServer = {
    assertEquals(2, brokers.size)
    val leaderId = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
    brokers.filter(_.config.brokerId == leaderId).head
  }

  private def follower: KafkaServer = {
    assertEquals(2, brokers.size)
    val leader = zkClient.getLeaderForPartition(new TopicPartition(topic, 0)).get
    brokers.filter(_.config.brokerId != leader).head
  }

  private def createBrokerForId(id: Int, enableUncleanLeaderElection: Boolean = false): KafkaServer = {
    val config = createBrokerConfig(id, zkConnect)
    TestUtils.setIbpAndMessageFormatVersions(config, apiVersion)
    config.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, enableUncleanLeaderElection.toString)
    createServer(fromProps(config))
  }
}
