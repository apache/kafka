/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.kafka.api

import kafka.controller.OfflinePartition
import kafka.log.Log
import kafka.log.LogManager.RecoveryPointCheckpointFile
import kafka.server.{GlobalConfig, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.getBrokerListStrFromServers
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.LegacyRecord
import org.apache.kafka.common.record.Records.{HEADER_SIZE_UP_TO_MAGIC, SIZE_OFFSET}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}
import scala.collection.{Map, Seq}

class DropCorruptedFilesTest extends ZooKeeperTestHarness {
  val topic = "test"
  val partition = 0
  val tp = new TopicPartition(topic, partition)
  var servers: Seq[KafkaServer] = _

  @AfterEach
  override def tearDown(): Unit = {
    if (servers != null) {
      TestUtils.shutdownServers(servers)
    }
    super.tearDown()
  }

  /**
   * This test goes through the following stages with 2 data brokers, with broker0 being the initial leader
   * and broker1 being the initial follower of one partition. The partition's leadership will switch in different
   * stages.
   *
   * Stage 1:
   * At leader epoch 0, one message is produced at offset 0 and replicated to the 2 brokers.
   * Then broker1 is shutdown, and the 2nd message is produced only to the leader.
   * Messages on broker0:  [epoch 0, offset 0] [epoch 0, offset 1]
   * Messages on broker1:  [epoch 0, offset 0]
   *
   * Stage 2:
   * Broker0 is shutdown, the leadership is transferred to broker1, and the 3rd message is produced only to broker1.
   * Messages on broker0:  [epoch 0, offset 0] [epoch 0, offset 1]
   * Messages on broker1:  [epoch 0, offset 0] [epoch 3, offset 1]
   *
   * Stage 3:
   * Broker1 is shutdown. Broker0 is leader-epoch-checkpoint file is corrupted.
   * We start broker0 to make it the leader, and the leader epoch gets bumped to 5 now.
   * It will detect the corrupted leader-epoch-checkpoint file, and clear its content.
   * Then we start broker1 to make it the follower.
   * Because the leader no longer has its leader epoch history, the follower won't be able to truncate properly.
   * Thus the messages on the two brokers remain the same
   * TODO: fix the log divergence
   *
   * Stage 4:
   * Another message is produced.
   * Messages on broker0:  [epoch 0, offset 0] [epoch 0, offset 1] [epoch 5, offset 2]
   * Messages on broker1:  [epoch 0, offset 0] [epoch 3, offset 1] [epoch 5, offset 2]
   */
  @Test
  def testCorruptedLeaderEpochCheckpointOnLeader(): Unit = {
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
      .map { props => {
        props.setProperty(KafkaConfig.LiDropCorruptedFilesEnableProp, "true")
        props
      }}
      .map(KafkaConfig.fromProps)
    // start servers in reverse order to ensure broker 2 becomes the controller
    servers = serverConfigs.reverseMap{s => TestUtils.createServer(s)}
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    assertTrue(controllerId == 2)

    // create a topic
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
    val expectedReplicaAssignment = Map(0 -> List(0, 1))
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers, topicConfig = topicConfig)


    // collect the data brokers
    val adminClient = createAdminClient(servers)
    val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
    val leader = topicDescMap.get(topic).partitions().get(0).leader().id()
    assertTrue(leader == 0)

    val dataBrokers = Seq(0, 1).map { brokerId =>
      servers.find(_.config.brokerId == brokerId).get
    }
    val broker0 = dataBrokers(0)
    val broker1 = dataBrokers(1)

    // produce 1 message
    val producerConfigs = new Properties()
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "-1")
    val producer = TestUtils.createProducer(getBrokerListStrFromServers(servers))
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
      "value".getBytes(StandardCharsets.UTF_8))
    producer.send(record).get

    // wait until all servers get the message
    TestUtils.waitUntilTrue(() => {
      dataBrokers.forall { broker => broker.replicaManager.getLog(tp).get.logEndOffset == 1 }
    }, "some brokers cannot get the message")

    // shutdown broker1
    broker1.shutdown()
    // produce the 2nd message, which will only be received by broker0
    producer.send(record).get

    // shutdown broker0 and startup broker1
    broker0.shutdown()
    TestUtils.waitUntilTrue(() => {
      controller.controllerContext.partitionState(tp) == OfflinePartition
    }, s"the partition $tp does not become offline after all replicas are shutdown")
    broker1.startup()

    // ensure that the leadership has transferred to broker1
    def ensureLeader(desiredLeader: Int): Unit = {
      TestUtils.waitUntilTrue(() => {
        val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
        val currentLeader = topicDescMap.get(topic).partitions().get(0).leader()
        desiredLeader.equals(currentLeader.id())
      }, s"the leadership cannot be transferred to $desiredLeader")
    }
    ensureLeader(1)

    // produce the record in new epoch
    producer.send(record).get()
    broker1.shutdown()
    // before broker0 startup, corrupt its leader epoch cache file
    corruptLeaderEpochCheckpoint(broker0.config.get(KafkaConfig.LogDirProp) + "/" + tp + "/leader-epoch-checkpoint")


    // start broker0, which will drop its leader-epoch-checkpoint file after detecting the corruption
    broker0.startup()
    ensureLeader(leader)
    broker1.startup()

    // wait until broker1 re-joins the ISR
    TestUtils.waitUntilTrue(() => {
      val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
      val currentISR = topicDescMap.get(topic).partitions().get(0).isr()
      currentISR.size() == 2
    }, "broker1 cannot rejoin the ISR")

    // produce another message and make sure both brokers can get it
    producer.send(record).get()
    TestUtils.waitUntilTrue(() => {
      dataBrokers.forall { broker => broker.replicaManager.getLog(tp).get.logEndOffset == 3 }
    }, "some brokers cannot get the message")

    adminClient.close()
    producer.close()
  }

  private def createAdminClient(servers: Seq[KafkaServer]): Admin = {
    val config = new Properties
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName("PLAINTEXT"))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    AdminClient.create(config)
  }
  private def corruptLeaderEpochCheckpoint(checkpointFile: String): Unit = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(checkpointFile)))
    // create a file with a corrupted version number
    bw.write("100")
    bw.newLine()
    bw.close()
  }

  private def setupSingleDataBrokerClusterWithTopic(segmentBytes: Long): (KafkaServer, KafkaProducer[Array[Byte], Array[Byte]]) = {
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(2, zkConnect, false)
      .map { props => {
        props.setProperty(KafkaConfig.LiDropCorruptedFilesEnableProp, "true")
        // set the max segment size to a small value so that each path lands in a separate segment file
        props.setProperty(KafkaConfig.LogSegmentBytesProp, segmentBytes.toString)
        props.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")
        props
      }}
      .map(KafkaConfig.fromProps)
    // start servers in reverse order to ensure broker 1 becomes the controller
    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    assertTrue(controllerId == 1)

    // create a topic
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
    val expectedReplicaAssignment = Map(0 -> List(0))
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)
    val dataBroker = servers.find(_.config.brokerId == 0).get

    val producerConfigs = new Properties()
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "-1")
    val producer = TestUtils.createProducer(getBrokerListStrFromServers(servers))
    (dataBroker, producer)
  }

  @Test
  def testCorruptedLog(): Unit = {
    val (dataBroker, producer) = setupSingleDataBrokerClusterWithTopic(100)

    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
      "value".getBytes(StandardCharsets.UTF_8))
    // produce 3 message
    val totalMessages = 3
    for (_ <- 0 until totalMessages)
      producer.send(record).get

    def segmentsOnBroker = dataBroker.replicaManager.logManager.getLog(tp).get.segments

    assertEquals(totalMessages, segmentsOnBroker.numberOfSegments)

    // create corruption in the middle segment
    val middleSegment = segmentsOnBroker.get(1).get
    val segmentFile = middleSegment.log.file()
    val raf = new RandomAccessFile(segmentFile, "rw")
    val mappedLogSegment = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE_UP_TO_MAGIC)
    val currentSize = mappedLogSegment.getInt(SIZE_OFFSET)
    assertTrue(currentSize >= LegacyRecord.RECORD_OVERHEAD_V0)
    // set the size to a value that's smaller than the smallest overhead to cause a corruption
    mappedLogSegment.putInt(SIZE_OFFSET, 1)

    dataBroker.shutdown()
    // delete the broker's clean shutdown file, so that it tries to recover log segments upon startup
    val logDir = dataBroker.config.logDirs(0)
    val cleanShutdownFile = new File(logDir, Log.CleanShutdownFile)
    cleanShutdownFile.delete()

    // delete the recovery-point-offset-checkpoint file so that all log segments need to go through recovery
    val recoveryPointCheckpointFile = new File(logDir, RecoveryPointCheckpointFile)
    recoveryPointCheckpointFile.delete()

    dataBroker.replicaManager.logManager
    dataBroker.startup()

    // verify that the segments after the corrupted are deleted and the corrupted segment is truncated to size 0
    assertEquals(2, segmentsOnBroker.numberOfSegments)
    assertEquals(0, segmentsOnBroker.lastSegment.get.size)

    producer.close()
  }

  @Test
  def testUnexpectedExceptionDuringLogLoading(): Unit = {
    val (dataBroker, producer) = setupSingleDataBrokerClusterWithTopic(100)

    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
      "value".getBytes(StandardCharsets.UTF_8))
    // produce 3 message
    val totalMessages = 3
    for (_ <- 0 until totalMessages)
      producer.send(record).get

    def segmentsOnBroker = dataBroker.replicaManager.logManager.getLog(tp).get.segments

    assertEquals(totalMessages, segmentsOnBroker.numberOfSegments)

    dataBroker.shutdown()
    // delete the broker's clean shutdown file, so that it tries to recover log segments upon startup
    val logDir = dataBroker.config.logDirs(0)
    val cleanShutdownFile = new File(logDir, Log.CleanShutdownFile)
    cleanShutdownFile.delete()
    // delete the recovery-point-offset-checkpoint file so that all log segments need to go through recovery
    val recoveryPointCheckpointFile = new File(logDir, RecoveryPointCheckpointFile)
    recoveryPointCheckpointFile.delete()

    GlobalConfig.logRecoveryShouldThrowException = true
    dataBroker.replicaManager.logManager
    dataBroker.startup()

    // verify that only the active segment is left with a size of 0
    assertEquals(1, segmentsOnBroker.numberOfSegments)
    assertEquals(0, segmentsOnBroker.lastSegment.get.size)

    // verify that we can still produce messages after the log dir has been cleaned
    // produce 3 message
    for (i <- 0 until totalMessages)
      assertEquals(i, producer.send(record).get.offset())
    assertEquals(totalMessages, segmentsOnBroker.numberOfSegments)
    producer.close()
  }

}
