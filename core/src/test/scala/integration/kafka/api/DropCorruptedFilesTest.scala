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
import kafka.server.{KafkaConfig, KafkaServer, ReplicaManager}
import kafka.utils.Implicits.PropertiesOps
import kafka.utils.TestUtils.getBrokerListStrFromServers
import kafka.utils.{Exit, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}
import scala.collection.{Map, Seq}

class DropCorruptedFilesTest extends ZooKeeperTestHarness {
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
    val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    assertTrue(controllerId == 2)

    // create a topic
    val topic = "test"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
    val expectedReplicaAssignment = Map(0 -> List(0, 1))
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)


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
    servers.foreach{_.shutdown()}
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


}
