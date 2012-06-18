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

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils._
import kafka.producer.ProducerData
import kafka.serializer.StringEncoder
import kafka.admin.CreateTopicCommand
import kafka.cluster.{Replica, Partition, Broker}
import kafka.utils.{MockTime, TestUtils}
import junit.framework.Assert._
import java.io.File
import kafka.log.Log

class ReplicaFetchTest extends JUnit3Suite with ZooKeeperTestHarness  {
  val props = createBrokerConfigs(2)
  val configs = props.map(p => new KafkaConfig(p) { override val flushInterval = 1})
  var brokers: Seq[KafkaServer] = null
  val topic = "foobar"

  override def setUp() {
    super.setUp()
    brokers = configs.map(config => TestUtils.createServer(config))
  }

  override def tearDown() {
    super.tearDown()
    brokers.foreach(_.shutdown())
  }

  def testReplicaFetcherThread() {
    val partition = 0
    val testMessageList = List("test1", "test2", "test3", "test4")
    val leaderBrokerId = configs.head.brokerId
    val followerBrokerId = configs.last.brokerId
    val leaderBroker = new Broker(leaderBrokerId, "localhost", "localhost", configs.head.port)

    // create a topic and partition and await leadership
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(c => c.brokerId).mkString(":"))
    TestUtils.waitUntilLeaderIsElected(zkClient, topic, 0, 1000)

    // send test messages to leader
    val producer = TestUtils.createProducer[String, String](zkConnect, new StringEncoder)
    producer.send(new ProducerData[String, String](topic, "test", testMessageList))

    // create a tmp directory
    val tmpLogDir = TestUtils.tempDir()
    val replicaLogDir = new File(tmpLogDir, topic + "-" + partition)
    replicaLogDir.mkdirs()
    val replicaLog = new Log(replicaLogDir, 500, 500, false)

    // create replica fetch thread
    val time = new MockTime
    val testPartition = new Partition(topic, partition, time)
    testPartition.leaderId(Some(leaderBrokerId))
    val testReplica = new Replica(followerBrokerId, testPartition, topic, Some(replicaLog))
    val replicaFetchThread = new ReplicaFetcherThread("replica-fetcher", testReplica, leaderBroker, configs.last)

    // start a replica fetch thread to the above broker
    replicaFetchThread.start()

    Thread.sleep(700)
    replicaFetchThread.shutdown()

    assertEquals(60L, testReplica.log.get.logEndOffset)
    replicaLog.close()
  }
}