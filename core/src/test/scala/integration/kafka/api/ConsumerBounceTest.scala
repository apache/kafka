/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.api

import java.util

import kafka.server.KafkaConfig
import kafka.utils.{Logging, ShutdownableThread, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.{IllegalGenerationException, UnknownMemberIdException}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{Test, Before}

import scala.collection.JavaConversions._



/**
 * Integration tests for the new consumer that cover basic usage as well as server failures
 */
class ConsumerBounceTest extends IntegrationTestHarness with Logging {

  val producerCount = 1
  val consumerCount = 2
  val serverCount = 3

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)

  // configure the servers and clients
  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "10") // set small enough session timeout
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-test")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096.toString)
  this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
  this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000")
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  override def generateConfigs() = {
    FixedPortTestUtils.createBrokerConfigs(serverCount, zkConnect,enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, serverConfig))
  }

  @Before
  override def setUp() {
    super.setUp()

    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(this.zkUtils, topic, 1, serverCount, this.servers)
  }

  @Test
  def testConsumptionWithBrokerFailures() = consumeWithBrokerFailures(10)

  /*
   * 1. Produce a bunch of messages
   * 2. Then consume the messages while killing and restarting brokers at random
   */
  def consumeWithBrokerFailures(numIters: Int) {
    val numRecords = 1000
    sendRecords(numRecords)
    this.producers.foreach(_.close)

    var consumed = 0L
    val consumer = this.consumers.head

    consumer.subscribe(List(topic))

    val scheduler = new BounceBrokerScheduler(numIters)
    scheduler.start()

    while (scheduler.isRunning.get()) {
      for (record <- consumer.poll(100)) {
        assertEquals(consumed, record.offset())
        consumed += 1
      }

      try {
        consumer.commitSync()
        assertEquals(consumer.position(tp), consumer.committed(tp).offset)

        if (consumer.position(tp) == numRecords) {
          consumer.seekToBeginning(List[TopicPartition]())
          consumed = 0
        }
      } catch {
        // TODO: should be no need to catch these exceptions once KAFKA-2017 is
        // merged since coordinator fail-over will not cause a rebalance
        case _: CommitFailedException =>
      }
    }
    scheduler.shutdown()
  }

  @Test
  def testSeekAndCommitWithBrokerFailures() = seekAndCommitWithBrokerFailures(5)

  def seekAndCommitWithBrokerFailures(numIters: Int) {
    val numRecords = 1000
    sendRecords(numRecords)
    this.producers.foreach(_.close)

    val consumer = this.consumers.head
    consumer.assign(List(tp))
    consumer.seek(tp, 0)

    // wait until all the followers have synced the last HW with leader
    TestUtils.waitUntilTrue(() => servers.forall(server =>
      server.replicaManager.getReplica(tp.topic(), tp.partition()).get.highWatermark.messageOffset == numRecords
    ), "Failed to update high watermark for followers after timeout")

    val scheduler = new BounceBrokerScheduler(numIters)
    scheduler.start()

    while(scheduler.isRunning.get()) {
      val coin = TestUtils.random.nextInt(3)
      if (coin == 0) {
        info("Seeking to end of log")
        consumer.seekToEnd(List[TopicPartition]())
        assertEquals(numRecords.toLong, consumer.position(tp))
      } else if (coin == 1) {
        val pos = TestUtils.random.nextInt(numRecords).toLong
        info("Seeking to " + pos)
        consumer.seek(tp, pos)
        assertEquals(pos, consumer.position(tp))
      } else if (coin == 2) {
        info("Committing offset.")
        consumer.commitSync()
        assertEquals(consumer.position(tp), consumer.committed(tp).offset)
      }
    }
  }

  private class BounceBrokerScheduler(val numIters: Int) extends ShutdownableThread("daemon-bounce-broker", false)
  {
    var iter: Int = 0

    override def doWork(): Unit = {
      killRandomBroker()
      Thread.sleep(500)
      restartDeadBrokers()

      iter += 1
      if (iter == numIters)
        initiateShutdown()
      else
        Thread.sleep(500)
    }
  }

  private def sendRecords(numRecords: Int) {
    val futures = (0 until numRecords).map { i =>
      this.producers.head.send(new ProducerRecord(topic, part, i.toString.getBytes, i.toString.getBytes))
    }
    futures.map(_.get)
  }


}
