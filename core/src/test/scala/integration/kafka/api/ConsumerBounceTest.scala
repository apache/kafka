/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package kafka.api

import kafka.server.KafkaConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.CommitType
import org.apache.kafka.common.TopicPartition

import kafka.utils.{ShutdownableThread, TestUtils, Logging}

import org.junit.Assert._

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
  this.serverConfig.setProperty("controlled.shutdown.enable", "false") // speed up shutdown
  this.serverConfig.setProperty("offsets.topic.replication.factor", "3") // don't want to lose offset
  this.serverConfig.setProperty("offsets.topic.num.partitions", "1")
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-test")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 4096.toString)
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  override def generateConfigs() = {
    FixedPortTestUtils.createBrokerConfigs(serverCount, zkConnect,enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, serverConfig))
  }

  override def setUp() {
    super.setUp()

    // create the test topic with all the brokers as replicas
    TestUtils.createTopic(this.zkClient, topic, 1, serverCount, this.servers)
  }

  def testConsumptionWithBrokerFailures() = consumeWithBrokerFailures(5)

  /*
   * 1. Produce a bunch of messages
   * 2. Then consume the messages while killing and restarting brokers at random
   */
  def consumeWithBrokerFailures(numIters: Int) {
    val numRecords = 1000
    sendRecords(numRecords)
    this.producers.map(_.close)

    var consumed = 0
    val consumer = this.consumers(0)
    consumer.subscribe(topic)

    val scheduler = new BounceBrokerScheduler(numIters)
    scheduler.start()

    while (scheduler.isRunning.get()) {
      for (record <- consumer.poll(100)) {
        assertEquals(consumed.toLong, record.offset())
        consumed += 1
      }
      consumer.commit(CommitType.SYNC)

      if (consumed == numRecords) {
        consumer.seekToBeginning()
        consumed = 0
      }
    }
    scheduler.shutdown()
  }

  def testSeekAndCommitWithBrokerFailures() = seekAndCommitWithBrokerFailures(5)

  def seekAndCommitWithBrokerFailures(numIters: Int) {
    val numRecords = 1000
    sendRecords(numRecords)
    this.producers.map(_.close)

    val consumer = this.consumers(0)
    consumer.subscribe(tp)
    consumer.seek(tp, 0)

    val scheduler = new BounceBrokerScheduler(numIters)
    scheduler.start()

    while(scheduler.isRunning.get()) {
      val coin = TestUtils.random.nextInt(3)
      if (coin == 0) {
        info("Seeking to end of log")
        consumer.seekToEnd()
        assertEquals(numRecords.toLong, consumer.position(tp))
      } else if (coin == 1) {
        val pos = TestUtils.random.nextInt(numRecords).toLong
        info("Seeking to " + pos)
        consumer.seek(tp, pos)
        assertEquals(pos, consumer.position(tp))
      } else if (coin == 2) {
        info("Committing offset.")
        consumer.commit(CommitType.SYNC)
        assertEquals(consumer.position(tp), consumer.committed(tp))
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
      this.producers(0).send(new ProducerRecord(topic, part, i.toString.getBytes, i.toString.getBytes))
    }
    futures.map(_.get)
  }
}