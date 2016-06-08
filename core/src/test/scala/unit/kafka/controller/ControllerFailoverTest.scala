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

package kafka.controller

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import kafka.api.RequestOrResponse
import kafka.common.TopicAndPartition
import kafka.integration.KafkaServerTestHarness
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.{AbstractRequestResponse, AbstractRequest}
import org.apache.kafka.common.utils.SystemTime
import org.apache.log4j.{Level, Logger}
import org.junit.{After, Before, Test}

import scala.collection.mutable


class ControllerFailoverTest extends KafkaServerTestHarness with Logging {
  val log = Logger.getLogger(classOf[ControllerFailoverTest])
  val numNodes = 2
  val numParts = 1
  val msgQueueSize = 1
  val topic = "topic1"
  val overridingProps = new Properties()
  val metrics = new Metrics()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  override def generateConfigs() = TestUtils.createBrokerConfigs(numNodes, zkConnect)
    .map(KafkaConfig.fromProps(_, overridingProps))

  @Before
  override def setUp() {
    super.setUp()
  }

  @After
  override def tearDown() {
    super.tearDown()
    this.metrics.close()
  }

  /**
   * See @link{https://issues.apache.org/jira/browse/KAFKA-2300}
   * for the background of this test case
   */
  @Test
  def testMetadataUpdate() {
    log.setLevel(Level.INFO)
    var controller: KafkaServer = this.servers.head
    // Find the current controller
    val epochMap: mutable.Map[Int, Int] = mutable.Map.empty
    for (server <- this.servers) {
      epochMap += (server.config.brokerId -> server.kafkaController.epoch)
      if(server.kafkaController.isActive()) {
        controller = server
      }
    }
    // Create topic with one partition
    kafka.admin.AdminUtils.createTopic(controller.zkUtils, topic, 1, 1)
    val topicPartition = TopicAndPartition("topic1", 0)
    var partitions = controller.kafkaController.partitionStateMachine.partitionsInState(OnlinePartition)
    while (!partitions.contains(topicPartition)) {
      partitions = controller.kafkaController.partitionStateMachine.partitionsInState(OnlinePartition)
      Thread.sleep(100)
    }
    // Replace channel manager with our mock manager
    controller.kafkaController.controllerContext.controllerChannelManager.shutdown()
    val channelManager = new MockChannelManager(controller.kafkaController.controllerContext, 
                                                  controller.kafkaController.config, metrics)
    channelManager.startup()
    controller.kafkaController.controllerContext.controllerChannelManager = channelManager
    channelManager.shrinkBlockingQueue(0)
    channelManager.stopSendThread(0)
    // Spawn a new thread to block on the outgoing channel
    // queue
    val thread = new Thread(new Runnable {
      def run() {
        try {
          controller.kafkaController.sendUpdateMetadataRequest(Seq(0), Set(topicPartition))
          log.info("Queue state %d %d".format(channelManager.queueCapacity(0), channelManager.queueSize(0)))
          controller.kafkaController.sendUpdateMetadataRequest(Seq(0), Set(topicPartition))
          log.info("Queue state %d %d".format(channelManager.queueCapacity(0), channelManager.queueSize(0)))
        } catch {
          case e : Exception => {
            log.info("Thread interrupted")
          }
        }
      }
    })
    thread.setName("mythread")
    thread.start()
    while (thread.getState() != Thread.State.WAITING) {
      Thread.sleep(100)
    }
    // Assume that the thread is WAITING because it is
    // blocked on the queue, so interrupt and move forward
    thread.interrupt()
    thread.join()
    channelManager.resumeSendThread(0)
    // Wait and find current controller
    var found = false
    var counter = 0
    while (!found && counter < 10) {
      for (server <- this.servers) {
        val previousEpoch = epochMap get server.config.brokerId match {
          case Some(epoch) =>
            epoch
          case None =>
            val msg = String.format("Missing element in epoch map %s", epochMap.mkString(", "))
            throw new IllegalStateException(msg)
        }

        if (server.kafkaController.isActive
            && previousEpoch < server.kafkaController.epoch) {
          controller = server
          found = true
        }
      }
      if (!found) {
          Thread.sleep(100)
          counter += 1
      }
    }
    // Give it a shot to make sure that sending isn't blocking
    try {
      controller.kafkaController.sendUpdateMetadataRequest(Seq(0), Set(topicPartition))
    } catch {
      case e : Throwable => {
        fail(e)
      }
    }
  }
}

class MockChannelManager(private val controllerContext: ControllerContext, config: KafkaConfig, metrics: Metrics)
  extends ControllerChannelManager(controllerContext, config, new SystemTime, metrics) {

  def stopSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    requestThread.isRunning.set(false)
    requestThread.interrupt
    requestThread.join
  }

  def shrinkBlockingQueue(brokerId: Int) {
    val messageQueue = new LinkedBlockingQueue[QueueItem](1)
    val brokerInfo = this.brokerStateInfo(brokerId)
    this.brokerStateInfo.put(brokerId, brokerInfo.copy(messageQueue = messageQueue))
  }

  def resumeSendThread (brokerId: Int) {
    this.startRequestSendThread(0)
  }

  def queueCapacity(brokerId: Int): Int = {
    this.brokerStateInfo(brokerId).messageQueue.remainingCapacity
  }

  def queueSize(brokerId: Int): Int = {
    this.brokerStateInfo(brokerId).messageQueue.size
  }
}
