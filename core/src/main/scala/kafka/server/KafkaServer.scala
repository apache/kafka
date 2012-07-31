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

import java.io.File
import kafka.network.{SocketServerStats, SocketServer}
import kafka.log.LogManager
import kafka.utils._
import java.util.concurrent._
import atomic.AtomicBoolean
import kafka.cluster.Replica
import org.I0Itec.zkclient.ZkClient
import kafka.common.KafkaZookeeperClient


/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = SystemTime) extends Logging {

  val CleanShutdownFile = ".kafka_cleanshutdown"
  private var isShuttingDown = new AtomicBoolean(false)
  private var shutdownLatch = new CountDownLatch(1)
  private val statsMBeanName = "kafka:type=kafka.SocketServerStats"
  var socketServer: SocketServer = null
  var requestHandlerPool: KafkaRequestHandlerPool = null
  var logManager: LogManager = null
  var kafkaZookeeper: KafkaZooKeeper = null
  var replicaManager: ReplicaManager = null
  private var apis: KafkaApis = null
  var kafkaController: KafkaController = new KafkaController(config)
  val kafkaScheduler = new KafkaScheduler(4)
  var zkClient: ZkClient = null

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    info("Starting Kafka server..." + config.brokerId)
    isShuttingDown = new AtomicBoolean(false)
    shutdownLatch = new CountDownLatch(1)
    var needRecovery = true
    val cleanShutDownFile = new File(new File(config.logDir), CleanShutdownFile)
    if (cleanShutDownFile.exists) {
      needRecovery = false
      cleanShutDownFile.delete
    }
    /* start client */
    info("Connecting to ZK: " + config.zkConnect)
    zkClient = KafkaZookeeperClient.getZookeeperClient(config)
    /* start scheduler */
    kafkaScheduler.startUp
    /* start log manager */
    logManager = new LogManager(config,
                                kafkaScheduler,
                                time,
                                1000L * 60 * config.logCleanupIntervalMinutes,
                                1000L * 60 * 60 * config.logRetentionHours,
                                needRecovery)
    logManager.startup()
                                                
    socketServer = new SocketServer(config.port,
                                    config.numNetworkThreads,
                                    config.monitoringPeriodSecs,
                                    config.numQueuedRequests,
                                    config.maxSocketRequestSize)
    Utils.registerMBean(socketServer.stats, statsMBeanName)

    kafkaZookeeper = new KafkaZooKeeper(config, zkClient, addReplica, getReplica, makeLeader, makeFollower)

    replicaManager = new ReplicaManager(config, time, zkClient, kafkaScheduler)

    apis = new KafkaApis(socketServer.requestChannel, logManager, replicaManager, kafkaZookeeper)
    requestHandlerPool = new KafkaRequestHandlerPool(socketServer.requestChannel, apis, config.numIoThreads)
    socketServer.startup()

    Mx4jLoader.maybeLoad

    // starting relevant replicas and leader election for partitions assigned to this broker
    kafkaZookeeper.startup()
    // start the replica manager
    replicaManager.startup()
    // start the controller
    kafkaController.startup()

    info("Server started.")
  }
  
  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      info("Shutting down Kafka server with id " + config.brokerId)
      kafkaScheduler.shutdown()
      apis.close()
      if(replicaManager != null)
        replicaManager.shutdown()
      if (socketServer != null)
        socketServer.shutdown()
      if(requestHandlerPool != null)
        requestHandlerPool.shutdown()
      Utils.unregisterMBean(statsMBeanName)
      if(logManager != null)
        logManager.shutdown()
      if(kafkaController != null)
        kafkaController.shutDown()
      kafkaZookeeper.shutdown()
      zkClient.close()
      val cleanShutDownFile = new File(new File(config.logDir), CleanShutdownFile)
      debug("Creating clean shutdown file " + cleanShutDownFile.getAbsolutePath())
      cleanShutDownFile.createNewFile
      shutdownLatch.countDown()
      info("Kafka server with id %d shut down completed".format(config.brokerId))
    }
  }
  
  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def addReplica(topic: String, partition: Int, assignedReplicas: Set[Int]): Replica = {
    info("Added local replica for topic %s partition %d on broker %d".format(topic, partition, config.brokerId))
    // get local log
    val log = logManager.getOrCreateLog(topic, partition)
    replicaManager.addLocalReplica(topic, partition, log, assignedReplicas)
  }

  def makeLeader(replica: Replica, currentISRInZk: Seq[Int]) {
    replicaManager.makeLeader(replica, currentISRInZk)
  }

  def makeFollower(replica: Replica, leaderBrokerId: Int, zkClient: ZkClient) {
    replicaManager.makeFollower(replica, leaderBrokerId, zkClient)
  }

  def getReplica(topic: String, partition: Int): Option[Replica] =
    replicaManager.getReplica(topic, partition)

  def getLogManager(): LogManager = logManager

  def getStats(): SocketServerStats = socketServer.stats
}


