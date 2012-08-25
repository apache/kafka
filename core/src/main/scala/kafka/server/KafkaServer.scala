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
import org.I0Itec.zkclient.ZkClient

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = SystemTime) extends Logging {
  this.logIdent = "[Kafka Server " + config.brokerId + "], "
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
  var kafkaController: KafkaController = null
  val kafkaScheduler = new KafkaScheduler(4)
  var zkClient: ZkClient = null

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    info("starting")
    isShuttingDown = new AtomicBoolean(false)
    shutdownLatch = new CountDownLatch(1)
    var needRecovery = true
    val cleanShutDownFile = new File(new File(config.logDir), CleanShutdownFile)
    if (cleanShutDownFile.exists) {
      needRecovery = false
      cleanShutDownFile.delete
    }

    /* start scheduler */
    kafkaScheduler.startup

    /* start log manager */
    logManager = new LogManager(config,
                                kafkaScheduler,
                                time,
                                1000L * 60 * 60 * config.logRollHours,
                                1000L * 60 * config.logCleanupIntervalMinutes,
                                1000L * 60 * 60 * config.logRetentionHours,
                                needRecovery)
    logManager.startup()

    socketServer = new SocketServer(config.brokerId,
                                    config.port,
                                    config.numNetworkThreads,
                                    config.monitoringPeriodSecs,
                                    config.numQueuedRequests,
                                    config.maxSocketRequestSize)

    socketServer.startup

    Utils.registerMBean(socketServer.stats, statsMBeanName)

    /* start client */
    kafkaZookeeper = new KafkaZooKeeper(config)
    // starting relevant replicas and leader election for partitions assigned to this broker
    kafkaZookeeper.startup

    info("Connecting to ZK: " + config.zkConnect)

    replicaManager = new ReplicaManager(config, time, kafkaZookeeper.getZookeeperClient, kafkaScheduler, logManager)

    kafkaController = new KafkaController(config, kafkaZookeeper.getZookeeperClient)
    apis = new KafkaApis(socketServer.requestChannel, replicaManager, kafkaZookeeper.getZookeeperClient, config.brokerId)
    requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
    Mx4jLoader.maybeLoad

    // start the replica manager
    replicaManager.startup()
    // start the controller
    kafkaController.startup()
    info("started")
  }


  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    info("shutting down")
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      if(requestHandlerPool != null)
        requestHandlerPool.shutdown()
      kafkaScheduler.shutdown()
      apis.close()
      kafkaZookeeper.shutdown()
      if(replicaManager != null)
        replicaManager.shutdown()
      if (socketServer != null)
        socketServer.shutdown()
      Utils.unregisterMBean(statsMBeanName)
      if(logManager != null)
        logManager.shutdown()

      if(kafkaController != null)
        kafkaController.shutdown()

      val cleanShutDownFile = new File(new File(config.logDir), CleanShutdownFile)
      debug("creating clean shutdown file " + cleanShutDownFile.getAbsolutePath())
      cleanShutDownFile.createNewFile
      shutdownLatch.countDown()
      info("shut down completed")
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager

  def getStats(): SocketServerStats = socketServer.stats
}


