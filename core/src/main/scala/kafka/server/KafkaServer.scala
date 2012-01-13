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

import scala.reflect.BeanProperty
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.io.File
import org.apache.log4j.Logger
import kafka.utils.{Mx4jLoader, Utils, SystemTime, KafkaScheduler, Logging}
import kafka.network.{SocketServerStats, SocketServer, RequestChannel}
import kafka.log.LogManager

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig) extends Logging {

  val CleanShutdownFile = ".kafka_cleanshutdown"
  private val isShuttingDown = new AtomicBoolean(false)  
  private val shutdownLatch = new CountDownLatch(1)
  private val statsMBeanName = "kafka:type=kafka.SocketServerStats"
  var socketServer: SocketServer = null
  var requestHandlerPool: KafkaRequestHandlerPool = null
  val scheduler = new KafkaScheduler(1, "kafka-logcleaner-", false)
  private var logManager: LogManager = null

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    info("Starting Kafka server...")
    var needRecovery = true
    val cleanShutDownFile = new File(new File(config.logDir), CleanShutdownFile)
    if (cleanShutDownFile.exists) {
      needRecovery = false
      cleanShutDownFile.delete
    }
    logManager = new LogManager(config,
                                scheduler,
                                SystemTime,
                                1000L * 60 * config.logCleanupIntervalMinutes,
                                1000L * 60 * 60 * config.logRetentionHours,
                                needRecovery)
                                                
    socketServer = new SocketServer(config.port,
                                    config.numNetworkThreads,
                                    config.monitoringPeriodSecs,
                                    config.numQueuedRequests,
                                    config.maxSocketRequestSize)
    Utils.registerMBean(socketServer.stats, statsMBeanName)
    requestHandlerPool = new KafkaRequestHandlerPool(socketServer.requestChannel, new KafkaApis(logManager).handle, config.numIoThreads)
    socketServer.startup

    Mx4jLoader.maybeLoad

    /**
     *  Registers this broker in ZK. After this, consumers can connect to broker.
     *  So this should happen after socket server start.
     */
    logManager.startup
    info("Server started.")
  }
  
  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      info("Shutting down Kafka server")
      scheduler.shutdown()
      if (socketServer != null)
        socketServer.shutdown()
      if(requestHandlerPool != null)
        requestHandlerPool.shutdown()
      Utils.unregisterMBean(statsMBeanName)
      if(logManager != null)
        logManager.close()

      val cleanShutDownFile = new File(new File(config.logDir), CleanShutdownFile)
      debug("Creating clean shutdown file " + cleanShutDownFile.getAbsolutePath())
      cleanShutDownFile.createNewFile
      shutdownLatch.countDown()
      info("Kafka server shut down completed")
    }
  }
  
  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager

  def getStats(): SocketServerStats = socketServer.stats
}
