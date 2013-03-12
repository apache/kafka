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

import kafka.network.SocketServer
import kafka.admin._
import kafka.log.LogConfig
import kafka.log.CleanerConfig
import kafka.log.LogManager
import kafka.utils._
import java.util.concurrent._
import java.io.File
import atomic.AtomicBoolean
import org.I0Itec.zkclient.ZkClient
import kafka.controller.{ControllerStats, KafkaController}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = SystemTime) extends Logging {
  this.logIdent = "[Kafka Server " + config.brokerId + "], "
  private var isShuttingDown = new AtomicBoolean(false)
  private var shutdownLatch = new CountDownLatch(1)
  var socketServer: SocketServer = null
  var requestHandlerPool: KafkaRequestHandlerPool = null
  var logManager: LogManager = null
  var kafkaHealthcheck: KafkaHealthcheck = null
  var topicConfigManager: TopicConfigManager = null
  var replicaManager: ReplicaManager = null
  var apis: KafkaApis = null
  var kafkaController: KafkaController = null
  val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
  var zkClient: ZkClient = null

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    info("starting")
    isShuttingDown = new AtomicBoolean(false)
    shutdownLatch = new CountDownLatch(1)

    /* start scheduler */
    kafkaScheduler.startup()
    
    /* setup zookeeper */
    zkClient = initZk()

    /* start log manager */
    logManager = createLogManager(zkClient)
    logManager.startup()

    socketServer = new SocketServer(config.brokerId,
                                    config.hostName,
                                    config.port,
                                    config.numNetworkThreads,
                                    config.queuedMaxRequests,
                                    config.socketRequestMaxBytes)
    socketServer.startup()

    replicaManager = new ReplicaManager(config, time, zkClient, kafkaScheduler, logManager)
    kafkaController = new KafkaController(config, zkClient)
    
    /* start processing requests */
    apis = new KafkaApis(socketServer.requestChannel, replicaManager, zkClient, config.brokerId, config)
    requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
   
    Mx4jLoader.maybeLoad()

    replicaManager.startup()

    kafkaController.startup()
    
    topicConfigManager = new TopicConfigManager(zkClient, logManager)
    topicConfigManager.startup()
    
    /* tell everyone we are alive */
    kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, config.hostName, config.port, zkClient)
    kafkaHealthcheck.startup()

    
    registerStats()
    
    info("started")
  }
  
  private def initZk(): ZkClient = {
    info("Connecting to zookeeper on " + config.zkConnect)
    val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
    ZkUtils.setupCommonPaths(zkClient)
    zkClient
  }

  /**
   *  Forces some dynamic jmx beans to be registered on server startup.
   */
  private def registerStats() {
    BrokerTopicStats.getBrokerAllTopicsStats()
    ControllerStats.offlinePartitionRate
    ControllerStats.uncleanLeaderElectionRate
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    info("shutting down")
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      if(socketServer != null)
        Utils.swallow(socketServer.shutdown())
      if(requestHandlerPool != null)
        Utils.swallow(requestHandlerPool.shutdown())
      Utils.swallow(kafkaScheduler.shutdown())
      if(apis != null)
        Utils.swallow(apis.close())
      if(replicaManager != null)
        Utils.swallow(replicaManager.shutdown())
      if(logManager != null)
        Utils.swallow(logManager.shutdown())
      if(kafkaController != null)
        Utils.swallow(kafkaController.shutdown())
      if(zkClient != null)
        Utils.swallow(zkClient.close())

      shutdownLatch.countDown()
      info("shut down completed")
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager
  
  private def createLogManager(zkClient: ZkClient): LogManager = {
    val defaultLogConfig = LogConfig(segmentSize = config.logSegmentBytes, 
                                     segmentMs = 60 * 60 * 1000 * config.logRollHours,
                                     flushInterval = config.logFlushIntervalMessages,
                                     flushMs = config.logFlushIntervalMs.toLong,
                                     retentionSize = config.logRetentionBytes,
                                     retentionMs = 60 * 60 * 1000 * config.logRetentionHours,
                                     maxMessageSize = config.messageMaxBytes,
                                     maxIndexSize = config.logIndexSizeMaxBytes,
                                     indexInterval = config.logIndexIntervalBytes,
                                     deleteRetentionMs = config.logCleanerDeleteRetentionMs,
                                     fileDeleteDelayMs = config.logDeleteDelayMs,
                                     minCleanableRatio = config.logCleanerMinCleanRatio,
                                     dedupe = config.logCleanupPolicy.trim.toLowerCase == "dedupe")
    val defaultProps = defaultLogConfig.toProps
    val configs = AdminUtils.fetchAllTopicConfigs(zkClient).mapValues(LogConfig.fromProps(defaultProps, _))
    // read the log configurations from zookeeper
    val cleanerConfig = CleanerConfig(numThreads = config.logCleanerThreads,
                                      dedupeBufferSize = config.logCleanerDedupeBufferSize,
                                      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
                                      ioBufferSize = config.logCleanerIoBufferSize,
                                      maxMessageSize = config.messageMaxBytes,
                                      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
                                      backOffMs = config.logCleanerBackoffMs,
                                      enableCleaner = config.logCleanerEnable)
    new LogManager(logDirs = config.logDirs.map(new File(_)).toArray,
                   topicConfigs = configs,
                   defaultConfig = defaultLogConfig,
                   cleanerConfig = cleanerConfig,
                   flushCheckMs = config.logFlushSchedulerIntervalMs,
                   retentionCheckMs = config.logCleanupIntervalMs,
                   scheduler = kafkaScheduler,
                   time = time)
  }

}


