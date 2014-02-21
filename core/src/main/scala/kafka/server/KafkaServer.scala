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

import kafka.admin._
import kafka.log.LogConfig
import kafka.log.CleanerConfig
import kafka.log.LogManager
import kafka.utils._
import java.util.concurrent._
import atomic.{AtomicInteger, AtomicBoolean}
import java.io.File
import org.I0Itec.zkclient.ZkClient
import kafka.controller.{ControllerStats, KafkaController}
import kafka.cluster.Broker
import kafka.api.{ControlledShutdownResponse, ControlledShutdownRequest}
import kafka.common.ErrorMapping
import kafka.network.{Receive, BlockingChannel, SocketServer}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = SystemTime) extends Logging {
  this.logIdent = "[Kafka Server " + config.brokerId + "], "
  private var isShuttingDown = new AtomicBoolean(false)
  private var shutdownLatch = new CountDownLatch(1)
  private var startupComplete = new AtomicBoolean(false);
  val correlationId: AtomicInteger = new AtomicInteger(0)
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
                                    config.socketSendBufferBytes,
                                    config.socketReceiveBufferBytes,
                                    config.socketRequestMaxBytes)
    socketServer.startup()

    replicaManager = new ReplicaManager(config, time, zkClient, kafkaScheduler, logManager, isShuttingDown)
    kafkaController = new KafkaController(config, zkClient)
    
    /* start processing requests */
    apis = new KafkaApis(socketServer.requestChannel, replicaManager, zkClient, config.brokerId, config, kafkaController)
    requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
   
    Mx4jLoader.maybeLoad()

    replicaManager.startup()

    kafkaController.startup()
    
    topicConfigManager = new TopicConfigManager(zkClient, logManager)
    topicConfigManager.startup()
    
    /* tell everyone we are alive */
    kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, config.advertisedHostName, config.advertisedPort, config.zkSessionTimeoutMs, zkClient)
    kafkaHealthcheck.startup()

    
    registerStats()
    startupComplete.set(true);
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
    ControllerStats.uncleanLeaderElectionRate
    ControllerStats.leaderElectionTimer
  }

  /**
   *  Performs controlled shutdown
   */
  private def controlledShutdown() {
    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      var remainingRetries = config.controlledShutdownMaxRetries
      info("Starting controlled shutdown")
      var channel : BlockingChannel = null
      var prevController : Broker = null
      var shutdownSuceeded : Boolean = false
      try {
        while (!shutdownSuceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request
          val controllerId = ZkUtils.getController(zkClient)
          ZkUtils.getBrokerInfo(zkClient, controllerId) match {
            case Some(broker) =>
              if (channel == null || prevController == null || !prevController.equals(broker)) {
                // if this is the first attempt or if the controller has changed, create a channel to the most recent
                // controller
                if (channel != null) {
                  channel.disconnect()
                }
                channel = new BlockingChannel(broker.host, broker.port,
                  BlockingChannel.UseDefaultBufferSize,
                  BlockingChannel.UseDefaultBufferSize,
                  config.controllerSocketTimeoutMs)
                channel.connect()
                prevController = broker
              }
            case None=>
              //ignore and try again
          }

          // 2. issue a controlled shutdown to the controller
          if (channel != null) {
            var response: Receive = null
            try {
              // send the controlled shutdown request
              val request = new ControlledShutdownRequest(correlationId.getAndIncrement, config.brokerId)
              channel.send(request)
              response = channel.receive()
              val shutdownResponse = ControlledShutdownResponse.readFrom(response.buffer)
              if (shutdownResponse.errorCode == ErrorMapping.NoError && shutdownResponse.partitionsRemaining != null &&
                  shutdownResponse.partitionsRemaining.size == 0) {
                shutdownSuceeded = true
                info ("Controlled shutdown succeeded")
              }
              else {
                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.mkString(",")))
                info("Error code from controller: %d".format(shutdownResponse.errorCode))
              }
            }
            catch {
              case ioe: java.io.IOException =>
                channel.disconnect()
                channel = null
                // ignore and try again
            }
          }
          if (!shutdownSuceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally {
        if (channel != null) {
          channel.disconnect()
          channel = null
        }
      }
      if (!shutdownSuceeded) {
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
      }
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    info("shutting down")
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      Utils.swallow(controlledShutdown())
      if(kafkaHealthcheck != null)
        Utils.swallow(kafkaHealthcheck.shutdown())
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
      startupComplete.set(false);
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
                                     segmentMs = 60L * 60L * 1000L * config.logRollHours,
                                     flushInterval = config.logFlushIntervalMessages,
                                     flushMs = config.logFlushIntervalMs.toLong,
                                     retentionSize = config.logRetentionBytes,
                                     retentionMs = config.logRetentionTimeMillis,
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
                   flushCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
                   retentionCheckMs = config.logCleanupIntervalMs,
                   scheduler = kafkaScheduler,
                   time = time)
  }

}


