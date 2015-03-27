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
import collection.mutable
import org.I0Itec.zkclient.ZkClient
import kafka.controller.{ControllerStats, KafkaController}
import kafka.cluster.Broker
import kafka.api.{ControlledShutdownResponse, ControlledShutdownRequest}
import kafka.common.{ErrorMapping, InconsistentBrokerIdException, GenerateBrokerIdException}
import kafka.network.{Receive, BlockingChannel, SocketServer}
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import kafka.coordinator.ConsumerCoordinator

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = SystemTime) extends Logging with KafkaMetricsGroup {
  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)

  private var shutdownLatch = new CountDownLatch(1)

  val brokerState: BrokerState = new BrokerState

  var apis: KafkaApis = null
  var socketServer: SocketServer = null
  var requestHandlerPool: KafkaRequestHandlerPool = null

  var logManager: LogManager = null

  var offsetManager: OffsetManager = null

  var replicaManager: ReplicaManager = null

  var topicConfigManager: TopicConfigManager = null

  var consumerCoordinator: ConsumerCoordinator = null

  var kafkaController: KafkaController = null

  val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)

  var kafkaHealthcheck: KafkaHealthcheck = null
  val metadataCache: MetadataCache = new MetadataCache(config.brokerId)



  var zkClient: ZkClient = null
  val correlationId: AtomicInteger = new AtomicInteger(0)
  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator +brokerMetaPropsFile)))).toMap

  newGauge(
    "BrokerState",
    new Gauge[Int] {
      def value = brokerState.currentState
    }
  )

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    try {
      info("starting")

      if(isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if(startupComplete.get)
        return

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        brokerState.newState(Starting)

        /* start scheduler */
        kafkaScheduler.startup()

        /* setup zookeeper */
        zkClient = initZk()

        /* start log manager */
        logManager = createLogManager(zkClient, brokerState)
        logManager.startup()

        /* generate brokerId */
        config.brokerId =  getBrokerId
        this.logIdent = "[Kafka Server " + config.brokerId + "], "

        socketServer = new SocketServer(config.brokerId,
          config.hostName,
          config.port,
          config.numNetworkThreads,
          config.queuedMaxRequests,
          config.socketSendBufferBytes,
          config.socketReceiveBufferBytes,
          config.socketRequestMaxBytes,
          config.maxConnectionsPerIp,
          config.connectionsMaxIdleMs,
          config.maxConnectionsPerIpOverrides)
        socketServer.startup()

        /* start replica manager */
        replicaManager = new ReplicaManager(config, time, zkClient, kafkaScheduler, logManager, isShuttingDown)
        replicaManager.startup()

        /* start offset manager */
        offsetManager = createOffsetManager()

        /* start kafka controller */
        kafkaController = new KafkaController(config, zkClient, brokerState)
        kafkaController.startup()

        /* start kafka coordinator */
        consumerCoordinator = new ConsumerCoordinator(config, zkClient)
        consumerCoordinator.startup()

        /* start processing requests */
        apis = new KafkaApis(socketServer.requestChannel, replicaManager, offsetManager, consumerCoordinator,
          kafkaController, zkClient, config.brokerId, config, metadataCache)
        requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
        brokerState.newState(RunningAsBroker)

        Mx4jLoader.maybeLoad()

        /* start topic config manager */
        topicConfigManager = new TopicConfigManager(zkClient, logManager)
        topicConfigManager.startup()

        /* tell everyone we are alive */
        kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, config.advertisedHostName, config.advertisedPort, config.zkSessionTimeoutMs, zkClient)
        kafkaHealthcheck.startup()

        /* register broker metrics */
        registerStats()

        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        info("started")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  private def initZk(): ZkClient = {
    info("Connecting to zookeeper on " + config.zkConnect)

    val chroot = {
      if (config.zkConnect.indexOf("/") > 0)
        config.zkConnect.substring(config.zkConnect.indexOf("/"))
      else
        ""
    }

    if (chroot.length > 1) {
      val zkConnForChrootCreation = config.zkConnect.substring(0, config.zkConnect.indexOf("/"))
      val zkClientForChrootCreation = new ZkClient(zkConnForChrootCreation, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
      ZkUtils.makeSurePersistentPathExists(zkClientForChrootCreation, chroot)
      info("Created zookeeper path " + chroot)
      zkClientForChrootCreation.close()
    }

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
      var shutdownSucceeded : Boolean = false
      try {
        brokerState.newState(PendingControlledShutdown)
        while (!shutdownSucceeded && remainingRetries > 0) {
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
                shutdownSucceeded = true
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
                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s".format(ioe.getMessage))
                // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
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
      if (!shutdownSucceeded) {
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
      }
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    try {
      info("shutting down")

      if(isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      val canShutdown = isShuttingDown.compareAndSet(false, true)
      if (canShutdown && shutdownLatch.getCount > 0) {
        Utils.swallow(controlledShutdown())
        brokerState.newState(BrokerShuttingDown)
        if(socketServer != null)
          Utils.swallow(socketServer.shutdown())
        if(requestHandlerPool != null)
          Utils.swallow(requestHandlerPool.shutdown())
        if(offsetManager != null)
          offsetManager.shutdown()
        Utils.swallow(kafkaScheduler.shutdown())
        if(apis != null)
          Utils.swallow(apis.close())
        if(replicaManager != null)
          Utils.swallow(replicaManager.shutdown())
        if(logManager != null)
          Utils.swallow(logManager.shutdown())
        if(consumerCoordinator != null)
          Utils.swallow(consumerCoordinator.shutdown())
        if(kafkaController != null)
          Utils.swallow(kafkaController.shutdown())
        if(zkClient != null)
          Utils.swallow(zkClient.close())

        brokerState.newState(NotRunning)

        startupComplete.set(false)
        isShuttingDown.set(false)
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager

  private def createLogManager(zkClient: ZkClient, brokerState: BrokerState): LogManager = {
    val defaultLogConfig = LogConfig(segmentSize = config.logSegmentBytes,
                                     segmentMs = config.logRollTimeMillis,
                                     segmentJitterMs = config.logRollTimeJitterMillis,
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
                                     compact = config.logCleanupPolicy.trim.toLowerCase == "compact",
                                     compressionType = config.compressionType)
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
                   ioThreads = config.numRecoveryThreadsPerDataDir,
                   flushCheckMs = config.logFlushSchedulerIntervalMs,
                   flushCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
                   retentionCheckMs = config.logCleanupIntervalMs,
                   scheduler = kafkaScheduler,
                   brokerState = brokerState,
                   time = time)
  }

  private def createOffsetManager(): OffsetManager = {
    val offsetManagerConfig = OffsetManagerConfig(
      maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
    new OffsetManager(offsetManagerConfig, replicaManager, zkClient, kafkaScheduler, metadataCache)
  }

  /**
    * Generates new brokerId or reads from meta.properties based on following conditions
    * <ol>
    * <li> config has no broker.id provided , generates a broker.id based on Zookeeper's sequence
    * <li> stored broker.id in meta.properties doesn't match in all the log.dirs throws InconsistentBrokerIdException
    * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
    * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
    * <ol>
    * @returns A brokerId.
    */
  private def getBrokerId: Int =  {
    var brokerId = config.brokerId
    var logDirsWithoutMetaProps: List[String] = List()
    val brokerIdSet = mutable.HashSet[Int]()

    for (logDir <- config.logDirs) {
      val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
      brokerMetadataOpt match {
        case Some(brokerMetadata: BrokerMetadata) =>
          brokerIdSet.add(brokerMetadata.brokerId)
        case None =>
          logDirsWithoutMetaProps ++= List(logDir)
      }
    }

    if(brokerIdSet.size > 1)
      throw new InconsistentBrokerIdException("Failed to match brokerId across logDirs")
    else if(brokerId >= 0 && brokerIdSet.size == 1 && brokerIdSet.last != brokerId)
      throw new InconsistentBrokerIdException("Configured brokerId %s doesn't match stored brokerId %s in meta.properties".format(brokerId, brokerIdSet.last))
    else if(brokerIdSet.size == 0 && brokerId < 0)  // generate a new brokerId from Zookeeper
      brokerId = generateBrokerId
    else if(brokerIdSet.size == 1) // pick broker.id from meta.properties
      brokerId = brokerIdSet.last

    for(logDir <- logDirsWithoutMetaProps) {
      val checkpoint = brokerMetadataCheckpoints(logDir)
      checkpoint.write(new BrokerMetadata(brokerId))
    }

    return brokerId
  }

  private def generateBrokerId: Int = {
    try {
      ZkUtils.getBrokerSequenceId(zkClient, config.maxReservedBrokerId)
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }
}
