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

package kafka.tools

import joptsimple.OptionParser
import kafka.api._
import kafka.utils.{IncludeList, _}
import org.apache.kafka.clients._
import org.apache.kafka.clients.admin.{Admin, ListTopicsOptions, TopicDescription}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.AbstractRequest.Builder
import org.apache.kafka.common.requests.{AbstractRequest, FetchResponse, ListOffsetsRequest, FetchRequest => JFetchRequest}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import java.net.SocketTimeoutException
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.regex.{Pattern, PatternSyntaxException}
import java.util.{Date, Optional, Properties}

import scala.collection.Seq
import scala.jdk.CollectionConverters._

/**
 * For verifying the consistency among replicas.
 *
 *  1. start a fetcher on every broker.
 *  2. each fetcher does the following
 *    2.1 issues fetch request
 *    2.2 puts the fetched result in a shared buffer
 *    2.3 waits for all other fetchers to finish step 2.2
 *    2.4 one of the fetchers verifies the consistency of fetched results among replicas
 *
 * The consistency verification is up to the high watermark. The tool reports the
 * max lag between the verified offset and the high watermark among all partitions.
 *
 * If a broker goes down, the verification of the partitions on that broker is delayed
 * until the broker is up again.
 *
 * Caveats:
 * 1. The tools needs all brokers to be up at startup time.
 * 2. The tool doesn't handle out of range offsets.
 */

object ReplicaVerificationTool extends Logging {
  val clientId = "replicaVerificationTool"
  val dateFormatString = "yyyy-MM-dd HH:mm:ss,SSS"
  val dateFormat = new SimpleDateFormat(dateFormatString)

  def getCurrentTimeString() = {
    ReplicaVerificationTool.dateFormat.format(new Date(Time.SYSTEM.milliseconds))
  }

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The list of hostname and port of the server to connect to.")
                         .withRequiredArg
                         .describedAs("hostname:port,...,hostname:port")
                         .ofType(classOf[String])
    val fetchSizeOpt = parser.accepts("fetch-size", "The fetch size of each request.")
                         .withRequiredArg
                         .describedAs("bytes")
                         .ofType(classOf[java.lang.Integer])
                         .defaultsTo(ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES)
    val maxWaitMsOpt = parser.accepts("max-wait-ms", "The max amount of time each fetch request waits.")
                         .withRequiredArg
                         .describedAs("ms")
                         .ofType(classOf[java.lang.Integer])
                         .defaultsTo(1000)
    val topicWhiteListOpt = parser.accepts("topic-white-list", "DEPRECATED use --topics-include instead; ignored if --topics-include specified. List of topics to verify replica consistency. Defaults to '.*' (all topics)")
                         .withRequiredArg
                         .describedAs("Java regex (String)")
                         .ofType(classOf[String])
                         .defaultsTo(".*")
    val topicsIncludeOpt = parser.accepts("topics-include", "List of topics to verify replica consistency. Defaults to '.*' (all topics)")
                        .withRequiredArg
                        .describedAs("Java regex (String)")
                        .ofType(classOf[String])
                        .defaultsTo(".*")
    val initialOffsetTimeOpt = parser.accepts("time", "Timestamp for getting the initial offsets.")
                           .withRequiredArg
                           .describedAs("timestamp/-1(latest)/-2(earliest)")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(-1L)
    val reportIntervalOpt = parser.accepts("report-interval-ms", "The reporting interval.")
                         .withRequiredArg
                         .describedAs("ms")
                         .ofType(classOf[java.lang.Long])
                         .defaultsTo(30 * 1000L)
    val helpOpt = parser.accepts("help", "Print usage information.").forHelp()
    val versionOpt = parser.accepts("version", "Print version information and exit.").forHelp()

    val options = parser.parse(args: _*)

    if (args.isEmpty || options.has(helpOpt)) {
      CommandLineUtils.printUsageAndDie(parser, "Validate that all replicas for a set of topics have the same data.")
    }

    if (options.has(versionOpt)) {
      CommandLineUtils.printVersionAndDie()
    }
    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt)

    val regex = if (options.has(topicsIncludeOpt))
      options.valueOf(topicsIncludeOpt)
    else
      options.valueOf(topicWhiteListOpt)

    val topicsIncludeFilter = new IncludeList(regex)

    try Pattern.compile(regex)
    catch {
      case _: PatternSyntaxException =>
        throw new RuntimeException(s"$regex is an invalid regex.")
    }

    val fetchSize = options.valueOf(fetchSizeOpt).intValue
    val maxWaitMs = options.valueOf(maxWaitMsOpt).intValue
    val initialOffsetTime = options.valueOf(initialOffsetTimeOpt).longValue
    val reportInterval = options.valueOf(reportIntervalOpt).longValue
    // getting topic metadata
    info("Getting topic metadata...")
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)

    val (topicsMetadata, brokerInfo) = {
      val adminClient = createAdminClient(brokerList)
      try ((listTopicsMetadata(adminClient), brokerDetails(adminClient)))
      finally CoreUtils.swallow(adminClient.close(), this)
    }

    val topicIds = topicsMetadata.map( metadata => metadata.name() -> metadata.topicId()).toMap

    val filteredTopicMetadata = topicsMetadata.filter { topicMetaData =>
      topicsIncludeFilter.isTopicAllowed(topicMetaData.name, excludeInternalTopics = false)
    }

    if (filteredTopicMetadata.isEmpty) {
      error(s"No topics found. $topicsIncludeOpt if specified, is either filtering out all topics or there is no topic.")
      Exit.exit(1)
    }

    val topicPartitionReplicas = filteredTopicMetadata.flatMap { topicMetadata =>
      topicMetadata.partitions.asScala.flatMap { partitionMetadata =>
        partitionMetadata.replicas.asScala.map { node =>
          TopicPartitionReplica(topic = topicMetadata.name, partitionId = partitionMetadata.partition, replicaId = node.id)
        }
      }
    }
    debug(s"Selected topic partitions: $topicPartitionReplicas")
    val brokerToTopicPartitions = topicPartitionReplicas.groupBy(_.replicaId).map { case (brokerId, partitions) =>
      brokerId -> partitions.map { partition => new TopicPartition(partition.topic, partition.partitionId) }
    }
    debug(s"Topic partitions per broker: $brokerToTopicPartitions")
    val expectedReplicasPerTopicPartition = topicPartitionReplicas.groupBy { replica =>
      new TopicPartition(replica.topic, replica.partitionId)
    }.map { case (topicAndPartition, replicaSet) => topicAndPartition -> replicaSet.size }
    debug(s"Expected replicas per topic partition: $expectedReplicasPerTopicPartition")

    val topicPartitions = filteredTopicMetadata.flatMap { topicMetaData =>
      topicMetaData.partitions.asScala.map { partitionMetadata =>
        new TopicPartition(topicMetaData.name, partitionMetadata.partition)
      }
    }

    val consumerProps = consumerConfig(brokerList)

    val replicaBuffer = new ReplicaBuffer(expectedReplicasPerTopicPartition,
      initialOffsets(topicPartitions, consumerProps, initialOffsetTime),
      brokerToTopicPartitions.size,
      reportInterval)
    // create all replica fetcher threads
    val verificationBrokerId = brokerToTopicPartitions.head._1
    val counter = new AtomicInteger(0)
    val fetcherThreads = brokerToTopicPartitions.map { case (brokerId, topicPartitions) =>
      new ReplicaFetcher(name = s"ReplicaFetcher-$brokerId",
        sourceBroker = brokerInfo(brokerId),
        topicPartitions = topicPartitions,
        topicIds = topicIds,
        replicaBuffer = replicaBuffer,
        socketTimeout = 30000,
        socketBufferSize = 256000,
        fetchSize = fetchSize,
        maxWait = maxWaitMs,
        minBytes = 1,
        doVerification = brokerId == verificationBrokerId,
        consumerProps,
        fetcherId = counter.incrementAndGet())
    }

    Exit.addShutdownHook("ReplicaVerificationToolShutdownHook", {
        info("Stopping all fetchers")
        fetcherThreads.foreach(_.shutdown())
    })
    fetcherThreads.foreach(_.start())
    println(s"${ReplicaVerificationTool.getCurrentTimeString()}: verification process is started.")

  }

  private def listTopicsMetadata(adminClient: Admin): Seq[TopicDescription] = {
    val topics = adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names.get
    adminClient.describeTopics(topics).allTopicNames.get.values.asScala.toBuffer
  }

  private def brokerDetails(adminClient: Admin): Map[Int, Node] = {
    adminClient.describeCluster.nodes.get.asScala.map(n => (n.id, n)).toMap
  }

  private def createAdminClient(brokerUrl: String): Admin = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    Admin.create(props)
  }

  private def initialOffsets(topicPartitions: Seq[TopicPartition], consumerConfig: Properties,
                             initialOffsetTime: Long): collection.Map[TopicPartition, Long] = {
    val consumer = createConsumer(consumerConfig)
    try {
      if (ListOffsetsRequest.LATEST_TIMESTAMP == initialOffsetTime)
        consumer.endOffsets(topicPartitions.asJava).asScala.map { case (k, v) => k -> v.longValue }
      else if (ListOffsetsRequest.EARLIEST_TIMESTAMP == initialOffsetTime)
        consumer.beginningOffsets(topicPartitions.asJava).asScala.map { case (k, v) => k -> v.longValue }
      else {
        val timestampsToSearch = topicPartitions.map(tp => tp -> (initialOffsetTime: java.lang.Long)).toMap
        consumer.offsetsForTimes(timestampsToSearch.asJava).asScala.map { case (k, v) => k -> v.offset }
      }
    } finally consumer.close()
  }

  private def consumerConfig(brokerUrl: String): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ReplicaVerification")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    properties
  }

  private def createConsumer(consumerConfig: Properties): KafkaConsumer[String, String] =
    new KafkaConsumer(consumerConfig)
}

private case class TopicPartitionReplica(topic: String, partitionId: Int, replicaId: Int)

private case class MessageInfo(replicaId: Int, offset: Long, nextOffset: Long, checksum: Long)

private class ReplicaBuffer(expectedReplicasPerTopicPartition: collection.Map[TopicPartition, Int],
                            initialOffsets: collection.Map[TopicPartition, Long],
                            expectedNumFetchers: Int,
                            reportInterval: Long) extends Logging {
  private val fetchOffsetMap = new Pool[TopicPartition, Long]
  private val recordsCache = new Pool[TopicPartition, Pool[Int, FetchResponseData.PartitionData]]
  private val fetcherBarrier = new AtomicReference(new CountDownLatch(expectedNumFetchers))
  private val verificationBarrier = new AtomicReference(new CountDownLatch(1))
  @volatile private var lastReportTime = Time.SYSTEM.milliseconds
  private var maxLag: Long = -1L
  private var offsetWithMaxLag: Long = -1L
  private var maxLagTopicAndPartition: TopicPartition = _
  initialize()

  def createNewFetcherBarrier(): Unit = {
    fetcherBarrier.set(new CountDownLatch(expectedNumFetchers))
  }

  def getFetcherBarrier() = fetcherBarrier.get

  def createNewVerificationBarrier(): Unit = {
    verificationBarrier.set(new CountDownLatch(1))
  }

  def getVerificationBarrier() = verificationBarrier.get

  private def initialize(): Unit = {
    for (topicPartition <- expectedReplicasPerTopicPartition.keySet)
      recordsCache.put(topicPartition, new Pool[Int, FetchResponseData.PartitionData])
    setInitialOffsets()
  }


  private def setInitialOffsets(): Unit = {
    for ((tp, offset) <- initialOffsets)
      fetchOffsetMap.put(tp, offset)
  }

  def addFetchedData(topicAndPartition: TopicPartition, replicaId: Int, partitionData: FetchResponseData.PartitionData): Unit = {
    recordsCache.get(topicAndPartition).put(replicaId, partitionData)
  }

  def getOffset(topicAndPartition: TopicPartition) = {
    fetchOffsetMap.get(topicAndPartition)
  }

  def verifyCheckSum(println: String => Unit): Unit = {
    debug("Begin verification")
    maxLag = -1L
    for ((topicPartition, fetchResponsePerReplica) <- recordsCache) {
      debug(s"Verifying $topicPartition")
      assert(fetchResponsePerReplica.size == expectedReplicasPerTopicPartition(topicPartition),
        "fetched " + fetchResponsePerReplica.size + " replicas for " + topicPartition + ", but expected "
          + expectedReplicasPerTopicPartition(topicPartition) + " replicas")
      val recordBatchIteratorMap = fetchResponsePerReplica.map { case (replicaId, fetchResponse) =>
        replicaId -> FetchResponse.recordsOrFail(fetchResponse).batches.iterator
      }
      val maxHw = fetchResponsePerReplica.values.map(_.highWatermark).max

      // Iterate one message at a time from every replica, until high watermark is reached.
      var isMessageInAllReplicas = true
      while (isMessageInAllReplicas) {
        var messageInfoFromFirstReplicaOpt: Option[MessageInfo] = None
        for ((replicaId, recordBatchIterator) <- recordBatchIteratorMap) {
          try {
            if (recordBatchIterator.hasNext) {
              val batch = recordBatchIterator.next()

              // only verify up to the high watermark
              if (batch.lastOffset >= fetchResponsePerReplica.get(replicaId).highWatermark)
                isMessageInAllReplicas = false
              else {
                messageInfoFromFirstReplicaOpt match {
                  case None =>
                    messageInfoFromFirstReplicaOpt = Some(
                      MessageInfo(replicaId, batch.lastOffset, batch.nextOffset, batch.checksum))
                  case Some(messageInfoFromFirstReplica) =>
                    if (messageInfoFromFirstReplica.offset != batch.lastOffset) {
                      println(ReplicaVerificationTool.getCurrentTimeString() + ": partition " + topicPartition
                        + ": replica " + messageInfoFromFirstReplica.replicaId + "'s offset "
                        + messageInfoFromFirstReplica.offset + " doesn't match replica "
                        + replicaId + "'s offset " + batch.lastOffset)
                      Exit.exit(1)
                    }
                    if (messageInfoFromFirstReplica.checksum != batch.checksum)
                      println(ReplicaVerificationTool.getCurrentTimeString() + ": partition "
                        + topicPartition + " has unmatched checksum at offset " + batch.lastOffset + "; replica "
                        + messageInfoFromFirstReplica.replicaId + "'s checksum " + messageInfoFromFirstReplica.checksum
                        + "; replica " + replicaId + "'s checksum " + batch.checksum)
                }
              }
            } else
              isMessageInAllReplicas = false
          } catch {
            case t: Throwable =>
              throw new RuntimeException("Error in processing replica %d in partition %s at offset %d."
                .format(replicaId, topicPartition, fetchOffsetMap.get(topicPartition)), t)
          }
        }
        if (isMessageInAllReplicas) {
          val nextOffset = messageInfoFromFirstReplicaOpt.get.nextOffset
          fetchOffsetMap.put(topicPartition, nextOffset)
          debug(s"${expectedReplicasPerTopicPartition(topicPartition)} replicas match at offset " +
            s"$nextOffset for $topicPartition")
        }
      }
      if (maxHw - fetchOffsetMap.get(topicPartition) > maxLag) {
        offsetWithMaxLag = fetchOffsetMap.get(topicPartition)
        maxLag = maxHw - offsetWithMaxLag
        maxLagTopicAndPartition = topicPartition
      }
      fetchResponsePerReplica.clear()
    }
    val currentTimeMs = Time.SYSTEM.milliseconds
    if (currentTimeMs - lastReportTime > reportInterval) {
      println(ReplicaVerificationTool.dateFormat.format(new Date(currentTimeMs)) + ": max lag is "
        + maxLag + " for partition " + maxLagTopicAndPartition + " at offset " + offsetWithMaxLag
        + " among " + recordsCache.size + " partitions")
      lastReportTime = currentTimeMs
    }
  }
}

private class ReplicaFetcher(name: String, sourceBroker: Node, topicPartitions: Iterable[TopicPartition],
                             topicIds: Map[String, Uuid], replicaBuffer: ReplicaBuffer, socketTimeout: Int, socketBufferSize: Int,
                             fetchSize: Int, maxWait: Int, minBytes: Int, doVerification: Boolean, consumerConfig: Properties,
                             fetcherId: Int)
  extends ShutdownableThread(name) {

  private val fetchEndpoint = new ReplicaFetcherBlockingSend(sourceBroker, new ConsumerConfig(consumerConfig), new Metrics(), Time.SYSTEM, fetcherId,
    s"broker-${Request.DebuggingConsumerId}-fetcher-$fetcherId")

  private val topicNames = topicIds.map(_.swap)

  override def doWork(): Unit = {

    val fetcherBarrier = replicaBuffer.getFetcherBarrier()
    val verificationBarrier = replicaBuffer.getVerificationBarrier()

    val requestMap = new util.LinkedHashMap[TopicPartition, JFetchRequest.PartitionData]
    for (topicPartition <- topicPartitions)
      requestMap.put(topicPartition, new JFetchRequest.PartitionData(topicIds.getOrElse(topicPartition.topic, Uuid.ZERO_UUID), replicaBuffer.getOffset(topicPartition),
        0L, fetchSize, Optional.empty()))

    val fetchRequestBuilder = JFetchRequest.Builder.
      forReplica(ApiKeys.FETCH.latestVersion, Request.DebuggingConsumerId, maxWait, minBytes, requestMap)

    debug("Issuing fetch request ")

    var fetchResponse: FetchResponse = null
    try {
      val clientResponse = fetchEndpoint.sendRequest(fetchRequestBuilder)
      fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse]
    } catch {
      case t: Throwable =>
        if (!isRunning)
          throw t
    }

    if (fetchResponse != null) {
      fetchResponse.responseData(topicNames.asJava, ApiKeys.FETCH.latestVersion()).forEach { (tp, partitionData) =>
        replicaBuffer.addFetchedData(tp, sourceBroker.id, partitionData)
      }
    } else {
      for (topicAndPartition <- topicPartitions)
        replicaBuffer.addFetchedData(topicAndPartition, sourceBroker.id, FetchResponse.partitionResponse(topicAndPartition.partition, Errors.NONE))
    }

    fetcherBarrier.countDown()
    debug("Done fetching")

    // wait for all fetchers to finish
    fetcherBarrier.await()
    debug("Ready for verification")

    // one of the fetchers will do the verification
    if (doVerification) {
      debug("Do verification")
      replicaBuffer.verifyCheckSum(println)
      replicaBuffer.createNewFetcherBarrier()
      replicaBuffer.createNewVerificationBarrier()
      debug("Created new barrier")
      verificationBarrier.countDown()
    }

    verificationBarrier.await()
    debug("Done verification")
  }
}

private class ReplicaFetcherBlockingSend(sourceNode: Node,
                                         consumerConfig: ConsumerConfig,
                                         metrics: Metrics,
                                         time: Time,
                                         fetcherId: Int,
                                         clientId: String) {

  private val socketTimeout: Int = consumerConfig.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)

  private val networkClient = {
    val logContext = new LogContext()
    val channelBuilder = org.apache.kafka.clients.ClientUtils.createChannelBuilder(consumerConfig, time, logContext)
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      consumerConfig.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
      metrics,
      time,
      "replica-fetcher",
      Map("broker-id" -> sourceNode.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
      false,
      channelBuilder,
      logContext
    )
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      1,
      0,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      consumerConfig.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
      consumerConfig.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
      consumerConfig.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
      consumerConfig.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
      time,
      false,
      new ApiVersions,
      logContext
    )
  }

  def sendRequest(requestBuilder: Builder[_ <: AbstractRequest]): ClientResponse = {
    try {
      if (!NetworkClientUtils.awaitReady(networkClient, sourceNode, time, socketTimeout))
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
      else {
        val clientRequest = networkClient.newClientRequest(sourceNode.id.toString, requestBuilder,
          time.milliseconds(), true)
        NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(sourceNode.id.toString)
        throw e
    }
  }

  def close(): Unit = {
    networkClient.close()
  }
}
