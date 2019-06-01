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
package kafka.admin

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{Collections, Properties}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, Future, TimeUnit}

import kafka.common.KafkaException
import kafka.coordinator.group.GroupOverview
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.internals.{ConsumerNetworkClient, ConsumerProtocol, RequestFuture}
import org.apache.kafka.common.config.ConfigDef.ValidString._
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.errors.{AuthenticationException, TimeoutException}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.{DescribeGroupsRequestData, DescribeGroupsResponseData, FindCoordinatorRequestData}

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.{KafkaThread, Time}
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType

/**
  * A Scala administrative client for Kafka which supports managing and inspecting topics, brokers,
  * and configurations. This client is deprecated, and will be replaced by org.apache.kafka.clients.admin.AdminClient.
  */
@deprecated("This class is deprecated in favour of org.apache.kafka.clients.admin.AdminClient and it will be removed in " +
  "a future release.", since = "0.11.0")
class AdminClient(val time: Time,
                  val requestTimeoutMs: Int,
                  val retryBackoffMs: Long,
                  val client: ConsumerNetworkClient,
                  val bootstrapBrokers: List[Node]) extends Logging {

  @volatile var running: Boolean = true
  val pendingFutures = new ConcurrentLinkedQueue[RequestFuture[ClientResponse]]()

  val networkThread = new KafkaThread("admin-client-network-thread", new Runnable {
    override def run() {
      try {
        while (running)
          client.poll(time.timer(Long.MaxValue))
      } catch {
        case t : Throwable =>
          error("admin-client-network-thread exited", t)
      } finally {
        pendingFutures.asScala.foreach { future =>
          try {
            future.raise(Errors.UNKNOWN_SERVER_ERROR)
          } catch {
            case _: IllegalStateException => // It is OK if the future has been completed
          }
        }
        pendingFutures.clear()
      }
    }
  }, true)

  networkThread.start()

  private def send(target: Node,
                   api: ApiKeys,
                   request: AbstractRequest.Builder[_ <: AbstractRequest]): AbstractResponse = {
    val future: RequestFuture[ClientResponse] = client.send(target, request)
    pendingFutures.add(future)
    future.awaitDone(Long.MaxValue, TimeUnit.MILLISECONDS)
    pendingFutures.remove(future)
    if (future.succeeded())
      future.value().responseBody()
    else
      throw future.exception()
  }

  private def sendAnyNode(api: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest]): AbstractResponse = {
    bootstrapBrokers.foreach { broker =>
      try {
        return send(broker, api, request)
      } catch {
        case e: AuthenticationException =>
          throw e
        case e: Exception =>
          debug(s"Request $api failed against node $broker", e)
      }
    }
    throw new RuntimeException(s"Request $api failed on brokers $bootstrapBrokers")
  }

  def findCoordinator(groupId: String, timeoutMs: Long = 0): Node = {
    val requestBuilder = new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(CoordinatorType.GROUP.id)
          .setKey(groupId))

    def sendRequest: Try[FindCoordinatorResponse] =
      Try(sendAnyNode(ApiKeys.FIND_COORDINATOR, requestBuilder).asInstanceOf[FindCoordinatorResponse])

    val startTime = time.milliseconds
    var response = sendRequest

    while ((response.isFailure || response.get.error == Errors.COORDINATOR_NOT_AVAILABLE) &&
      (time.milliseconds - startTime < timeoutMs)) {

      Thread.sleep(retryBackoffMs)
      response = sendRequest
    }

    def timeoutException(cause: Throwable) =
      throw new TimeoutException("The consumer group command timed out while waiting for group to initialize: ", cause)

    response match {
      case Failure(exception) => throw timeoutException(exception)
      case Success(response) =>
        if (response.error == Errors.COORDINATOR_NOT_AVAILABLE)
          throw timeoutException(response.error.exception)
        response.error.maybeThrow()
        response.node
    }
  }

  def listGroups(node: Node): List[GroupOverview] = {
    val response = send(node, ApiKeys.LIST_GROUPS, new ListGroupsRequest.Builder()).asInstanceOf[ListGroupsResponse]
    response.error.maybeThrow()
    response.groups.asScala.map(group => GroupOverview(group.groupId, group.protocolType)).toList
  }

  def getApiVersions(node: Node): List[ApiVersion] = {
    val response = send(node, ApiKeys.API_VERSIONS, new ApiVersionsRequest.Builder()).asInstanceOf[ApiVersionsResponse]
    response.error.maybeThrow()
    response.apiVersions.asScala.toList
  }

  /**
   * Wait until there is a non-empty list of brokers in the cluster.
   */
  def awaitBrokers() {
    var nodes = List[Node]()
    do {
      nodes = findAllBrokers()
      if (nodes.isEmpty)
        Thread.sleep(50)
    } while (nodes.isEmpty)
  }

  def findAllBrokers(): List[Node] = {
    val request = MetadataRequest.Builder.allTopics()
    val response = sendAnyNode(ApiKeys.METADATA, request).asInstanceOf[MetadataResponse]
    val errors = response.errors
    if (!errors.isEmpty)
      debug(s"Metadata request contained errors: $errors")
    response.cluster.nodes.asScala.toList
  }

  def listAllGroups(): Map[Node, List[GroupOverview]] = {
    findAllBrokers().map { broker =>
      broker -> {
        try {
          listGroups(broker)
        } catch {
          case e: Exception =>
            debug(s"Failed to find groups from broker $broker", e)
            List[GroupOverview]()
        }
      }
    }.toMap
  }

  def listAllConsumerGroups(): Map[Node, List[GroupOverview]] = {
    listAllGroups().mapValues { groups =>
      groups.filter(isConsumerGroup)
    }
  }

  def listAllGroupsFlattened(): List[GroupOverview] = {
    listAllGroups().values.flatten.toList
  }

  def listAllConsumerGroupsFlattened(): List[GroupOverview] = {
    listAllGroupsFlattened().filter(isConsumerGroup)
  }

  private def isConsumerGroup(group: GroupOverview): Boolean = {
    // Consumer groups which are using group management use the "consumer" protocol type.
    // Consumer groups which are only using offset storage will have an empty protocol type.
    group.protocolType.isEmpty || group.protocolType == ConsumerProtocol.PROTOCOL_TYPE
  }

  def listGroupOffsets(groupId: String): Map[TopicPartition, Long] = {
    val coordinator = findCoordinator(groupId)
    val responseBody = send(coordinator, ApiKeys.OFFSET_FETCH, OffsetFetchRequest.Builder.allTopicPartitions(groupId))
    val response = responseBody.asInstanceOf[OffsetFetchResponse]
    if (response.hasError)
      throw response.error.exception
    response.maybeThrowFirstPartitionError()
    response.responseData.asScala.map { case (tp, partitionData) => (tp, partitionData.offset) }.toMap
  }

  def listAllBrokerVersionInfo(): Map[Node, Try[NodeApiVersions]] =
    findAllBrokers().map { broker =>
      broker -> Try[NodeApiVersions](new NodeApiVersions(getApiVersions(broker).asJava))
    }.toMap

  /**
   * Case class used to represent a consumer of a consumer group
   */
  case class ConsumerSummary(consumerId: String,
                             clientId: String,
                             host: String,
                             assignment: List[TopicPartition])

  /**
   * Case class used to represent group metadata (including the group coordinator) for the DescribeGroup API
   */
  case class ConsumerGroupSummary(state: String,
                                  assignmentStrategy: String,
                                  consumers: Option[List[ConsumerSummary]],
                                  coordinator: Node)

  def describeConsumerGroupHandler(coordinator: Node, groupId: String): DescribeGroupsResponseData.DescribedGroup = {
    val responseBody = send(coordinator, ApiKeys.DESCRIBE_GROUPS,
      new DescribeGroupsRequest.Builder(new DescribeGroupsRequestData().setGroups(Collections.singletonList(groupId))))
    val response = responseBody.asInstanceOf[DescribeGroupsResponse]
    val metadata = response.data().groups().asScala.find(group => groupId.equals(group.groupId()))
      .getOrElse(throw new KafkaException(s"Response from broker contained no metadata for group $groupId"))
    metadata
  }

  def describeConsumerGroup(groupId: String, timeoutMs: Long = 0): ConsumerGroupSummary = {

    def isValidConsumerGroupResponse(metadata: DescribeGroupsResponseData.DescribedGroup): Boolean =
      metadata.errorCode() == Errors.NONE.code() && (metadata.groupState() == "Dead" ||
        metadata.groupState() == "Empty" || metadata.protocolType == ConsumerProtocol.PROTOCOL_TYPE)

    val startTime = time.milliseconds
    val coordinator = findCoordinator(groupId, timeoutMs)
    var metadata = describeConsumerGroupHandler(coordinator, groupId)

    while (!isValidConsumerGroupResponse(metadata) && time.milliseconds - startTime < timeoutMs) {
      debug(s"The consumer group response for group '$groupId' is invalid. Retrying the request as the group is initializing ...")
      Thread.sleep(retryBackoffMs)
      metadata = describeConsumerGroupHandler(coordinator, groupId)
    }

    if (!isValidConsumerGroupResponse(metadata))
      throw new TimeoutException("The consumer group command timed out while waiting for group to initialize")

    val consumers = metadata.members.asScala.map { consumer =>
      ConsumerSummary(consumer.memberId, consumer.clientId, consumer.clientHost, metadata.groupState() match {
        case "Stable" =>
          val assignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(consumer.memberAssignment))
          assignment.partitions.asScala.toList
        case _ =>
          List()
      })
    }.toList

    ConsumerGroupSummary(metadata.groupState(), metadata.protocolData(), Some(consumers), coordinator)
  }

  def deleteConsumerGroups(groups: List[String]): Map[String, Errors] = {

    def coordinatorLookup(group: String): Either[Node, Errors] = {
      try {
        Left(findCoordinator(group))
      } catch {
        case e: Throwable =>
          if (e.isInstanceOf[TimeoutException])
            Right(Errors.COORDINATOR_NOT_AVAILABLE)
          else
            Right(Errors.forException(e))
      }
    }

    var errors: Map[String, Errors] = Map()
    var groupsPerCoordinator: Map[Node, List[String]] = Map()

    groups.foreach { group =>
      coordinatorLookup(group) match {
        case Right(error) =>
          errors += group -> error
        case Left(coordinator) =>
          groupsPerCoordinator.get(coordinator) match {
            case Some(gList) =>
              val gListNew = group :: gList
              groupsPerCoordinator += coordinator -> gListNew
            case None =>
              groupsPerCoordinator += coordinator -> List(group)
          }
      }
    }

    groupsPerCoordinator.foreach { case (coordinator, groups) =>
      val responseBody = send(coordinator, ApiKeys.DELETE_GROUPS, new DeleteGroupsRequest.Builder(groups.toSet.asJava))
      val response = responseBody.asInstanceOf[DeleteGroupsResponse]
      groups.foreach {
        case group if response.hasError(group) => errors += group -> response.errors.get(group)
        case group => errors += group -> Errors.NONE
      }
    }

    errors
  }

  def close() {
    running = false
    try {
      client.close()
    } catch {
      case e: IOException =>
        error("Exception closing nioSelector:", e)
    }
  }

}

/*
 * CompositeFuture assumes that the future object in the futures list does not raise error
 */
class CompositeFuture[T](time: Time,
                         defaultResults: Map[TopicPartition, T],
                         futures: List[RequestFuture[Map[TopicPartition, T]]]) extends Future[Map[TopicPartition, T]] {

  override def isCancelled = false

  override def cancel(interrupt: Boolean) = false

  override def get(): Map[TopicPartition, T] = {
    get(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  override def get(timeout: Long, unit: TimeUnit): Map[TopicPartition, T] = {
    val start: Long = time.milliseconds()
    val timeoutMs = unit.toMillis(timeout)
    var remaining: Long = timeoutMs

    val observedResults = futures.flatMap { future =>
      val elapsed = time.milliseconds() - start
      remaining = if (timeoutMs - elapsed > 0) timeoutMs - elapsed else 0L

      if (future.awaitDone(remaining, TimeUnit.MILLISECONDS)) future.value()
      else Map.empty[TopicPartition, T]
    }.toMap

    defaultResults ++ observedResults
  }

  override def isDone: Boolean = {
    futures.forall(_.isDone)
  }
}

@deprecated("This class is deprecated in favour of org.apache.kafka.clients.admin.AdminClient and it will be removed in " +
  "a future release.", since = "0.11.0")
object AdminClient {
  val DefaultConnectionMaxIdleMs = 9 * 60 * 1000
  val DefaultRequestTimeoutMs = 5000
  val DefaultMaxInFlightRequestsPerConnection = 100
  val DefaultReconnectBackoffMs = 50
  val DefaultReconnectBackoffMax = 50
  val DefaultSendBufferBytes = 128 * 1024
  val DefaultReceiveBufferBytes = 32 * 1024
  val DefaultRetryBackoffMs = 100

  val AdminClientIdSequence = new AtomicInteger(1)
  val AdminConfigDef = {
    val config = new ConfigDef()
      .define(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        Type.LIST,
        Importance.HIGH,
        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
      .define(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
        Type.STRING,
        ClientDnsLookup.DEFAULT.toString,
        in(ClientDnsLookup.DEFAULT.toString,
           ClientDnsLookup.USE_ALL_DNS_IPS.toString,
           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString),
        Importance.MEDIUM,
        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
      .define(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        ConfigDef.Type.STRING,
        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
        ConfigDef.Importance.MEDIUM,
        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
      .define(
        CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
        ConfigDef.Type.INT,
        DefaultRequestTimeoutMs,
        ConfigDef.Importance.MEDIUM,
        CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
      .define(
        CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
        ConfigDef.Type.LONG,
        DefaultRetryBackoffMs,
        ConfigDef.Importance.MEDIUM,
        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
      .withClientSslSupport()
      .withClientSaslSupport()
    config
  }

  class AdminConfig(originals: Map[_,_]) extends AbstractConfig(AdminConfigDef, originals.asJava, false)

  def createSimplePlaintext(brokerUrl: String): AdminClient = {
    val config = Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> brokerUrl)
    create(new AdminConfig(config))
  }

  def create(props: Properties): AdminClient = create(props.asScala.toMap)

  def create(props: Map[String, _]): AdminClient = create(new AdminConfig(props))

  def create(config: AdminConfig): AdminClient = {
    val clientId = "admin-" + AdminClientIdSequence.getAndIncrement()
    val logContext = new LogContext(s"[LegacyAdminClient clientId=$clientId] ")
    val time = Time.SYSTEM
    val metrics = new Metrics(time)
    val metadata = new Metadata(100L, 60 * 60 * 1000L, logContext,
      new ClusterResourceListeners)
    val channelBuilder = ClientUtils.createChannelBuilder(config, time)
    val requestTimeoutMs = config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG)
    val retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG)

    val brokerUrls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
    val clientDnsLookup = config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG)
    val brokerAddresses = ClientUtils.parseAndValidateAddresses(brokerUrls, clientDnsLookup)
    metadata.bootstrap(brokerAddresses, time.milliseconds())

    val selector = new Selector(
      DefaultConnectionMaxIdleMs,
      metrics,
      time,
      "admin",
      channelBuilder,
      logContext)

    val networkClient = new NetworkClient(
      selector,
      metadata,
      clientId,
      DefaultMaxInFlightRequestsPerConnection,
      DefaultReconnectBackoffMs,
      DefaultReconnectBackoffMax,
      DefaultSendBufferBytes,
      DefaultReceiveBufferBytes,
      requestTimeoutMs,
      ClientDnsLookup.DEFAULT,
      time,
      true,
      new ApiVersions,
      logContext)

    val highLevelClient = new ConsumerNetworkClient(
      logContext,
      networkClient,
      metadata,
      time,
      retryBackoffMs,
      requestTimeoutMs,
      Integer.MAX_VALUE)

    new AdminClient(
      time,
      requestTimeoutMs,
      retryBackoffMs,
      highLevelClient,
      metadata.fetch.nodes.asScala.toList)
  }
}
