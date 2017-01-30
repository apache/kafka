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

import java.nio.ByteBuffer
import java.util.{Collections, Properties}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion
import kafka.common.KafkaException
import kafka.coordinator.GroupOverview
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.internals.{ConsumerNetworkClient, ConsumerProtocol, RequestFuture}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{Cluster, Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.util.Try

class AdminClient(val time: Time,
                  val requestTimeoutMs: Int,
                  val client: ConsumerNetworkClient,
                  val bootstrapBrokers: List[Node]) extends Logging {

  private def send(target: Node,
                   api: ApiKeys,
                   request: AbstractRequest.Builder[_ <: AbstractRequest]): AbstractResponse = {
    var future: RequestFuture[ClientResponse] = null

    future = client.send(target, request)
    client.poll(future)

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
        case e: Exception =>
          debug(s"Request $api failed against node $broker", e)
      }
    }
    throw new RuntimeException(s"Request $api failed on brokers $bootstrapBrokers")
  }

  def findCoordinator(groupId: String): Node = {
    val requestBuilder = new GroupCoordinatorRequest.Builder(groupId)
    val response = sendAnyNode(ApiKeys.GROUP_COORDINATOR, requestBuilder).asInstanceOf[GroupCoordinatorResponse]
    Errors.forCode(response.errorCode).maybeThrow()
    response.node
  }

  def listGroups(node: Node): List[GroupOverview] = {
    val response = send(node, ApiKeys.LIST_GROUPS, new ListGroupsRequest.Builder()).asInstanceOf[ListGroupsResponse]
    Errors.forCode(response.errorCode).maybeThrow()
    response.groups.asScala.map(group => GroupOverview(group.groupId, group.protocolType)).toList
  }

  def getApiVersions(node: Node): List[ApiVersion] = {
    val response = send(node, ApiKeys.API_VERSIONS, new ApiVersionsRequest.Builder()).asInstanceOf[ApiVersionsResponse]
    Errors.forCode(response.errorCode).maybeThrow()
    response.apiVersions.asScala.toList
  }

  private def findAllBrokers(): List[Node] = {
    val request = MetadataRequest.Builder.allTopics()
    val response = sendAnyNode(ApiKeys.METADATA, request).asInstanceOf[MetadataResponse]
    val errors = response.errors
    if (!errors.isEmpty)
      debug(s"Metadata request contained errors: $errors")
    response.cluster.nodes.asScala.toList
  }

  def listAllGroups(): Map[Node, List[GroupOverview]] = {
    findAllBrokers.map { broker =>
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
      groups.filter(_.protocolType == ConsumerProtocol.PROTOCOL_TYPE)
    }
  }

  def listAllGroupsFlattened(): List[GroupOverview] = {
    listAllGroups.values.flatten.toList
  }

  def listAllConsumerGroupsFlattened(): List[GroupOverview] = {
    listAllGroupsFlattened.filter(_.protocolType == ConsumerProtocol.PROTOCOL_TYPE)
  }

  def listGroupOffsets(groupId: String): Map[TopicPartition, Long] = {
    val coordinator = findCoordinator(groupId)
    val responseBody = send(coordinator, ApiKeys.OFFSET_FETCH, OffsetFetchRequest.Builder.allTopicPartitions(groupId))
    val response = responseBody.asInstanceOf[OffsetFetchResponse]
    if (response.hasError)
      throw response.error.exception
    response.maybeThrowFirstPartitionError
    response.responseData.asScala.map { case (tp, partitionData) => (tp, partitionData.offset) }.toMap
  }

  def listAllBrokerVersionInfo(): Map[Node, Try[NodeApiVersions]] =
    findAllBrokers.map { broker =>
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

  def describeConsumerGroup(groupId: String): ConsumerGroupSummary = {
    val coordinator = findCoordinator(groupId)
    val responseBody = send(coordinator, ApiKeys.DESCRIBE_GROUPS,
        new DescribeGroupsRequest.Builder(Collections.singletonList(groupId)))
    val response = responseBody.asInstanceOf[DescribeGroupsResponse]
    val metadata = response.groups.get(groupId)
    if (metadata == null)
      throw new KafkaException(s"Response from broker contained no metadata for group $groupId")
    if (metadata.state != "Dead" && metadata.state != "Empty" && metadata.protocolType != ConsumerProtocol.PROTOCOL_TYPE)
      throw new IllegalArgumentException(s"Consumer Group $groupId with protocol type '${metadata.protocolType}' is not a valid consumer group")

    Errors.forCode(metadata.errorCode()).maybeThrow()
    val consumers = metadata.members.asScala.map { consumer =>
      ConsumerSummary(consumer.memberId, consumer.clientId, consumer.clientHost, metadata.state match {
        case "Stable" =>
          val assignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(Utils.readBytes(consumer.memberAssignment)))
          assignment.partitions.asScala.toList
        case _ =>
          List()
      })
    }.toList

    ConsumerGroupSummary(metadata.state, metadata.protocol, Some(consumers), coordinator)
  }

  def close() {
    client.close()
  }

}

object AdminClient {
  val DefaultConnectionMaxIdleMs = 9 * 60 * 1000
  val DefaultRequestTimeoutMs = 5000
  val DefaultMaxInFlightRequestsPerConnection = 100
  val DefaultReconnectBackoffMs = 50
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
      .define(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        ConfigDef.Type.STRING,
        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
        ConfigDef.Importance.MEDIUM,
        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
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
    val time = Time.SYSTEM
    val metrics = new Metrics(time)
    val metadata = new Metadata
    val channelBuilder = ClientUtils.createChannelBuilder(config)

    val brokerUrls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
    val brokerAddresses = ClientUtils.parseAndValidateAddresses(brokerUrls)
    val bootstrapCluster = Cluster.bootstrap(brokerAddresses)
    metadata.update(bootstrapCluster, 0)

    val selector = new Selector(
      DefaultConnectionMaxIdleMs,
      metrics,
      time,
      "admin",
      channelBuilder)

    val networkClient = new NetworkClient(
      selector,
      metadata,
      "admin-" + AdminClientIdSequence.getAndIncrement(),
      DefaultMaxInFlightRequestsPerConnection,
      DefaultReconnectBackoffMs,
      DefaultSendBufferBytes,
      DefaultReceiveBufferBytes,
      DefaultRequestTimeoutMs,
      time,
      true)

    val highLevelClient = new ConsumerNetworkClient(
      networkClient,
      metadata,
      time,
      DefaultRetryBackoffMs,
      DefaultRequestTimeoutMs)

    new AdminClient(
      time,
      DefaultRequestTimeoutMs,
      highLevelClient,
      bootstrapCluster.nodes.asScala.toList)
  }
}
