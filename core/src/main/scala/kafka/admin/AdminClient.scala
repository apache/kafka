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
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import kafka.common.KafkaException
import kafka.coordinator.{GroupOverview, GroupSummary, MemberSummary}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.internals.{ConsumerNetworkClient, ConsumerProtocol, RequestFuture}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{SystemTime, Time, Utils}
import org.apache.kafka.common.{Cluster, Node, TopicPartition}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class AdminClient(val time: Time,
                  val requestTimeoutMs: Int,
                  val client: ConsumerNetworkClient,
                  val bootstrapBrokers: List[Node]) extends Logging {

  private def send(target: Node,
                   api: ApiKeys,
                   request: AbstractRequest): Struct = {
    var future: RequestFuture[ClientResponse] = null

    future = client.send(target, api, request)
    client.poll(future)

    if (future.succeeded())
      return future.value().responseBody()
    else
      throw future.exception()
  }

  private def sendAnyNode(api: ApiKeys, request: AbstractRequest): Struct = {
    bootstrapBrokers.foreach {
      case broker =>
        try {
          return send(broker, api, request)
        } catch {
          case e: Exception =>
            debug(s"Request ${api} failed against node ${broker}", e)
        }
    }
    throw new RuntimeException(s"Request ${api} failed on brokers ${bootstrapBrokers}")
  }

  private def findCoordinator(groupId: String): Node = {
    val request = new GroupCoordinatorRequest(groupId)
    val responseBody = sendAnyNode(ApiKeys.GROUP_COORDINATOR, request)
    val response = new GroupCoordinatorResponse(responseBody)
    Errors.forCode(response.errorCode()).maybeThrow()
    response.node()
  }

  def listGroups(node: Node): List[GroupOverview] = {
    val responseBody = send(node, ApiKeys.LIST_GROUPS, new ListGroupsRequest())
    val response = new ListGroupsResponse(responseBody)
    Errors.forCode(response.errorCode()).maybeThrow()
    response.groups().map(group => GroupOverview(group.groupId(), group.protocolType())).toList
  }

  private def findAllBrokers(): List[Node] = {
    val request = new MetadataRequest(List[String]())
    val responseBody = sendAnyNode(ApiKeys.METADATA, request)
    val response = new MetadataResponse(responseBody)
    val errors = response.errors()
    if (!errors.isEmpty)
      debug(s"Metadata request contained errors: ${errors}")
    response.cluster().nodes().asScala.toList
  }

  def listAllGroups(): Map[Node, List[GroupOverview]] = {
    findAllBrokers.map {
      case broker =>
        broker -> {
          try {
            listGroups(broker)
          } catch {
            case e: Exception =>
              debug(s"Failed to find groups from broker ${broker}", e)
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

  def describeGroup(groupId: String): GroupSummary = {
    val coordinator = findCoordinator(groupId)
    val responseBody = send(coordinator, ApiKeys.DESCRIBE_GROUPS, new DescribeGroupsRequest(List(groupId).asJava))
    val response = new DescribeGroupsResponse(responseBody)
    val metadata = response.groups().get(groupId)
    if (metadata == null)
      throw new KafkaException(s"Response from broker contained no metadata for group ${groupId}")

    Errors.forCode(metadata.errorCode()).maybeThrow()
    val members = metadata.members().map { member =>
      val metadata = Utils.readBytes(member.memberMetadata())
      val assignment = Utils.readBytes(member.memberAssignment())
      MemberSummary(member.memberId(), member.clientId(), member.clientHost(), metadata, assignment)
    }.toList
    GroupSummary(metadata.state(), metadata.protocolType(), metadata.protocol(), members)
  }

  case class ConsumerSummary(memberId: String,
                             clientId: String,
                             clientHost: String,
                             assignment: List[TopicPartition])

  def describeConsumerGroup(groupId: String): List[ConsumerSummary] = {
    val group = describeGroup(groupId)
    if (group.state == "Dead")
      return List.empty[ConsumerSummary]

    if (group.protocolType != ConsumerProtocol.PROTOCOL_TYPE)
      throw new IllegalArgumentException(s"Group ${groupId} with protocol type '${group.protocolType}' is not a valid consumer group")

    if (group.state == "Stable") {
      group.members.map { member =>
        val assignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment))
        new ConsumerSummary(member.memberId, member.clientId, member.clientHost, assignment.partitions().asScala.toList)
      }
    } else {
      List.empty
    }
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
        CommonClientConfigs.BOOSTRAP_SERVERS_DOC)
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

  class AdminConfig(originals: Map[_,_]) extends AbstractConfig(AdminConfigDef, originals, false)

  def createSimplePlaintext(brokerUrl: String): AdminClient = {
    val config = Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> brokerUrl)
    create(new AdminConfig(config))
  }

  def create(props: Properties): AdminClient = create(props.asScala.toMap)

  def create(props: Map[String, _]): AdminClient = create(new AdminConfig(props))

  def create(config: AdminConfig): AdminClient = {
    val time = new SystemTime
    val metrics = new Metrics(time)
    val metadata = new Metadata
    val channelBuilder = ClientUtils.createChannelBuilder(config.values())

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
      time)

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
      bootstrapCluster.nodes().asScala.toList)
  }
}
