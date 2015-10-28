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
import java.util.concurrent.atomic.AtomicInteger

import kafka.coordinator.{GroupOverview, GroupSummary, MemberSummary}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.internals.{SendFailedException, ConsumerProtocol, ConsumerNetworkClient, RequestFuture}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, SaslConfigs, SslConfigs}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{SystemTime, Time, Utils}
import org.apache.kafka.common.{TopicPartition, Cluster, Node}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class AdminClient(val time: Time,
                  val requestTimeoutMs: Int,
                  val client: ConsumerNetworkClient) extends Logging {

  private def send(target: Node,
                   api: ApiKeys,
                   request: AbstractRequest): Option[Struct]= {
    var now = time.milliseconds()
    val deadline = now + requestTimeoutMs
    var future: RequestFuture[ClientResponse] = null

    do {
      future = client.send(target, api, request)
      client.poll(future)

      if (future.succeeded())
        return if (future.value().wasDisconnected()) {
          debug(s"Broker ${target} disconnected while handling request ${api}")
          None
        } else {
          Some(future.value().responseBody())
        }

      now = time.milliseconds()
    } while (now < deadline && future.exception().isInstanceOf[SendFailedException])

    debug(s"Request ${api} failed against broker ${target}", future.exception())
    None
  }

  private def sendAnyNode(api: ApiKeys, request: AbstractRequest): Option[Struct] = {
    val node = client.leastLoadedNode()
    if (node == null)
      return None
    send(node, api, request)
  }

  private def findCoordinator(groupId: String): Option[Node] = {
    val request = new GroupMetadataRequest(groupId)
    sendAnyNode(ApiKeys.GROUP_METADATA, request).flatMap{ responseBody =>
      val response = new GroupMetadataResponse(responseBody)
      if (response.errorCode() == Errors.NONE.code)
        Some(response.node())
      else
        None
    }
  }

  def listGroups(node: Node): Option[List[GroupOverview]] = {
    send(node, ApiKeys.LIST_GROUPS, new ListGroupsRequest()).flatMap{ responseBody =>
      val response = new ListGroupsResponse(responseBody)
      if (response.errorCode() == Errors.NONE.code)
        Some(response.groups().map(group => GroupOverview(group.groupId(), group.protocolType())).toList)
      else
        None
    }
  }

  private def findAllBrokers(): Option[List[Node]] = {
    val request = new MetadataRequest(List[String]())
    sendAnyNode(ApiKeys.METADATA, request).flatMap{ responseBody =>
      val response = new MetadataResponse(responseBody)
      Some(response.cluster().nodes().asScala.toList)
    }
  }

  def listAllGroups(): Map[Node, Option[List[GroupOverview]]] = {
    findAllBrokers match {
      case None => Map.empty
      case Some(brokers) => brokers.map{ broker =>
        broker -> listGroups(broker)
      }.toMap
    }
  }

  def listAllConsumerGroups(): Map[Node, Option[List[GroupOverview]]] = {
    listAllGroups().mapValues { maybeGroups =>
      maybeGroups.map(_.filter(_.protocolType == ConsumerProtocol.PROTOCOL_TYPE))
    }
  }

  def listAllGroupsFlattened = {
    listAllGroups.values.filter(_.isDefined).map(_.get).flatten.toList
  }

  def listAllConsumerGroupsFlattened = {
    listAllGroupsFlattened.filter(_.protocolType == ConsumerProtocol.PROTOCOL_TYPE)
  }

  def describeGroup(groupId: String): Option[GroupSummary] = {
    findCoordinator(groupId).flatMap{ coordinator =>
      send(coordinator, ApiKeys.DESCRIBE_GROUP, new DescribeGroupRequest(groupId)).flatMap{ struct =>
        val response = new DescribeGroupResponse(struct)
        if (response.errorCode() == Errors.NONE.code) {
          val members = response.members().map { member =>
            val metadata = Utils.readBytes(member.memberMetadata())
            val assignment = Utils.readBytes(member.memberAssignment())
            MemberSummary(member.memberId(), member.clientId(), member.clientHost(), metadata, assignment)
          }.toList
          Some(GroupSummary(response.state(), response.protocolType(), response.protocol(), members))
        } else {
          None
        }
      }
    }
  }

  def describeConsumerGroup(groupId: String): Option[Map[String, List[TopicPartition]]] = {
    describeGroup(groupId).map { group =>
      if (group.protocolType != ConsumerProtocol.PROTOCOL_TYPE)
        throw new IllegalArgumentException(s"Group ${groupId} is not a consumer group")
      group.members.map { member =>
        val assignment = ConsumerProtocol.deserializeAssignment(
          ByteBuffer.wrap(member.assignment))
        member.memberId -> assignment.partitions().asScala.toList
      }.toMap
    }
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

    SslConfigs.addSslSupport(config)
    SaslConfigs.addSaslSupport(config)
    config
  }

  class AdminConfig(originals: Map[_,_]) extends AbstractConfig(AdminConfigDef, originals, false)

  def createSimplePlaintext(brokerUrl: String): AdminClient = {
    val config = Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> brokerUrl)
    create(new AdminConfig(config))
  }

  def create(config: AdminConfig): AdminClient = {
    val time = new SystemTime
    val metrics = new Metrics(time)
    val metadata = new Metadata
    val channelBuilder = ClientUtils.createChannelBuilder(config.values())

    val brokerUrls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
    val brokerAddresses = ClientUtils.parseAndValidateAddresses(brokerUrls)
    metadata.update(Cluster.bootstrap(brokerAddresses), 0)

    val selector = new Selector(
      DefaultConnectionMaxIdleMs,
      metrics,
      time,
      "admin",
      Map[String, String](),
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
      DefaultRetryBackoffMs)

    new AdminClient(time, DefaultRequestTimeoutMs, highLevelClient)
  }
}