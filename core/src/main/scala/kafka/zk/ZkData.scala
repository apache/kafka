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
package kafka.zk

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonProcessingException
import kafka.api.{ApiVersion, KAFKA_0_10_0_IV1, LeaderAndIsr}
import kafka.cluster.{Broker, EndPoint}
import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.controller.{IsrChangeNotificationHandler, LeaderIsrAndControllerEpoch}
import kafka.security.auth.Resource.Separator
import kafka.security.auth.SimpleAclAuthorizer.VersionedAcls
import kafka.security.auth.{Acl, Resource, ResourceType}
import kafka.server.{ConfigType, DelegationTokenManager}
import kafka.utils.Json
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.{ACL, Stat}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, breakOut}
import scala.util.{Failure, Success, Try}

// This file contains objects for encoding/decoding data stored in ZooKeeper nodes (znodes).

object ControllerZNode {
  def path = "/controller"
  def encode(brokerId: Int, timestamp: Long): Array[Byte] = {
    Json.encodeAsBytes(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp.toString).asJava)
  }
  def decode(bytes: Array[Byte]): Option[Int] = Json.parseBytes(bytes).map { js =>
    js.asJsonObject("brokerid").to[Int]
  }
}

object ControllerEpochZNode {
  def path = "/controller_epoch"
  def encode(epoch: Int): Array[Byte] = epoch.toString.getBytes(UTF_8)
  def decode(bytes: Array[Byte]): Int = new String(bytes, UTF_8).toInt
}

object ConfigZNode {
  def path = "/config"
}

object BrokersZNode {
  def path = "/brokers"
}

object BrokerIdsZNode {
  def path = s"${BrokersZNode.path}/ids"
  def encode: Array[Byte] = null
}

object BrokerInfo {

  /**
   * Create a broker info with v4 json format (which includes multiple endpoints and rack) if
   * the apiVersion is 0.10.0.X or above. Register the broker with v2 json format otherwise.
   *
   * Due to KAFKA-3100, 0.9.0.0 broker and old clients will break if JSON version is above 2.
   *
   * We include v2 to make it possible for the broker to migrate from 0.9.0.0 to 0.10.0.X or above without having to
   * upgrade to 0.9.0.1 first (clients have to be upgraded to 0.9.0.1 in any case).
   */
  def apply(broker: Broker, apiVersion: ApiVersion, jmxPort: Int): BrokerInfo = {
    // see method documentation for the reason why we do this
    val version = if (apiVersion >= KAFKA_0_10_0_IV1) 4 else 2
    BrokerInfo(broker, version, jmxPort)
  }

}

case class BrokerInfo(broker: Broker, version: Int, jmxPort: Int) {
  val path: String = BrokerIdZNode.path(broker.id)
  def toJsonBytes: Array[Byte] = BrokerIdZNode.encode(this)
}

object BrokerIdZNode {
  private val HostKey = "host"
  private val PortKey = "port"
  private val VersionKey = "version"
  private val EndpointsKey = "endpoints"
  private val RackKey = "rack"
  private val JmxPortKey = "jmx_port"
  private val ListenerSecurityProtocolMapKey = "listener_security_protocol_map"
  private val TimestampKey = "timestamp"

  def path(id: Int) = s"${BrokerIdsZNode.path}/$id"

  /**
   * Encode to JSON bytes.
   *
   * The JSON format includes a top level host and port for compatibility with older clients.
   */
  def encode(version: Int, host: String, port: Int, advertisedEndpoints: Seq[EndPoint], jmxPort: Int,
             rack: Option[String]): Array[Byte] = {
    val jsonMap = collection.mutable.Map(VersionKey -> version,
      HostKey -> host,
      PortKey -> port,
      EndpointsKey -> advertisedEndpoints.map(_.connectionString).toBuffer.asJava,
      JmxPortKey -> jmxPort,
      TimestampKey -> Time.SYSTEM.absoluteMilliseconds().toString
    )
    rack.foreach(rack => if (version >= 3) jsonMap += (RackKey -> rack))

    if (version >= 4) {
      jsonMap += (ListenerSecurityProtocolMapKey -> advertisedEndpoints.map { endPoint =>
        endPoint.listenerName.value -> endPoint.securityProtocol.name
      }.toMap.asJava)
    }
    Json.encodeAsBytes(jsonMap.asJava)
  }

  def encode(brokerInfo: BrokerInfo): Array[Byte] = {
    val broker = brokerInfo.broker
    // the default host and port are here for compatibility with older clients that only support PLAINTEXT
    // we choose the first plaintext port, if there is one
    // or we register an empty endpoint, which means that older clients will not be able to connect
    val plaintextEndpoint = broker.endPoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).getOrElse(
      new EndPoint(null, -1, null, null))
    encode(brokerInfo.version, plaintextEndpoint.host, plaintextEndpoint.port, broker.endPoints, brokerInfo.jmxPort,
      broker.rack)
  }

  /**
    * Create a BrokerInfo object from id and JSON bytes.
    *
    * @param id
    * @param jsonBytes
    *
    * Version 1 JSON schema for a broker is:
    * {
    *   "version":1,
    *   "host":"localhost",
    *   "port":9092
    *   "jmx_port":9999,
    *   "timestamp":"2233345666"
    * }
    *
    * Version 2 JSON schema for a broker is:
    * {
    *   "version":2,
    *   "host":"localhost",
    *   "port":9092,
    *   "jmx_port":9999,
    *   "timestamp":"2233345666",
    *   "endpoints":["PLAINTEXT://host1:9092", "SSL://host1:9093"]
    * }
    *
    * Version 3 JSON schema for a broker is:
    * {
    *   "version":3,
    *   "host":"localhost",
    *   "port":9092,
    *   "jmx_port":9999,
    *   "timestamp":"2233345666",
    *   "endpoints":["PLAINTEXT://host1:9092", "SSL://host1:9093"],
    *   "rack":"dc1"
    * }
    *
    * Version 4 (current) JSON schema for a broker is:
    * {
    *   "version":4,
    *   "host":"localhost",
    *   "port":9092,
    *   "jmx_port":9999,
    *   "timestamp":"2233345666",
    *   "endpoints":["CLIENT://host1:9092", "REPLICATION://host1:9093"],
    *   "listener_security_protocol_map":{"CLIENT":"SSL", "REPLICATION":"PLAINTEXT"},
    *   "rack":"dc1"
    * }
    */
  def decode(id: Int, jsonBytes: Array[Byte]): BrokerInfo = {
    Json.tryParseBytes(jsonBytes) match {
      case Right(js) =>
        val brokerInfo = js.asJsonObject
        val version = brokerInfo(VersionKey).to[Int]
        val jmxPort = brokerInfo(JmxPortKey).to[Int]

        val endpoints =
          if (version < 1)
            throw new KafkaException("Unsupported version of broker registration: " +
              s"${new String(jsonBytes, UTF_8)}")
          else if (version == 1) {
            val host = brokerInfo(HostKey).to[String]
            val port = brokerInfo(PortKey).to[Int]
            val securityProtocol = SecurityProtocol.PLAINTEXT
            val endPoint = new EndPoint(host, port, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
            Seq(endPoint)
          }
          else {
            val securityProtocolMap = brokerInfo.get(ListenerSecurityProtocolMapKey).map(
              _.to[Map[String, String]].map { case (listenerName, securityProtocol) =>
                new ListenerName(listenerName) -> SecurityProtocol.forName(securityProtocol)
              })
            val listeners = brokerInfo(EndpointsKey).to[Seq[String]]
            listeners.map(EndPoint.createEndPoint(_, securityProtocolMap))
          }

        val rack = brokerInfo.get(RackKey).flatMap(_.to[Option[String]])
        BrokerInfo(Broker(id, endpoints, rack), version, jmxPort)
      case Left(e) =>
        throw new KafkaException(s"Failed to parse ZooKeeper registration for broker $id: " +
          s"${new String(jsonBytes, UTF_8)}", e)
    }
  }
}

object TopicsZNode {
  def path = s"${BrokersZNode.path}/topics"
}

object TopicZNode {
  def path(topic: String) = s"${TopicsZNode.path}/$topic"
  def encode(assignment: collection.Map[TopicPartition, Seq[Int]]): Array[Byte] = {
    val assignmentJson = assignment.map { case (partition, replicas) =>
      partition.partition.toString -> replicas.asJava
    }
    Json.encodeAsBytes(Map("version" -> 1, "partitions" -> assignmentJson.asJava).asJava)
  }
  def decode(topic: String, bytes: Array[Byte]): Map[TopicPartition, Seq[Int]] = {
    Json.parseBytes(bytes).flatMap { js =>
      val assignmentJson = js.asJsonObject
      val partitionsJsonOpt = assignmentJson.get("partitions").map(_.asJsonObject)
      partitionsJsonOpt.map { partitionsJson =>
        partitionsJson.iterator.map { case (partition, replicas) =>
          new TopicPartition(topic, partition.toInt) -> replicas.to[Seq[Int]]
        }
      }
    }.map(_.toMap).getOrElse(Map.empty)
  }
}

object TopicPartitionsZNode {
  def path(topic: String) = s"${TopicZNode.path(topic)}/partitions"
}

object TopicPartitionZNode {
  def path(partition: TopicPartition) = s"${TopicPartitionsZNode.path(partition.topic)}/${partition.partition}"
}

object TopicPartitionStateZNode {
  def path(partition: TopicPartition) = s"${TopicPartitionZNode.path(partition)}/state"
  def encode(leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch): Array[Byte] = {
    val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
    val controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
    Json.encodeAsBytes(Map("version" -> 1, "leader" -> leaderAndIsr.leader, "leader_epoch" -> leaderAndIsr.leaderEpoch,
      "controller_epoch" -> controllerEpoch, "isr" -> leaderAndIsr.isr.asJava).asJava)
  }
  def decode(bytes: Array[Byte], stat: Stat): Option[LeaderIsrAndControllerEpoch] = {
    Json.parseBytes(bytes).map { js =>
      val leaderIsrAndEpochInfo = js.asJsonObject
      val leader = leaderIsrAndEpochInfo("leader").to[Int]
      val epoch = leaderIsrAndEpochInfo("leader_epoch").to[Int]
      val isr = leaderIsrAndEpochInfo("isr").to[List[Int]]
      val controllerEpoch = leaderIsrAndEpochInfo("controller_epoch").to[Int]
      val zkPathVersion = stat.getVersion
      LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch)
    }
  }
}

object ConfigEntityTypeZNode {
  def path(entityType: String) = s"${ConfigZNode.path}/$entityType"
}

object ConfigEntityZNode {
  def path(entityType: String, entityName: String) = s"${ConfigEntityTypeZNode.path(entityType)}/$entityName"
  def encode(config: Properties): Array[Byte] = {
    Json.encodeAsBytes(Map("version" -> 1, "config" -> config).asJava)
  }
  def decode(bytes: Array[Byte]): Properties = {
    val props = new Properties()
    if (bytes != null) {
      Json.parseBytes(bytes).foreach { js =>
        val configOpt = js.asJsonObjectOption.flatMap(_.get("config").flatMap(_.asJsonObjectOption))
        configOpt.foreach(config => config.iterator.foreach { case (k, v) => props.setProperty(k, v.to[String]) })
      }
    }
    props
  }
}

object ConfigEntityChangeNotificationZNode {
  def path = s"${ConfigZNode.path}/changes"
}

object ConfigEntityChangeNotificationSequenceZNode {
  val SequenceNumberPrefix = "config_change_"
  def createPath = s"${ConfigEntityChangeNotificationZNode.path}/$SequenceNumberPrefix"
  def encode(sanitizedEntityPath: String): Array[Byte] = Json.encodeAsBytes(
    Map("version" -> 2, "entity_path" -> sanitizedEntityPath).asJava)
}

object IsrChangeNotificationZNode {
  def path = "/isr_change_notification"
}

object IsrChangeNotificationSequenceZNode {
  val SequenceNumberPrefix = "isr_change_"
  def path(sequenceNumber: String = "") = s"${IsrChangeNotificationZNode.path}/$SequenceNumberPrefix$sequenceNumber"
  def encode(partitions: collection.Set[TopicPartition]): Array[Byte] = {
    val partitionsJson = partitions.map(partition => Map("topic" -> partition.topic, "partition" -> partition.partition).asJava)
    Json.encodeAsBytes(Map("version" -> IsrChangeNotificationHandler.Version, "partitions" -> partitionsJson.asJava).asJava)
  }

  def decode(bytes: Array[Byte]): Set[TopicPartition] = {
    Json.parseBytes(bytes).map { js =>
      val partitionsJson = js.asJsonObject("partitions").asJsonArray
      partitionsJson.iterator.map { partitionsJson =>
        val partitionJson = partitionsJson.asJsonObject
        val topic = partitionJson("topic").to[String]
        val partition = partitionJson("partition").to[Int]
        new TopicPartition(topic, partition)
      }
    }
  }.map(_.toSet).getOrElse(Set.empty)
  def sequenceNumber(path: String) = path.substring(path.lastIndexOf(SequenceNumberPrefix) + SequenceNumberPrefix.length)
}

object LogDirEventNotificationZNode {
  def path = "/log_dir_event_notification"
}

object LogDirEventNotificationSequenceZNode {
  val SequenceNumberPrefix = "log_dir_event_"
  val LogDirFailureEvent = 1
  def path(sequenceNumber: String) = s"${LogDirEventNotificationZNode.path}/$SequenceNumberPrefix$sequenceNumber"
  def encode(brokerId: Int) = {
    Json.encodeAsBytes(Map("version" -> 1, "broker" -> brokerId, "event" -> LogDirFailureEvent).asJava)
  }
  def decode(bytes: Array[Byte]): Option[Int] = Json.parseBytes(bytes).map { js =>
    js.asJsonObject("broker").to[Int]
  }
  def sequenceNumber(path: String) = path.substring(path.lastIndexOf(SequenceNumberPrefix) + SequenceNumberPrefix.length)
}

object AdminZNode {
  def path = "/admin"
}

object DeleteTopicsZNode {
  def path = s"${AdminZNode.path}/delete_topics"
}

object DeleteTopicsTopicZNode {
  def path(topic: String) = s"${DeleteTopicsZNode.path}/$topic"
}

object ReassignPartitionsZNode {

  /**
    * The assignment of brokers for a `TopicPartition`.
    *
    * A replica assignment consists of a `topic`, `partition` and a list of `replicas`, which
    * represent the broker ids that the `TopicPartition` is assigned to.
    */
  case class ReplicaAssignment(@BeanProperty @JsonProperty("topic") topic: String,
                               @BeanProperty @JsonProperty("partition") partition: Int,
                               @BeanProperty @JsonProperty("replicas") replicas: java.util.List[Int])

  /**
    * An assignment consists of a `version` and a list of `partitions`, which represent the
    * assignment of topic-partitions to brokers.
    */
  case class PartitionAssignment(@BeanProperty @JsonProperty("version") version: Int,
                                 @BeanProperty @JsonProperty("partitions") partitions: java.util.List[ReplicaAssignment])

  def path = s"${AdminZNode.path}/reassign_partitions"

  def encode(reassignmentMap: collection.Map[TopicPartition, Seq[Int]]): Array[Byte] = {
    val reassignment = PartitionAssignment(1,
      reassignmentMap.toSeq.map { case (tp, replicas) =>
        ReplicaAssignment(tp.topic, tp.partition, replicas.asJava)
      }.asJava
    )
    Json.encodeAsBytes(reassignment)
  }

  def decode(bytes: Array[Byte]): Either[JsonProcessingException, collection.Map[TopicPartition, Seq[Int]]] =
    Json.parseBytesAs[PartitionAssignment](bytes).right.map { partitionAssignment =>
      partitionAssignment.partitions.asScala.map { replicaAssignment =>
        new TopicPartition(replicaAssignment.topic, replicaAssignment.partition) -> replicaAssignment.replicas.asScala
      }(breakOut)
    }
}

object PreferredReplicaElectionZNode {
  def path = s"${AdminZNode.path}/preferred_replica_election"
  def encode(partitions: Set[TopicPartition]): Array[Byte] = {
    val jsonMap = Map("version" -> 1,
      "partitions" -> partitions.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition).asJava).asJava)
    Json.encodeAsBytes(jsonMap.asJava)
  }
  def decode(bytes: Array[Byte]): Set[TopicPartition] = Json.parseBytes(bytes).map { js =>
    val partitionsJson = js.asJsonObject("partitions").asJsonArray
    partitionsJson.iterator.map { partitionsJson =>
      val partitionJson = partitionsJson.asJsonObject
      val topic = partitionJson("topic").to[String]
      val partition = partitionJson("partition").to[Int]
      new TopicPartition(topic, partition)
    }
  }.map(_.toSet).getOrElse(Set.empty)
}

//old consumer path znode
object ConsumerPathZNode {
  def path = "/consumers"
}

object ConsumerOffset {
  def path(group: String, topic: String, partition: Integer) = s"${ConsumerPathZNode.path}/${group}/offsets/${topic}/${partition}"
  def encode(offset: Long): Array[Byte] = offset.toString.getBytes(UTF_8)
  def decode(bytes: Array[Byte]): Option[Long] = Option(bytes).map(new String(_, UTF_8).toLong)
}

object ZkVersion {
  val MatchAnyVersion = -1 // if used in a conditional set, matches any version (the value should match ZooKeeper codebase)
  val UnknownVersion = -2  // Version returned from get if node does not exist (internal constant for Kafka codebase, unused value in ZK)
}

object ZkStat {
  val NoStat = new Stat()
}

object StateChangeHandlers {
  val ControllerHandler = "controller-state-change-handler"
  def zkNodeChangeListenerHandler(seqNodeRoot: String) = s"change-notification-$seqNodeRoot"
}

/**
  * Acls for resources are stored in ZK under two root paths:
  * <ul>
  *   <li>[[org.apache.kafka.common.resource.PatternType#LITERAL Literal]] patterns are stored under '/kafka-acl'.
  *   The format is JSON. See [[kafka.zk.ResourceZNode]] for details.</li>
  *   <li>All other patterns are stored under '/kafka-acl-extended/<i>pattern-type</i>'.
  *   The format is JSON. See [[kafka.zk.ResourceZNode]] for details.</li>
  * </ul>
  *
  * Under each root node there will be one child node per resource type (Topic, Cluster, Group, etc).
  * Under each resourceType there will be a unique child for each resource pattern and the data for that child will contain
  * list of its acls as a json object. Following gives an example:
  *
  * <pre>
  * // Literal patterns:
  * /kafka-acl/Topic/topic-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
  * /kafka-acl/Cluster/kafka-cluster => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
  *
  * // Prefixed patterns:
  * /kafka-acl-extended/PREFIXED/Group/group-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
  * </pre>
  *
  * Acl change events are also stored under two paths:
  * <ul>
  *   <li>[[org.apache.kafka.common.resource.PatternType#LITERAL Literal]] patterns are stored under '/kafka-acl-changes'.
  *   The format is a UTF8 string in the form: &lt;resource-type&gt;:&lt;resource-name&gt;</li>
  *   <li>All other patterns are stored under '/kafka-acl-extended-changes'
  *   The format is JSON, as defined by [[kafka.zk.ExtendedAclChangeEvent]]</li>
  * </ul>
  */
sealed trait ZkAclStore {
  val patternType: PatternType
  val aclPath: String

  def path(resourceType: ResourceType): String = s"$aclPath/$resourceType"

  def path(resourceType: ResourceType, resourceName: String): String = s"$aclPath/$resourceType/$resourceName"

  def changeStore: ZkAclChangeStore
}

object ZkAclStore {
  private val storesByType: Map[PatternType, ZkAclStore] = PatternType.values
    .filter(_.isSpecific)
    .map(patternType => (patternType, create(patternType)))
    .toMap

  val stores: Iterable[ZkAclStore] = storesByType.values

  val securePaths: Iterable[String] = stores
    .flatMap(store => Set(store.aclPath, store.changeStore.aclChangePath))

  def apply(patternType: PatternType): ZkAclStore = {
    storesByType.get(patternType) match {
      case Some(store) => store
      case None => throw new KafkaException(s"Invalid pattern type: $patternType")
    }
  }

  private def create(patternType: PatternType) = {
    patternType match {
      case PatternType.LITERAL => LiteralAclStore
      case _ => new ExtendedAclStore(patternType)
    }
  }
}

object LiteralAclStore extends ZkAclStore {
  val patternType: PatternType = PatternType.LITERAL
  val aclPath: String = "/kafka-acl"

  def changeStore: ZkAclChangeStore = LiteralAclChangeStore
}

class ExtendedAclStore(val patternType: PatternType) extends ZkAclStore {
  if (patternType == PatternType.LITERAL)
    throw new IllegalArgumentException("Literal pattern types are not supported")

  val aclPath: String = s"/kafka-acl-extended/${patternType.name.toLowerCase}"

  def changeStore: ZkAclChangeStore = ExtendedAclChangeStore
}

trait AclChangeNotificationHandler {
  def processNotification(resource: Resource): Unit
}

trait AclChangeSubscription extends AutoCloseable {
  def close(): Unit
}

case class AclChangeNode(path: String, bytes: Array[Byte])

sealed trait ZkAclChangeStore {
  val aclChangePath: String
  def createPath: String = s"$aclChangePath/${ZkAclChangeStore.SequenceNumberPrefix}"

  def decode(bytes: Array[Byte]): Resource

  protected def encode(resource: Resource): Array[Byte]

  def createChangeNode(resource: Resource): AclChangeNode = AclChangeNode(createPath, encode(resource))

  def createListener(handler: AclChangeNotificationHandler, zkClient: KafkaZkClient): AclChangeSubscription = {
    val rawHandler: NotificationHandler = new NotificationHandler {
      def processNotification(bytes: Array[Byte]): Unit =
        handler.processNotification(decode(bytes))
    }

    val aclChangeListener = new ZkNodeChangeNotificationListener(
      zkClient, aclChangePath, ZkAclChangeStore.SequenceNumberPrefix, rawHandler)

    aclChangeListener.init()

    new AclChangeSubscription {
      def close(): Unit = aclChangeListener.close()
    }
  }
}

object ZkAclChangeStore {
  val stores: Iterable[ZkAclChangeStore] = List(LiteralAclChangeStore, ExtendedAclChangeStore)

  def SequenceNumberPrefix = "acl_changes_"
}

case object LiteralAclChangeStore extends ZkAclChangeStore {
  val name = "LiteralAclChangeStore"
  val aclChangePath: String = "/kafka-acl-changes"

  def encode(resource: Resource): Array[Byte] = {
    if (resource.patternType != PatternType.LITERAL)
      throw new IllegalArgumentException("Only literal resource patterns can be encoded")

    val legacyName = resource.resourceType + Resource.Separator + resource.name
    legacyName.getBytes(UTF_8)
  }

  def decode(bytes: Array[Byte]): Resource = {
    val string = new String(bytes, UTF_8)
    string.split(Separator, 2) match {
        case Array(resourceType, resourceName, _*) => new Resource(ResourceType.fromString(resourceType), resourceName, PatternType.LITERAL)
        case _ => throw new IllegalArgumentException("expected a string in format ResourceType:ResourceName but got " + string)
      }
  }
}

case object ExtendedAclChangeStore extends ZkAclChangeStore {
  val name = "ExtendedAclChangeStore"
  val aclChangePath: String = "/kafka-acl-extended-changes"

  def encode(resource: Resource): Array[Byte] = {
    if (resource.patternType == PatternType.LITERAL)
      throw new IllegalArgumentException("Literal pattern types are not supported")

    Json.encodeAsBytes(ExtendedAclChangeEvent(
      ExtendedAclChangeEvent.currentVersion,
      resource.resourceType.name,
      resource.name,
      resource.patternType.name))
  }

  def decode(bytes: Array[Byte]): Resource = {
    val changeEvent = Json.parseBytesAs[ExtendedAclChangeEvent](bytes) match {
      case Right(event) => event
      case Left(e) => throw new IllegalArgumentException("Failed to parse ACL change event", e)
    }

    changeEvent.toResource match {
      case Success(r) => r
      case Failure(e) => throw new IllegalArgumentException("Failed to convert ACL change event to resource", e)
    }
  }
}

object ResourceZNode {
  def path(resource: Resource): String = ZkAclStore(resource.patternType).path(resource.resourceType, resource.name)

  def encode(acls: Set[Acl]): Array[Byte] = Json.encodeAsBytes(Acl.toJsonCompatibleMap(acls).asJava)
  def decode(bytes: Array[Byte], stat: Stat): VersionedAcls = VersionedAcls(Acl.fromBytes(bytes), stat.getVersion)
}

object ExtendedAclChangeEvent {
  val currentVersion: Int = 1
}

case class ExtendedAclChangeEvent(@BeanProperty @JsonProperty("version") version: Int,
                                  @BeanProperty @JsonProperty("resourceType") resourceType: String,
                                  @BeanProperty @JsonProperty("name") name: String,
                                  @BeanProperty @JsonProperty("patternType") patternType: String) {
  if (version > ExtendedAclChangeEvent.currentVersion)
    throw new UnsupportedVersionException(s"Acl change event received for unsupported version: $version")

  def toResource: Try[Resource] = {
    for {
      resType <- Try(ResourceType.fromString(resourceType))
      patType <- Try(PatternType.fromString(patternType))
      resource = Resource(resType, name, patType)
    } yield resource
  }
}

object ClusterZNode {
  def path = "/cluster"
}

object ClusterIdZNode {
  def path = s"${ClusterZNode.path}/id"

  def toJson(id: String): Array[Byte] = {
    Json.encodeAsBytes(Map("version" -> "1", "id" -> id).asJava)
  }

  def fromJson(clusterIdJson:  Array[Byte]): String = {
    Json.parseBytes(clusterIdJson).map(_.asJsonObject("id").to[String]).getOrElse {
      throw new KafkaException(s"Failed to parse the cluster id json $clusterIdJson")
    }
  }
}

object BrokerSequenceIdZNode {
  def path = s"${BrokersZNode.path}/seqid"
}

object ProducerIdBlockZNode {
  def path = "/latest_producer_id_block"
}

object DelegationTokenAuthZNode {
  def path = "/delegation_token"
}

object DelegationTokenChangeNotificationZNode {
  def path =  s"${DelegationTokenAuthZNode.path}/token_changes"
}

object DelegationTokenChangeNotificationSequenceZNode {
  val SequenceNumberPrefix = "token_change_"
  def createPath = s"${DelegationTokenChangeNotificationZNode.path}/$SequenceNumberPrefix"
  def deletePath(sequenceNode: String) = s"${DelegationTokenChangeNotificationZNode.path}/${sequenceNode}"
  def encode(tokenId : String): Array[Byte] = tokenId.getBytes(UTF_8)
  def decode(bytes: Array[Byte]): String = new String(bytes, UTF_8)
}

object DelegationTokensZNode {
  def path = s"${DelegationTokenAuthZNode.path}/tokens"
}

object DelegationTokenInfoZNode {
  def path(tokenId: String) =  s"${DelegationTokensZNode.path}/$tokenId"
  def encode(token: DelegationToken): Array[Byte] =  Json.encodeAsBytes(DelegationTokenManager.toJsonCompatibleMap(token).asJava)
  def decode(bytes: Array[Byte]): Option[TokenInformation] = DelegationTokenManager.fromBytes(bytes)
}

object ZkData {

  // Important: it is necessary to add any new top level Zookeeper path to the Seq
  val SecureRootPaths = Seq(AdminZNode.path,
    BrokersZNode.path,
    ClusterZNode.path,
    ConfigZNode.path,
    ControllerZNode.path,
    ControllerEpochZNode.path,
    IsrChangeNotificationZNode.path,
    ProducerIdBlockZNode.path,
    LogDirEventNotificationZNode.path,
    DelegationTokenAuthZNode.path) ++ ZkAclStore.securePaths

  // These are persistent ZK paths that should exist on kafka broker startup.
  val PersistentZkPaths = Seq(
    ConsumerPathZNode.path, // old consumer path
    BrokerIdsZNode.path,
    TopicsZNode.path,
    ConfigEntityChangeNotificationZNode.path,
    DeleteTopicsZNode.path,
    BrokerSequenceIdZNode.path,
    IsrChangeNotificationZNode.path,
    ProducerIdBlockZNode.path,
    LogDirEventNotificationZNode.path
  ) ++ ConfigType.all.map(ConfigEntityTypeZNode.path)

  val SensitiveRootPaths = Seq(
    ConfigEntityTypeZNode.path(ConfigType.User),
    ConfigEntityTypeZNode.path(ConfigType.Broker),
    DelegationTokensZNode.path
  )

  def sensitivePath(path: String): Boolean = {
    path != null && SensitiveRootPaths.exists(path.startsWith)
  }

  def defaultAcls(isSecure: Boolean, path: String): Seq[ACL] = {
    //Old Consumer path is kept open as different consumers will write under this node.
    if (!ConsumerPathZNode.path.equals(path) && isSecure) {
      val acls = new ArrayBuffer[ACL]
      acls ++= ZooDefs.Ids.CREATOR_ALL_ACL.asScala
      if (!sensitivePath(path))
        acls ++= ZooDefs.Ids.READ_ACL_UNSAFE.asScala
      acls
    } else ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala
  }
}
