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

import kafka.api.{ApiVersion, KAFKA_0_10_0_IV1, LeaderAndIsr}
import kafka.cluster.{Broker, EndPoint}
import kafka.common.PartitionReassignment
import kafka.controller.{IsrChangeNotificationHandler, LeaderIsrAndControllerEpoch}
import kafka.security.auth.{Acl, Resource}
import kafka.security.auth.SimpleAclAuthorizer.VersionedAcls
import kafka.utils.{Json, Logging}
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.data.Stat

import scala.collection.JavaConverters._

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

object BrokerIdZNode {
  def path(id: Int) = s"${BrokerIdsZNode.path}/$id"
  def encode(id: Int,
             host: String,
             port: Int,
             advertisedEndpoints: Seq[EndPoint],
             jmxPort: Int,
             rack: Option[String],
             apiVersion: ApiVersion): Array[Byte] = {
    val version = if (apiVersion >= KAFKA_0_10_0_IV1) 4 else 2
    Broker.toJsonBytes(version, id, host, port, advertisedEndpoints, jmxPort, rack)
  }

  def decode(id: Int, bytes: Array[Byte]): Broker = {
    Broker.createBroker(id, new String(bytes, UTF_8))
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
      Json.parseBytes(bytes).map { js =>
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

@deprecated
object LegacyReassignPartitionsZNode {
  def path = s"${AdminZNode.path}/reassign_partitions"
  def encode(reassignment: collection.Map[TopicPartition, Seq[Int]]): Array[Byte] = {
    val reassignmentJson = reassignment.map { case (tp, replicas) =>
      Map("topic" -> tp.topic, "partition" -> tp.partition, "replicas" -> replicas.asJava).asJava
    }.asJava
    Json.encodeAsBytes(Map("version" -> 1, "partitions" -> reassignmentJson).asJava)
  }
  def decode(bytes: Array[Byte]): Map[TopicPartition, Seq[Int]] = Json.parseBytes(bytes).flatMap { js =>
    val reassignmentJson = js.asJsonObject
    val partitionsJsonOpt = reassignmentJson.get("partitions")
    partitionsJsonOpt.map { partitionsJson =>
      partitionsJson.asJsonArray.iterator.map { partitionFieldsJs =>
        val partitionFields = partitionFieldsJs.asJsonObject
        val topic = partitionFields("topic").to[String]
        val partition = partitionFields("partition").to[Int]
        val replicas = partitionFields("replicas").to[Seq[Int]]
        new TopicPartition(topic, partition) -> replicas
      }
    }
  }.map(_.toMap).getOrElse(Map.empty)
}

/**
  * {@code /admin/reassignments} Parent for individual reassignments
  */
object ReassignmentsZNode {
  def path = s"${AdminZNode.path}/reassignments"
}

object ReassignmentsTopicZNode {
  def path(topic: String) = s"${ReassignmentsZNode.path}/$topic"
  def fromPath(path: String, partition: Int): TopicPartition = {
    val index = path.lastIndexOf('/')
    val topic = path.substring(index + 1)
    new TopicPartition(topic, partition)
  }
}

/**
  * {@code /admin/reassignments/$topic/$partition} Individual reassignment
  */
object PartitionReassignmentZNode {
  def path(topicPartition: TopicPartition) = s"${ReassignmentsTopicZNode.path(topicPartition.topic)}/${topicPartition.partition}"
  def topicPath(topic: String) = s"${ReassignmentsZNode.path}/${topic}"

  def fromPath(path: String): TopicPartition = {
    val index = path.lastIndexOf('/')
    val partition = path.substring(index + 1).toInt
    ReassignmentsTopicZNode.fromPath(path.substring(0, index), partition)
  }

  def encode(reassignment: PartitionReassignment): Array[Byte] = {
    Json.encodeAsBytes(Map("version" -> 1,
      "original"-> reassignment.originalAssignment.asJava,
      "assignment" -> reassignment.newAssignments.asJava,
      "legacy" -> reassignment.legacy).asJava)
  }
  def decode(bytes: Array[Byte]): Option[PartitionReassignment] = Json.parseBytes(bytes).flatMap { js =>
    val jsObj = js.asJsonObject
    Some(PartitionReassignment(
      jsObj("original").asJsonArray.iterator.map(_.to[Int]).toSeq,
      jsObj("assignment").asJsonArray.iterator.map(_.to[Int]).toSeq,
      jsObj("legacy").to[Boolean]
      )
    )
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

/**
  * {@code /admin/reassignment_changes} parent of changes znode
  */
object ReassignmentRequestsZNode {
  def path = s"${AdminZNode.path}/reassignment_requests"
}

/**
  * {@code /admin/reassignment_changes/change_XXX} notification of changes to reassignments.
  * If a {@code /admin/reassignments/$topic-$partition} needs to be changed
  * the client creates a {@code /admin/reassignment_changes/change_XXX} node, the controler is watching the children of
  * {@code /admin/reassignment_changes} and updates then {@code /admin/reassignments/$topic-$partition}.
  *
  * This avoids the need for the controller to watch each
  * {@code /admin/reassignments/$topic-$partition}.
  */
object ReassignmentRequestZNode extends Logging {
  def prefix = s"${ReassignmentRequestsZNode.path}/request_"
  def path(changeZnode: String) = s"${ReassignmentRequestsZNode.path}/$changeZnode"

  def encode(assignments: Map[TopicPartition,Seq[Int]], legacy: Boolean): Array[Byte] = {
    var jsonMap = Map("version" -> 1,
      "assignments" -> assignments.groupBy{
        case (tp, assignment) => tp.topic()
      }.map {
        case (topic, assigmentMap) =>
          topic -> assigmentMap.map {
            case (tp, assignment) => tp.partition.toString -> assignment.asJava
          }.asJava
      }.asJava)
    if (legacy) {
      jsonMap += "legacy" -> true
    }
    Json.encodeAsBytes(jsonMap.asJava)
  }

  def decode(bytes: Array[Byte]): Option[(Map[TopicPartition, Seq[Int]], Boolean)] = {
    Json.parseBytes(bytes).flatMap { js =>
      val legacy = js.asJsonObject.get("legacy").map(_.to[Boolean]).getOrElse(false)
      val assignments = js.asJsonObject("assignments").to[Map[String,Map[String,Seq[Int]]]].flatMap{
        case (topic, partitionAssignments) =>
          partitionAssignments.map { case (partition, assignment) =>
            new TopicPartition(topic, partition.toInt) -> assignment
          }
      }
      Some(assignments->legacy)
    }
  }
}

object ConsumerOffset {
  def path(group: String, topic: String, partition: Integer) = s"/consumers/${group}/offset/${topic}/${partition}"
  def encode(offset: Long): Array[Byte] = offset.toString.getBytes(UTF_8)
  def decode(bytes: Array[Byte]): Option[Long] = Option(bytes).map(new String(_, UTF_8).toLong)
}

object ZkVersion {
  val NoVersion = -1
}

object ZkStat {
  val NoStat = new Stat()
}

object StateChangeHandlers {
  val ControllerHandler = "controller-state-change-handler"
  def zkNodeChangeListenerHandler(seqNodeRoot: String) = s"change-notification-$seqNodeRoot"
}

/**
 * The root acl storage node. Under this node there will be one child node per resource type (Topic, Cluster, Group).
 * under each resourceType there will be a unique child for each resource instance and the data for that child will contain
 * list of its acls as a json object. Following gives an example:
 *
 * <pre>
 * /kafka-acl/Topic/topic-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
 * /kafka-acl/Cluster/kafka-cluster => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
 * /kafka-acl/Group/group-1 => {"version": 1, "acls": [ { "host":"host1", "permissionType": "Allow","operation": "Read","principal": "User:alice"}]}
 * </pre>
 */
object AclZNode {
  def path = "/kafka-acl"
}

object ResourceTypeZNode {
  def path(resourceType: String) = s"${AclZNode.path}/$resourceType"
}

object ResourceZNode {
  def path(resource: Resource) = s"${AclZNode.path}/${resource.resourceType}/${resource.name}"
  def encode(acls: Set[Acl]): Array[Byte] = {
    Json.encodeAsBytes(Acl.toJsonCompatibleMap(acls).asJava)
  }
  def decode(bytes: Array[Byte], stat: Stat): VersionedAcls = VersionedAcls(Acl.fromBytes(bytes), stat.getVersion)
}

object AclChangeNotificationZNode {
  def path = "/kafka-acl-changes"
}

object AclChangeNotificationSequenceZNode {
  val SequenceNumberPrefix = "acl_changes_"
  def createPath = s"${AclChangeNotificationZNode.path}/$SequenceNumberPrefix"
  def deletePath(sequenceNode: String) = s"${AclChangeNotificationZNode.path}/${sequenceNode}"
  def encode(resourceName : String): Array[Byte] = resourceName.getBytes(UTF_8)
  def decode(bytes: Array[Byte]): String = new String(bytes, UTF_8)
}
