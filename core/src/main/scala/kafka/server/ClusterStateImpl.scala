/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.admin.AdminUtils
import kafka.log.LogConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.server.policy.ClusterState
import org.apache.kafka.server.policy.TopicState

import scala.collection.JavaConverters._

class ClusterStateImpl(adminManager: AdminManager, listenerName: ListenerName) extends ClusterState {

  /**
    * Returns the current state of the given topic, or null if the topic does not exist.
    */
  override def topicState(topicName: String) = {
    new TopicStateImpl(topicName, adminManager, listenerName)
  }

  /**
    * Returns all the topics in the cluster, including internal topics if
    * {@code includeInternal} is true, and including those marked for deletion
    * if {@code includeMarkedForDeletion} is true.
    */
  override def topics(includeInternal: Boolean, includeMarkedForDeletion: Boolean) = {
    if (includeInternal && includeMarkedForDeletion)
      adminManager.metadataCache.getAllTopics().asJava
    else {
      adminManager.metadataCache.getAllTopics().filter { case (topic) =>
        val topicMeta = adminManager.metadataCache.getTopicMetadata(Set(topic), null).head
        (includeInternal || !topicMeta.isInternal) && (includeMarkedForDeletion || adminManager.zkUtils.isTopicMarkedForDeletion(topic))
      }.asJava
    }
  }

  /**
    * The number of brokers in the cluster.
    */
  override def clusterSize() = {
    adminManager.zkUtils.getAllBrokersInCluster().size
  }
}

class TopicStateImpl(topicName: String, adminManager: AdminManager, listenerName: ListenerName) extends TopicState {
  Topic.validate(topicName)
  val topicMeta = adminManager.metadataCache.getTopicMetadata(Set(topicName), listenerName).head // TODO this fugly null
  /**
    * The number of partitions of the topic.
    */
  override def numPartitions() = topicMeta.partitionMetadata().size

  /**
    * The replication factor of the topic. More precisely, the number of assigned replicas for partition 0.
    * // TODO what about during reassignment
    */
  override def replicationFactor() = {
    topicMeta.partitionMetadata().asScala.find ( _.partition == 0 ) match {
      case Some(partition) => partition.replicas.size.toShort
      case None => (-1).toShort// TODO -1 or null?
    }
  }

  /**
    * A map of the replica assignments of the topic, with partition ids as keys and
    * the assigned brokers as the corresponding values.
    * // TODO what about during reassignment
    */
  override def replicasAssignments() = {
    topicMeta.partitionMetadata().asScala.map { case (partitionMeta) =>
      new Integer(partitionMeta.partition) -> partitionMeta.replicas().asScala.map(node => new Integer(node.id)).asJava
    }.toMap.asJava
  }

  /**
    * The topic config.
    */
  override def configs() = {
    // TODO Copied from adminManager: Factor out a common method
    // Consider optimizing this by caching the configs or retrieving them from the `Log` when possible
    val topicProps = AdminUtils.fetchEntityConfig(adminManager.zkUtils, ConfigType.Topic, topicName)
    val logConfig = LogConfig.fromProps(KafkaServer.copyKafkaConfigToLog(adminManager.config), topicProps)
    logConfig.values.asScala.map { case (key, value) =>
      val configEntryType = logConfig.typeOf(key)
      // if there were passwords in the topic config, we shouldn't leak them to the policy.
      val isSensitive = configEntryType == ConfigDef.Type.PASSWORD
      val valueAsString =
        if (isSensitive) null
        else ConfigDef.convertToString(value, configEntryType)
      key -> valueAsString
    }.asJava
  }

  /**
    * Returns whether the topic is marked for deletion.
    */
  override def markedForDeletion() = adminManager.zkUtils.isTopicMarkedForDeletion(topicName)

  /**
    * Returns whether the topic is an internal topic.
    */
  override def internal() = topicMeta.isInternal
}