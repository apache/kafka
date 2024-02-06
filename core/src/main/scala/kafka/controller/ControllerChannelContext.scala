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
package kafka.controller

import kafka.cluster.Broker
import org.apache.kafka.common.{TopicPartition, Uuid}

trait ControllerChannelContext {
  def isTopicDeletionInProgress(topicName: String): Boolean

  def topicIds: collection.Map[String, Uuid]

  def liveBrokerIdAndEpochs: collection.Map[Int, Long]

  def liveOrShuttingDownBrokers: collection.Set[Broker]

  def isTopicQueuedUpForDeletion(topic: String): Boolean

  def isReplicaOnline(brokerId: Int, partition: TopicPartition): Boolean

  def partitionReplicaAssignment(partition: TopicPartition): collection.Seq[Int]

  def leaderEpoch(topicPartition: TopicPartition): Int

  def liveOrShuttingDownBrokerIds: collection.Set[Int]

  def partitionLeadershipInfo(topicPartition: TopicPartition): Option[LeaderIsrAndControllerEpoch]
}
