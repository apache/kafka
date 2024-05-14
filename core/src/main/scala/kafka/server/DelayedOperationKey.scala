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

import org.apache.kafka.common.{TopicIdPartition, TopicPartition}

/**
 * Keys used for delayed operation metrics recording
 */
trait DelayedOperationKey {
  def keyLabel: String
}

/* used by delayed-produce and delayed-fetch operations */
case class TopicPartitionOperationKey(topic: String, partition: Int) extends DelayedOperationKey {
  override def keyLabel: String = "%s-%d".format(topic, partition)
}

object TopicPartitionOperationKey {
  def apply(topicPartition: TopicPartition): TopicPartitionOperationKey = {
    apply(topicPartition.topic, topicPartition.partition)
  }
  def apply(topicIdPartition: TopicIdPartition): TopicPartitionOperationKey = {
    apply(topicIdPartition.topic, topicIdPartition.partition)
  }
}

/* used by delayed-join-group operations */
case class MemberKey(groupId: String, consumerId: String) extends DelayedOperationKey {
  override def keyLabel: String = "%s-%s".format(groupId, consumerId)
}

/* used by delayed-join operations */
case class GroupJoinKey(groupId: String) extends DelayedOperationKey {
  override def keyLabel: String = "join-%s".format(groupId)
}

/* used by delayed-sync operations */
case class GroupSyncKey(groupId: String) extends DelayedOperationKey {
  override def keyLabel: String = "sync-%s".format(groupId)
}

/* used by delayed-topic operations */
case class TopicKey(topic: String) extends DelayedOperationKey {
  override def keyLabel: String = topic
}
