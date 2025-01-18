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

// This file contains objects for encoding/decoding data stored in ZooKeeper nodes (znodes).

object ConfigZNode {
  def path = "/config"
}

object BrokersZNode {
  def path = "/brokers"
}

object TopicsZNode {
  def path = s"${BrokersZNode.path}/topics"
}

object TopicZNode {
  def path(topic: String) = s"${TopicsZNode.path}/$topic"
}

object ConfigEntityTypeZNode {
  def path(entityType: String) = s"${ConfigZNode.path}/$entityType"
}

object ConfigEntityZNode {
  def path(entityType: String, entityName: String) = s"${ConfigEntityTypeZNode.path(entityType)}/$entityName"
}

object ZkVersion {
  val MatchAnyVersion: Int = -1 // if used in a conditional set, matches any version (the value should match ZooKeeper codebase)
}

object DelegationTokenAuthZNode {
  def path = "/delegation_token"
}

object DelegationTokenChangeNotificationZNode {
  def path =  s"${DelegationTokenAuthZNode.path}/token_changes"
}

object DelegationTokenChangeNotificationSequenceZNode {
  val SequenceNumberPrefix = "token_change_"
}

object DelegationTokensZNode {
  def path = s"${DelegationTokenAuthZNode.path}/tokens"
}
