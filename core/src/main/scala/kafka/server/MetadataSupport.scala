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

import kafka.network.RequestChannel
import kafka.server.metadata.KRaftMetadataCache
import org.apache.kafka.common.requests.AbstractResponse

sealed trait MetadataSupport {
  /**
   * Provide a uniform way of getting to the ForwardingManager, which is a shared concept
   * despite being optional when using ZooKeeper and required when using Raft
   */
  val forwardingManager: Option[ForwardingManager]

  /**
   * Confirm that this instance is consistent with the given config
   *
   * @param config the config to check for consistency with this instance
   * @throws IllegalStateException if there is an inconsistency (Raft for a ZooKeeper config or vice-versa)
   */
  def ensureConsistentWith(config: KafkaConfig): Unit

  def canForward(): Boolean

  def forward(
    request: RequestChannel.Request,
    responseCallback: Option[AbstractResponse] => Unit
  ): Unit = {
    forwardingManager.get.forwardRequest(request, responseCallback)
  }
}

case class RaftSupport(fwdMgr: ForwardingManager,
                       metadataCache: KRaftMetadataCache)
    extends MetadataSupport {
  override val forwardingManager: Option[ForwardingManager] = Some(fwdMgr)

  override def ensureConsistentWith(config: KafkaConfig): Unit = {
    if (config.requiresZookeeper) {
      throw new IllegalStateException("Config specifies ZooKeeper but metadata support instance is for Raft")
    }
  }

  override def canForward(): Boolean = true
}
