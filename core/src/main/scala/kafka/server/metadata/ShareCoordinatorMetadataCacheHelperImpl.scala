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

package kafka.server.metadata

import org.apache.kafka.common.Node
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.server.share.ShareCoordinatorMetadataCacheHelper

import java.util
import scala.jdk.CollectionConverters._

class ShareCoordinatorMetadataCacheHelperImpl(
                                               val metadataCache: KRaftMetadataCache,
                                               val keyToPartitionSupplier: (String) => Int,
                                               val interBrokerListenerName: ListenerName
                                             ) extends ShareCoordinatorMetadataCacheHelper {
  override def containsTopic(topicName: String): Boolean = {
    metadataCache.contains(topicName)
  }

  override def getShareCoordinator(key: String, internalTopicName: String): Node = {
    if (metadataCache.contains(internalTopicName)) {
      val topicMetadata = metadataCache.getTopicMetadata(Set(internalTopicName), interBrokerListenerName)

      if (topicMetadata.headOption.isEmpty || topicMetadata.head.errorCode != Errors.NONE.code) {
        Node.noNode
      } else {
        val partition = keyToPartitionSupplier(key)
        val coordinatorEndpoint = topicMetadata.head.partitions.asScala
          .find(_.partitionIndex == partition)
          .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
          .flatMap(metadata => metadataCache.
            getAliveBrokerNode(metadata.leaderId, interBrokerListenerName))

        coordinatorEndpoint match {
          case Some(endpoint) =>
            endpoint
          case _ =>
            Node.noNode
        }
      }
    } else {
      Node.noNode
    }
  }

  override def getClusterNodes: util.List[Node] = {
    metadataCache.getAliveBrokerNodes(interBrokerListenerName).asJava
  }
}
