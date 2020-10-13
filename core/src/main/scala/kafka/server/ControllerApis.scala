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
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AlterIsrRequest
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.CLUSTER
import org.apache.kafka.common.utils.Time
import org.apache.kafka.controller.{Controller, LeaderAndIsr}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Request handler for Controller APIs
 */
class ControllerApis(val apisUtil: ApisUtils,
                     val time: Time,
                     val controller: Controller) extends ApiRequestHandler with Logging {

  override def handle(request: RequestChannel.Request): Unit = {
    try {
      request.header.apiKey match {
        case ApiKeys.ALTER_ISR => handleAlterIsrRequest(request)
          // TODO other APIs
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey()}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => apisUtil.handleError(request, e)
    } finally {

    }
  }

  def handleAlterIsrRequest(request: RequestChannel.Request): Unit = {
    val alterIsrRequest = request.body[AlterIsrRequest]
    if (!apisUtil.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      val isrsToAlter = mutable.Map[TopicPartition, LeaderAndIsr]()
      alterIsrRequest.data.topics.forEach { topicReq =>
        topicReq.partitions.forEach { partitionReq =>
          val tp = new TopicPartition(topicReq.name, partitionReq.partitionIndex)
          val newIsr = partitionReq.newIsr()
          isrsToAlter.put(tp, new LeaderAndIsr(
            alterIsrRequest.data.brokerId,
            partitionReq.leaderEpoch,
            newIsr,
            partitionReq.currentIsrVersion))
        }
      }

      controller.alterIsr(
        alterIsrRequest.data().brokerId(),
        alterIsrRequest.data().brokerEpoch(),
        isrsToAlter.asJava)
    }
  }
}
