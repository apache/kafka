/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics.clientmetrics

import org.apache.kafka.common.metrics.ClientTelemetryReceiver
import org.apache.kafka.common.requests.{PushTelemetryRequest, RequestContext}

import scala.collection.mutable.ListBuffer

object ClientMetricsReceiverPlugin {
  val cmReceivers  = new ListBuffer[ClientTelemetryReceiver]()
  def isEmpty = cmReceivers.isEmpty

  def add(receiver: ClientTelemetryReceiver) = {
    cmReceivers.append(receiver)
  }

  def getPayLoad(request: PushTelemetryRequest) : ClientMetricsPayload = {
     new ClientMetricsPayload(request.getClientInstanceId, request.isClientTerminating,
       request.getMetricsContentType, request.getMetricsData)
  }

  def exportMetrics(context: RequestContext, request: PushTelemetryRequest): Unit = {
    val payload = getPayLoad(request)
    cmReceivers.foreach(x => x.exportMetrics(context, payload))
  }

}
