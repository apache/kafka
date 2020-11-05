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

import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, EnvelopeRequest, EnvelopeResponse}
import org.apache.kafka.common.utils.Time

import scala.compat.java8.OptionConverters._

class ForwardingManager(metadataCache: kafka.server.MetadataCache,
                        time: Time,
                        metrics: Metrics,
                        config: KafkaConfig,
                        threadNamePrefix: Option[String] = None) extends
  BrokerToControllerChannelManagerImpl(metadataCache, time, metrics,
    config, "forwardingChannel", threadNamePrefix) with KafkaMetricsGroup {

  private val forwardingMetricName = "NumRequestsForwardingToControllerPerSec"

  def forwardRequest(request: RequestChannel.Request,
                     responseCallback: AbstractResponse => Unit): Unit = {
    val principalSerde = request.context.principalSerde.asScala.getOrElse(
      throw new IllegalArgumentException(s"Cannot deserialize principal from request $request " +
        "since there is no serde defined")
    )
    val serializedPrincipal = principalSerde.serialize(request.context.principal)
    val forwardRequestBuffer = request.buffer.duplicate()
    forwardRequestBuffer.flip()
    val envelopeRequest = new EnvelopeRequest.Builder(
      forwardRequestBuffer,
      serializedPrincipal,
      request.context.clientAddress.getAddress
    )

    def onClientResponse(clientResponse: ClientResponse): Unit = {
      val envelopeResponse = clientResponse.responseBody.asInstanceOf[EnvelopeResponse]
      val envelopeError = envelopeResponse.error()

      val response = if (envelopeError != Errors.NONE) {
        // An envelope error indicates broker misconfiguration (e.g. the principal serde
        // might not be defined on the receiving broker). In this case, we do not return
        // the error directly to the client since it would not be expected. Instead we
        // return `UNKNOWN_SERVER_ERROR` so that the user knows that there is a problem
        // on the broker.
        debug(s"Forwarded request $request failed with an error in envelope response $envelopeError")
        request.body[AbstractRequest].getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception())
      } else {
        AbstractResponse.parseResponse(envelopeResponse.responseData, request.header)
      }
      responseCallback(response)
    }

    sendRequest(envelopeRequest, onClientResponse)
  }

  override def start(): Unit = {
    super.start()
    newGauge(forwardingMetricName, () => requestQueue.size())
  }

  override def shutdown(): Unit = {
    removeMetric(forwardingMetricName)
    super.shutdown()
  }
}
