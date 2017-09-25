/**
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
package kafka.server

import java.net.SocketTimeoutException

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.clients.{ApiVersions, ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.AbstractRequest.Builder

import scala.collection.JavaConverters._

trait BlockingSend {

  def sendRequest(requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): ClientResponse

  def close()
}

class ReplicaFetcherBlockingSend(sourceBroker: BrokerEndPoint,
                                 brokerConfig: KafkaConfig,
                                 metrics: Metrics,
                                 time: Time,
                                 fetcherId: Int,
                                 clientId: String,
                                 logContext: LogContext) extends BlockingSend {

  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)
  private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs

  private val networkClient = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      brokerConfig.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      brokerConfig,
      brokerConfig.interBrokerListenerName,
      brokerConfig.saslMechanismInterBrokerProtocol,
      brokerConfig.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      metrics,
      time,
      "replica-fetcher",
      Map("broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
      false,
      channelBuilder,
      logContext
    )
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      1,
      0,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      brokerConfig.replicaSocketReceiveBufferBytes,
      brokerConfig.requestTimeoutMs,
      time,
      false,
      new ApiVersions,
      logContext
    )
  }

  override def sendRequest(requestBuilder: Builder[_ <: AbstractRequest]): ClientResponse =  {
    try {
      if (!NetworkClientUtils.awaitReady(networkClient, sourceNode, time, socketTimeout))
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
      else {
        val clientRequest = networkClient.newClientRequest(sourceBroker.id.toString, requestBuilder,
          time.milliseconds(), true)
        NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(sourceBroker.id.toString)
        throw e
    }
  }

  def close(): Unit = {
    networkClient.close()
  }
}
