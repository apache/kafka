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

import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.clients.{ApiVersions,  ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.AbstractRequest.Builder

import scala.collection.JavaConverters._

class LeaderPusherNetworkClient( brokerConfig: KafkaConfig,
                                 metrics: Metrics,
                                 time: Time,
                                 clientId: String ) {
  private val logContext = new LogContext(s"[LeaderPusherNetworkClient clientId=$clientId]")

  private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs
  private val requestTimeoutMs: Int = brokerConfig.requestTimeoutMs

  private val networkClient = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      brokerConfig.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      brokerConfig,
      brokerConfig.interBrokerListenerName,
      brokerConfig.saslMechanismInterBrokerProtocol,
      time,
      brokerConfig.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      new Metrics(),
      time,
      "leader-pusher",
      Map.empty[String,String].asJava,
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
      ClientDnsLookup.DEFAULT,
      time,
      false,
      new ApiVersions,
      logContext
    )
  }


  def sendRequest(node: Node,requestBuilder:Builder[_ <: AbstractRequest],callback: RequestCompletionHandler ) = {
    try {
      if (!NetworkClientUtils.awaitReady(networkClient, node, time, socketTimeout)){
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
      }
      else {
        val now = time.milliseconds()
        val clientRequest: ClientRequest = networkClient.newClientRequest(node.id.toString, requestBuilder, now, true,
          requestTimeoutMs, callback)
        networkClient.send(clientRequest, now)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(node.id.toString)
        throw e
    }
  }


  def poll(timeout: Int = 0) ={
    networkClient.poll(timeout, time.milliseconds())
  }


  def initiateClose(): Unit = {
    networkClient.initiateClose()
  }

  def close(): Unit = {
    networkClient.close()
  }
}
