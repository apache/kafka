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
package kafka.metrics

import kafka.metrics.ClientMetricsTestUtils.{createCMSubscription, getCM}
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMatchingParams.{CLIENT_SOFTWARE_NAME, CLIENT_SOFTWARE_VERSION}
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics
import kafka.metrics.clientmetrics.{ClientMetricsCache, ClientMetricsConfig, CmClientInformation}
import kafka.server.ClientMetricsManager
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.requests.{GetTelemetrySubscriptionRequest, GetTelemetrySubscriptionResponse}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterEach, Test}

import java.util.Properties

class ClientMetricsRequestResponseTest {

  @AfterEach
  def cleanup(): Unit = {
    ClientMetricsConfig.clearClientSubscriptions()
    ClientMetricsCache.getInstance.clear()
  }

  private def sendGetSubscriptionRequest(clientInfo: CmClientInformation,
                                         id: Uuid = Uuid.ZERO_UUID): GetTelemetrySubscriptionResponse = {
    val request = new GetTelemetrySubscriptionRequest.Builder(id).build(0);
    getCM.processGetSubscriptionRequest(request, clientInfo, 20)
  }

  @Test def testGetClientMetricsRequestAndResponse(): Unit = {
    val subscription1 = createCMSubscription("cm_1")
    assertTrue(subscription1 != null)

    val clientInfo = CmClientInformation("testClient1", "clientId3", "Java", "11.1.0", "192.168.1.7", "9093")
    val response = sendGetSubscriptionRequest(clientInfo).data()
    assertTrue(response != null)
    val clientInstanceId = response.clientInstanceId()

    // verify all the parameters ..
    assertTrue(clientInstanceId != Uuid.ZERO_UUID)
    val cmClient = getCM.getClientInstance(clientInstanceId)
    assertTrue(cmClient != null)

    assertTrue(response.throttleTimeMs() == 20)
    assertTrue(response.deltaTemporality() == java.lang.Boolean.TRUE)
    assertTrue(response.pushIntervalMs() == cmClient.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)

    assertTrue(response.requestedMetrics().size == cmClient.getMetrics.size)
    response.requestedMetrics().forEach(x => assertTrue(cmClient.getMetrics.contains(x)))

    assertTrue(response.acceptedCompressionTypes().size() == ClientMetricsManager.getSupportedCompressionTypes.size)

    response.acceptedCompressionTypes().forEach(x => {
      assertTrue(ClientMetricsManager.getSupportedCompressionTypes.contains(x))
      assertTrue(CompressionType.values().contains(CompressionType.forId(x.toInt)))
    })
  }

  @Test def testRequestAndResponseWithNoMatchingMetrics(): Unit = {
    val subscription1 = createCMSubscription("cm_2")
    assertTrue(subscription1 != null)

    // Create a python client that do not have any matching subscriptions.
    val clientInfo = CmClientInformation("testClient1", "clientId3", "Python", "11.1.0", "192.168.1.7", "9093")
    var response = sendGetSubscriptionRequest(clientInfo).data()
    var cmClient = getCM.getClientInstance(response.clientInstanceId())
    val clientInstanceId = response.clientInstanceId()

    // Push interval must be set to the default push interval and requested metrics list should be empty
    assertTrue(response.pushIntervalMs() == ClientMetrics.DEFAULT_PUSH_INTERVAL &&
               response.pushIntervalMs() != subscription1.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)
    assertTrue(response.requestedMetrics().isEmpty)

    // Now create a client subscription with client id that matches with the client.
    val props = new Properties()
    val clientMatch = List(s"${CLIENT_SOFTWARE_NAME}=Python", s"${CLIENT_SOFTWARE_VERSION}=11.1.*")
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatch.mkString(","))
    val subscription2 = createCMSubscription("cm_2", props)
    assertTrue(subscription2 != null)

    // should have got the positive response with all the valid parameters
    response = sendGetSubscriptionRequest(clientInfo, clientInstanceId).data()
    cmClient = getCM.getClientInstance(clientInstanceId)
    assertTrue(response.pushIntervalMs() == subscription2.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)
    assertTrue(!response.requestedMetrics().isEmpty)
    response.requestedMetrics().forEach(x => assertTrue(cmClient.getMetrics.contains(x)))
  }

  @Test def testRequestWithAllMetricsSubscription(): Unit = {
    val clientInfo = CmClientInformation("testClient1", "clientId3", "Java", "11.1.0", "192.168.1.7", "9093")

    // Add first subscription with default metrics.
    createCMSubscription("subscription1")

    // Add second subscription that contains allMetrics flag set to true
    val props = new Properties
    props.put(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true")
    val subscription1 = createCMSubscription("cm_all_metrics", props)
    assertTrue(subscription1 != null)

    val response = sendGetSubscriptionRequest(clientInfo).data()

    // verify all the parameters ..
    assertTrue(response.clientInstanceId() != Uuid.ZERO_UUID)
    val cmClient = getCM.getClientInstance(response.clientInstanceId())
    assertTrue(cmClient != null)
    assertTrue(response.pushIntervalMs() == cmClient.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)
    assertTrue(response.requestedMetrics().size() == 1)
    assertTrue(response.requestedMetrics().get(0).isEmpty)
  }

  @Test def testRequestWithDisabledClient(): Unit = {
    val clientInfo = CmClientInformation("testClient5", "clientId5", "Java", "11.1.0", "192.168.1.7", "9093")

    val subscription1 = createCMSubscription("cm_4")
    assertTrue(subscription1 != null)

    // Submit a request to get the subscribed metrics
    var response = sendGetSubscriptionRequest(clientInfo).data()
    val clientInstanceId = response.clientInstanceId()
    var cmClient = getCM.getClientInstance(clientInstanceId)
    assertTrue(cmClient != null)

    val oldSubscriptionId = response.subscriptionId()
    assertTrue(response.pushIntervalMs() == subscription1.getPushIntervalMs)
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)
    response.requestedMetrics().forEach(x => assertTrue(subscription1.getSubscribedMetrics.contains(x)))

    // Now create a new client subscription with push interval set to 0.
    val props = new Properties()
    props.put(ClientMetrics.PushIntervalMs, 0.toString)
    val subscription2 = createCMSubscription("cm_update_2_disable", props)
    assertTrue(subscription2 != null)

    // should have got the invalid response with empty metrics list.
    // set the client instance id which is obtained in earlier request.
    val res = sendGetSubscriptionRequest(clientInfo, clientInstanceId)
    cmClient = getCM.getClientInstance(clientInstanceId)
    assertTrue(cmClient != null)

    response = res.data()
    assertTrue(response.pushIntervalMs() == 0)
    assertTrue(response.requestedMetrics().isEmpty)
    assertTrue(oldSubscriptionId != response.subscriptionId())
    assertTrue(response.subscriptionId() == cmClient.getSubscriptionId)
  }
}
