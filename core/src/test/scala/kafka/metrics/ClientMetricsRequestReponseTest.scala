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

import kafka.metrics.ClientMetricsTestUtils.{createCMSubscription, getCM, getClientInstance, getSerializedMetricsData, setupClientMetricsPlugin}
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMatchingParams.{CLIENT_SOFTWARE_NAME, CLIENT_SOFTWARE_VERSION}
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics
import kafka.metrics.clientmetrics.{ClientMetricsCache, ClientMetricsConfig, CmClientInformation}
import kafka.server.ClientMetricsManager
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.PushTelemetryRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.requests.{GetTelemetrySubscriptionRequest, GetTelemetrySubscriptionResponse, PushTelemetryRequest, PushTelemetryResponse}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNotNull, assertNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

import java.nio.ReadOnlyBufferException
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

  private def sendPushTelemetryRequest(data: PushTelemetryRequestData, clientInfo: CmClientInformation, delay: Int = 0): PushTelemetryResponse = {
    val request = new PushTelemetryRequest(data, 0)
    if (delay > 0) {
      Thread.sleep(delay + 1)
    }
    getCM.processPushTelemetryRequest(request, null, clientInfo, 5000)
  }

  @Test def testGetClientMetricsRequestAndResponse(): Unit = {
    val subscription1 = createCMSubscription("cm_1")
    assertNotNull(subscription1)

    val clientInfo = CmClientInformation("testClient1", "clientId3", "Java", "11.1.0", "192.168.1.7", "9093")
    val response = sendGetSubscriptionRequest(clientInfo).data()
    assertNotNull(response)
    val clientInstanceId = response.clientInstanceId()

    // verify all the parameters ..
    assertNotEquals(clientInstanceId, Uuid.ZERO_UUID)
    val cmClient = getClientInstance(clientInstanceId)
    assertNotNull(cmClient)

    assertEquals(response.throttleTimeMs(), 20)
    assertEquals(response.deltaTemporality(), java.lang.Boolean.TRUE)
    assertEquals(response.pushIntervalMs(), cmClient.getPushIntervalMs)
    assertEquals(response.subscriptionId(), cmClient.getSubscriptionId)

    assertEquals(response.requestedMetrics().size, cmClient.getMetrics.size)
    response.requestedMetrics().forEach(x => assertTrue(cmClient.getMetrics.contains(x)))

    assertEquals(response.acceptedCompressionTypes().size(), ClientMetricsManager.getSupportedCompressionTypes.size)

    response.acceptedCompressionTypes().forEach(x => {
      assertTrue(ClientMetricsManager.getSupportedCompressionTypes.contains(x))
      assertTrue(CompressionType.values().contains(CompressionType.forId(x.toInt)))
    })
  }

  @Test def testRequestAndResponseWithNoMatchingMetrics(): Unit = {
    val subscription1 = createCMSubscription("cm_2")
    assertNotNull(subscription1)

    // Create a python client that do not have any matching subscriptions.
    val clientInfo = CmClientInformation("testClient1", "clientId3", "Python", "11.1.0", "192.168.1.7", "9093")
    var response = sendGetSubscriptionRequest(clientInfo).data()
    var cmClient = getClientInstance(response.clientInstanceId())
    val clientInstanceId = response.clientInstanceId()

    // Push interval must be set to the default push interval and requested metrics list should be empty
    assertEquals(response.pushIntervalMs(), ClientMetrics.DEFAULT_PUSH_INTERVAL)
    assertNotEquals(response.pushIntervalMs(), subscription1.getPushIntervalMs)
    assertEquals(response.subscriptionId(), cmClient.getSubscriptionId)
    assertTrue(response.requestedMetrics().isEmpty)

    // Now create a client subscription with client id that matches with the client.
    val props = new Properties()
    val clientMatch = List(s"${CLIENT_SOFTWARE_NAME}=Python", s"${CLIENT_SOFTWARE_VERSION}=11.1.*")
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatch.mkString(","))
    val subscription2 = createCMSubscription("cm_2", props)
    assertNotNull(subscription2)

    // should have got the positive response with all the valid parameters
    response = sendGetSubscriptionRequest(clientInfo, clientInstanceId).data()
    cmClient = getClientInstance(clientInstanceId)
    assertEquals(response.pushIntervalMs(), subscription2.getPushIntervalMs)
    assertEquals(response.subscriptionId(), cmClient.getSubscriptionId)
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
    assertNotNull(subscription1)

    val response = sendGetSubscriptionRequest(clientInfo).data()

    // verify all the parameters ..
    assertNotEquals(response.clientInstanceId(), Uuid.ZERO_UUID)
    val cmClient = getClientInstance(response.clientInstanceId())
    assertNotNull(cmClient)
    assertEquals(response.pushIntervalMs(), cmClient.getPushIntervalMs)
    assertEquals(response.subscriptionId(), cmClient.getSubscriptionId)
    assertEquals(response.requestedMetrics().size(), 1)
    assertTrue(response.requestedMetrics().get(0).isEmpty)
  }

  @Test def testRequestWithDisabledClient(): Unit = {
    val clientInfo = CmClientInformation("testClient5", "clientId5", "Java", "11.1.0", "192.168.1.7", "9093")

    val subscription1 = createCMSubscription("cm_4")
    assertNotNull(subscription1)

    // Submit a request to get the subscribed metrics
    var response = sendGetSubscriptionRequest(clientInfo).data()
    val clientInstanceId = response.clientInstanceId()
    var cmClient = getClientInstance(clientInstanceId)
    assertNotNull(cmClient)

    val oldSubscriptionId = response.subscriptionId()
    assertEquals(response.pushIntervalMs(), subscription1.getPushIntervalMs)
    assertEquals(response.subscriptionId(), cmClient.getSubscriptionId)
    response.requestedMetrics().forEach(x => assertTrue(subscription1.getSubscribedMetrics.contains(x)))

    // Now create a new client subscription with push interval set to 0.
    val props = new Properties()
    props.put(ClientMetrics.PushIntervalMs, 0.toString)
    val subscription2 = createCMSubscription("cm_update_2_disable", props)
    assertNotNull(subscription2)

    // should have got the invalid response with empty metrics list.
    // set the client instance id which is obtained in earlier request.
    val res = sendGetSubscriptionRequest(clientInfo, clientInstanceId)
    cmClient = getClientInstance(clientInstanceId)
    assertNotNull(cmClient)

    response = res.data()
    assertEquals(response.pushIntervalMs(), 0)
    assertTrue(response.requestedMetrics().isEmpty)
    assertNotEquals(oldSubscriptionId, response.subscriptionId())
    assertEquals(response.subscriptionId(), cmClient.getSubscriptionId)
  }

  @Test
  def testPushMetricsRequestParameters(): Unit = {
    val props = new Properties()
    val pushInterval = 10
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, pushInterval.toString)
    val subscription = createCMSubscription("cm_1", props)
    assertNotNull(subscription)

    val clientInfo = CmClientInformation("testClient1", "clientId1", "Java", "11.1.0", "192.168.1.7", "9093")
    val response = sendGetSubscriptionRequest(clientInfo).data()
    assertNotNull(response)
    val cmClient = getClientInstance(response.clientInstanceId())
    assertNotNull(cmClient)

    val data = new PushTelemetryRequestData()

    // Test-1: send the request with no clientInstanceId.
    var pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertNotNull(pushResponse)
    assertEquals(pushResponse.error().code(), Errors.INVALID_REQUEST.code())

    // Test-2: send the request with no subscription Id
    data.setClientInstanceId(response.clientInstanceId())
    pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertNotNull(pushResponse)
    assertEquals(pushResponse.error().code(), Errors.UNKNOWN_CLIENT_METRICS_SUBSCRIPTION_ID.code())

    // Test-3: send the request with incorrect subscription Id
    data.setClientInstanceId(response.clientInstanceId())
    data.setSubscriptionId(response.subscriptionId() ^ 0x1234)
    pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertNotNull(pushResponse)
    assertEquals(pushResponse.error().code(), Errors.UNKNOWN_CLIENT_METRICS_SUBSCRIPTION_ID.code())

    // Test-3: send the request with incorrect compression type
    data.setClientInstanceId(response.clientInstanceId())
    data.setSubscriptionId(response.subscriptionId())
    data.setCompressionType(0x30)
    pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertNotNull(pushResponse)
    assertEquals(pushResponse.error().code(), Errors.UNSUPPORTED_COMPRESSION_TYPE.code())

    // TEST-4 Update the subscription information and send the request.
    // Since we have updated the subscription information, it would also change the SubscriptionId,
    // first try to send the request without updating the new subscription id and it should fail.
    // send the second request with new subscription id and then id should pass.
    createCMSubscription("cm_1", props)
    val response2 = sendGetSubscriptionRequest(clientInfo).data()

    data.setClientInstanceId(response2.clientInstanceId())
    data.setCompressionType(CompressionType.NONE.id.toByte)
    pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertEquals(pushResponse.error().code(), Errors.UNKNOWN_CLIENT_METRICS_SUBSCRIPTION_ID.code())

    // Update the subscription id with the new id from the response2
    data.setSubscriptionId(response2.subscriptionId())
    pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertEquals(pushResponse.error().code(), Errors.NONE.code())
    assertEquals(pushResponse.data().throttleTimeMs(), 5000)
  }

  @Test
  def testPushMetricsRateLimiting(): Unit = {
    val props = new Properties()
    var pushInterval = 60 * 1000
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, pushInterval.toString)
    val subscription = createCMSubscription("cm_1", props)
    assertNotNull(subscription)

    val clientInfo = CmClientInformation("testClient1", "clientId1", "Java", "11.1.0", "192.168.1.7", "9093")
    val response = sendGetSubscriptionRequest(clientInfo).data()
    assertNotNull(response)

    val data = new PushTelemetryRequestData()
    data.setClientInstanceId(response.clientInstanceId())
    data.setSubscriptionId(response.subscriptionId())
    data.setCompressionType(CompressionType.GZIP.id.toByte)

    // Test-1: send the request before the next push interval
    var pushResponse = sendPushTelemetryRequest(data, clientInfo)
    assertNotNull(pushResponse)
    assertEquals(pushResponse.error().code(), Errors.CLIENT_METRICS_RATE_LIMITED.code())

    // Test-2: set the Terminating flag, this time push request should have been accepted
    // even though request is sent before the next push interval
    data.setTerminating(true)
    pushResponse = sendPushTelemetryRequest(data, clientInfo)
    assertNotNull(pushResponse)
    assertEquals(pushResponse.error().code(), Errors.NONE.code())

    // Update the push interval to 10 ms also update the new subscription id and carry
    // forward the terminating flag in the new client instance object
    pushInterval = 10
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, pushInterval.toString)
    createCMSubscription("cm_1", props)
    val response2 = sendGetSubscriptionRequest(clientInfo).data()
    data.setClientInstanceId(response2.clientInstanceId())
    data.setSubscriptionId(response2.subscriptionId())
    getClientInstance(response2.clientInstanceId()).setTerminatingFlag(true)

    // Test-3: send the second push request with terminating flag set. This time request should
    // have been rejected as only one request should be accepted when client is terminating
    pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertNotNull(pushResponse)
    assertEquals(pushResponse.error().code(), Errors.INVALID_REQUEST.code())

    // Test-4: Set the terminating flag to false, even then client request should be rejected
    data.setTerminating(false)
    pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertNotNull(pushResponse)
    assertEquals(pushResponse.error().code(), Errors.INVALID_REQUEST.code())
  }

  @Test
  def testExportMetrics(): Unit = {
    val plugin = setupClientMetricsPlugin()
    val props = new Properties()
    val pushInterval = 10
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, pushInterval.toString)
    val subscription = createCMSubscription("cm_2", props)
    assertNotNull(subscription)

    val clientInfo = CmClientInformation("testClient1", "clientId1", "Java", "11.1.0", "192.168.1.7", "9093")
    val response = sendGetSubscriptionRequest(clientInfo).data()
    assertNotNull(response)

    val data = new PushTelemetryRequestData()
    data.setClientInstanceId(response.clientInstanceId())
    data.setSubscriptionId(response.subscriptionId())
    data.setCompressionType(CompressionType.NONE.id.toByte)

    // Test-1: Send the push metrics request with empty metrics data, in response to that broker
    // should not have invoked the metrics plugin's exportMetrics method.
    var pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertEquals(pushResponse.error().code(), Errors.NONE.code())
    assertEquals(plugin.exportMetricsInvoked, 0)
    assertNull(plugin.metricsData)

    // Test-2: Add the metrics data and send the request again, this time exportMetrics
    // call out should have been invoked.
    val metricsData = "org.apache.kafka/client.producer.partition.queue.size=1234"
    data.setMetrics(metricsData.getBytes)
    pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
    assertEquals(pushResponse.error().code(), Errors.NONE.code())
    assertEquals(plugin.exportMetricsInvoked, 1)
    assertNotNull(plugin.metricsData)

    // Test-3 : Make sure that metrics data passed to the export metrics is read only buffer.
    //  - Accessing the raw bytebuffer's array should throw ReadOnlyBufferException
    //  - No assert when accessing the bytebuffer after the deep copy.
    assertThrows(classOf[ReadOnlyBufferException], () => plugin.metricsData.array())
    assertNotNull(plugin.getMetricsDataAsCopy().array())
  }

  @Test
  def testPushMetricsWithCompressionTypes(): Unit = {
    val plugin = setupClientMetricsPlugin()
    val props = new Properties()
    val pushInterval = 5
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, pushInterval.toString)
    val subscription = createCMSubscription("cm_5", props)
    assertNotNull(subscription)

    val clientInfo = CmClientInformation("testClient1", "clientId1", "Java", "11.1.0", "192.168.1.7", "9093")
    val response = sendGetSubscriptionRequest(clientInfo).data()
    assertNotNull(response)

    val data = new PushTelemetryRequestData()
    val metricsMap = Map("metric1" -> 1, "metric2" -> 2)
    data.setClientInstanceId(response.clientInstanceId())
    data.setSubscriptionId(response.subscriptionId())
    var count = 1
    val values = List(CompressionType.GZIP, CompressionType.LZ4)
    values.foreach(x => {
      data.setCompressionType(x.id.toByte)
      val (compressedData, metricStr) = getSerializedMetricsData(x, metricsMap)
      data.setMetrics(compressedData.array())
      val pushResponse = sendPushTelemetryRequest(data, clientInfo, pushInterval)
      assertEquals(pushResponse.error().code(), Errors.NONE.code())
      assertEquals(plugin.exportMetricsInvoked, count)
      val s1 = new String(Utils.readBytes(plugin.getMetricsDataAsCopy())).trim
      assertEquals(s1, metricStr)
      count += 1
    })
  }

}
