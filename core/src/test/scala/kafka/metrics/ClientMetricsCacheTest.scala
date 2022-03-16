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

import kafka.metrics.ClientMetricsTestUtils.{createCMSubscription, createClientInstance, defaultMetrics, defaultPushInterval, getClientInstance}
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMatchingParams.{CLIENT_SOFTWARE_NAME, CLIENT_SOFTWARE_VERSION, CLIENT_SOURCE_ADDRESS}
import kafka.metrics.clientmetrics.{ClientMetricsCache, ClientMetricsConfig, CmClientInformation}
import kafka.utils.TestUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

import java.util.Properties

class ClientMetricsCacheTest {
  @AfterEach
  def cleanup(): Unit = {
    ClientMetricsConfig.clearClientSubscriptions()
    ClientMetricsCache.getInstance.clear()
  }

  @Test
  def testClientMetricsSubscription(): Unit = {
    // create a client metric subscription.
    val subscription1 = createCMSubscription("cm_1")
    assertNotNull(subscription1)

    // create a client instance state object and make sure it picks up the metrics from the previously created
    // metrics subscription.
    val client = CmClientInformation("testClient1", "clientId", "Java", "11.1.0.1", "", "")
    val clientState = createClientInstance(client)
    val clientStateFromCache = getClientInstance(clientState.getId)
    assertEquals(clientState, clientStateFromCache)
    assertEquals(clientStateFromCache.getSubscriptions.size(), 1)
    assertEquals(clientStateFromCache.getPushIntervalMs, defaultPushInterval)
    assertEquals(clientStateFromCache.metrics.size, 2)
    assertTrue(clientStateFromCache.getMetrics.mkString(",").equals(defaultMetrics))
  }

  @Test
  def testAddingSubscriptionsAfterClients(): Unit = {
    // create a client instance state object  when there are no client metrics subscriptions exists
    val client = CmClientInformation("testClient1", "clientId", "Java", "11.1.0.1", "", "")
    val clientState = createClientInstance(client)
    var clientStateFromCache = getClientInstance(clientState.getId)
    assertEquals(clientState, clientStateFromCache)
    assertTrue(clientStateFromCache.getSubscriptions.isEmpty)
    assertTrue(clientStateFromCache.getMetrics.isEmpty)
    val oldSubscriptionId = clientStateFromCache.getSubscriptionId

    // Now create a new client subscription and make sure the client instance is updated with the metrics.
    createCMSubscription("cm_1")
    clientStateFromCache = getClientInstance(clientState.getId)
    assertEquals(clientStateFromCache.getSubscriptions.size(), 1)
    assertEquals(clientStateFromCache.getPushIntervalMs, defaultPushInterval)
    assertNotEquals(clientStateFromCache.getSubscriptionId, oldSubscriptionId)
    assertEquals(clientStateFromCache.metrics.size, 2)
    assertTrue(clientStateFromCache.getMetrics.mkString(",").equals(defaultMetrics))
  }

  @Test
  def testAddingMultipleSubscriptions(): Unit = {
    val props = new Properties()
    val clientMatchPatterns = List(s"${CLIENT_SOFTWARE_NAME}=Java", s"${CLIENT_SOFTWARE_VERSION}=8.1.*")
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchPatterns.mkString(","))

    // TEST-1: CREATE new metric subscriptions and make sure client instance picks up those metrics.
    val subscription1 = createCMSubscription("cm_1")
    val subscription2 = createCMSubscription("cm_2", props)
    assertNotNull(subscription1)
    assertNotNull(subscription2)

    // create a client instance state object and make sure every thing is in correct order.
    val client = CmClientInformation("testClient1", "clientId", "Java", "11.1.0.1", "", "")
    val clientState = createClientInstance(client)
    val clientStateFromCache = getClientInstance(clientState.getId)
    assertEquals(clientState, clientStateFromCache)

    val res = clientState.getSubscriptions
    assertEquals(res.size(), 1)
    assertTrue(res.contains(subscription1))
    assertEquals(clientState.getPushIntervalMs, defaultPushInterval)
    assertEquals(clientState.metrics.size, 2)
    assertTrue(clientState.metrics.mkString(",").equals(defaultMetrics))

    // TEST-2: UPDATE the metrics subscription: Create update the metrics subscriptions by adding new
    // subscription with different metrics and make sure that client instance object is updated
    // with the new metric and new subscription id.
    val metrics3 = "org.apache.kafka/client.producer.write.latency"
    val props3 = new Properties()
    props3.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics3)
    createCMSubscription("cm_3", props3)
    val afterAddingNewSubscription = getClientInstance(clientState.getId)
    assertEquals(clientStateFromCache.getId, afterAddingNewSubscription.getId)
    assertNotEquals(clientState.getSubscriptionId, afterAddingNewSubscription.getSubscriptionId)
    assertEquals(afterAddingNewSubscription.metrics.size, 3)
    assertTrue(afterAddingNewSubscription.metrics.mkString(",").equals(defaultMetrics + "," + metrics3))

    // TEST-3: UPDATE the first subscription's metrics and make sure
    // client instance picked up the change.
    val updated_metrics = "updated_metrics_for_clients"
    val updatedProps = new Properties()
    updatedProps.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, updated_metrics)
    createCMSubscription("cm_1", updatedProps)
    val afterSecondUpdate = getClientInstance(clientState.getId)
    assertEquals(afterSecondUpdate.getId, afterAddingNewSubscription.getId)
    assertNotEquals(afterSecondUpdate.getSubscriptionId, afterAddingNewSubscription.getSubscriptionId)
    assertEquals(afterSecondUpdate.metrics.size, 2)
    assertTrue(afterSecondUpdate.metrics.mkString(",").equals(metrics3 + "," + updated_metrics))

    // TEST3: DELETE the metrics subscription: Delete the first subscription and make sure
    // client instance is updated
    val props4 = new Properties()
    props4.put(ClientMetricsConfig.ClientMetrics.DeleteSubscription, "true")
    createCMSubscription("cm_1", props4)

    // subscription should have been deleted.
    assertNull(ClientMetricsConfig.getClientSubscriptionInfo("cm_1"))

    val afterDeleting = getClientInstance(clientState.getId)
    assertEquals(afterAddingNewSubscription.getId, afterDeleting.getId)
    assertNotEquals(afterAddingNewSubscription.getSubscriptionId, afterDeleting.getSubscriptionId)
    assertEquals(afterAddingNewSubscription.getSubscriptions.size() - afterDeleting.getSubscriptions.size(), 1)
    assertEquals(afterDeleting.metrics.size, 1)
    assertTrue(afterDeleting.metrics.mkString(",").equals(metrics3))
  }

  @Test
  def testMultipleSubscriptionsAndClients(): Unit = {
    createCMSubscription("cm_1")

    val metrics2 = "org.apache.kafka/client.producer.write.latency"
    val props2 = new Properties()
    props2.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics2)
    createCMSubscription("cm_2", props2)

    val props3 = new Properties()
    val clientPatterns3 = List(s"${CLIENT_SOFTWARE_NAME}=Python", s"${CLIENT_SOFTWARE_VERSION}=8.*")
    val metrics3 = "org.apache.kafka/client.consumer.read.latency"
    props3.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics3)
    props3.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientPatterns3.mkString(","))
    createCMSubscription("cm_3", props3)

    val props4 = new Properties()
    val clientPatterns4 = List(s"${CLIENT_SOFTWARE_NAME}=Python",
                               s"${CLIENT_SOFTWARE_VERSION}=8.*",
                               s"${CLIENT_SOURCE_ADDRESS} = 1.2.3.4")
    val metrics4 = "org.apache.kafka/client.consumer.*.latency"
    props4.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientPatterns4.mkString(","))
    props4.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics4)
    createCMSubscription("cm_4", props4)
    assertEquals(ClientMetricsConfig.getSubscriptionsCount, 4)

    val client1 = createClientInstance(CmClientInformation("testClient1", "Id1", "Java", "11.1.0.1", "", ""))
    val client2 = createClientInstance(CmClientInformation("testClient2", "Id2", "Python", "8.2.1", "abcd", "0"))
    val client3 = createClientInstance(CmClientInformation("testClient3", "Id3", "C++", "12.1", "192.168.1.7", "9093"))
    val client4 = createClientInstance(CmClientInformation("testClient4", "Id4", "Java", "11.1", "1.2.3.4", "8080"))
    val client5 = createClientInstance(CmClientInformation("testClient2", "Id5", "Python", "8.2.1", "1.2.3.4", "0"))
    assertEquals(ClientMetricsCache.getInstance.getSize, 5)

    // Verifications:
    // Client 1 should have the metrics from the subscription1 and subscription2
    assertTrue(client1.getMetrics.mkString(",").equals(defaultMetrics + "," + metrics2))

    // Client 2 should have the subscription3 which is just default metrics
    assertTrue(client2.getMetrics.mkString(",").equals(metrics3))

    // client 3 should end up with nothing.
    assertTrue(client3.getMetrics.isEmpty)

    // Client 4 should have the metrics from subscription1 and subscription2
    assertTrue(client4.getMetrics.mkString(",").equals(defaultMetrics + "," + metrics2))

    // Client 5 should have the metrics from subscription-3 and subscription-4
    assertTrue(client5.getMetrics.mkString(",").equals(metrics3 + "," + metrics4))
  }

  @Test
  def testMultipleClientsAndSubscriptions(): Unit = {
    // Create the Client instances first
    val client1 = createClientInstance(CmClientInformation("t1", "c1", "Java", "11.1.0.1", "", "")).getId
    val client2 = createClientInstance(CmClientInformation("t2", "c2", "Python", "8.2.1", "abcd", "0")).getId
    val client3 = createClientInstance(CmClientInformation("t3", "c3", "C++", "12.1", "192.168.1.7", "9093")).getId
    val client4 = createClientInstance(CmClientInformation("t4", "c4", "Java", "11.1", "1.2.3.4", "8080")).getId
    val client5 = createClientInstance(CmClientInformation("t5", "c5", "Python", "8.2.1", "1.2.3.4", "0")).getId
    assertEquals(ClientMetricsCache.getInstance.getSize, 5)

    // Now create the subscriptions.
    createCMSubscription("cm_1")

    val metrics2 = "org.apache.kafka/client.producer.write.latency"
    val props2 = new Properties()
    props2.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics2)
    createCMSubscription("cm_2", props2)

    val props3 = new Properties()
    val clientPatterns3 = List(s"${CLIENT_SOFTWARE_NAME}=Python", s"${CLIENT_SOFTWARE_VERSION}=8.*")
    val metrics3 = "org.apache.kafka/client.consumer.read.latency"
    props3.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics3)
    props3.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientPatterns3.mkString(","))
    createCMSubscription("cm_3", props3)

    val props4 = new Properties()
    val clientPatterns4 = List(s"${CLIENT_SOFTWARE_NAME}=Python",
                               s"${CLIENT_SOFTWARE_VERSION}=8.*",
                               s"${CLIENT_SOURCE_ADDRESS} = 1.2.3.4")
    val metrics4 = "org.apache.kafka/client.consumer.*.latency"
    props4.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientPatterns4.mkString(","))
    props4.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics4)
    createCMSubscription("cm_4", props4)
    assertEquals(ClientMetricsConfig.getSubscriptionsCount, 4)

    // Verifications:
    // Client 1 should have the metrics from subscription1 and subscription2
    assertTrue(getClientInstance(client1).getMetrics.mkString(",").equals(defaultMetrics + "," + metrics2))

    // Client 2 should have the subscription3 which is just default metrics
    assertTrue(getClientInstance(client2).getMetrics.mkString(",").equals(metrics3))

    // client 3 should end up with nothing.
    assertTrue(getClientInstance(client3).getMetrics.isEmpty)

    // Client 4 should have the metrics from subscription1 and subscription2
    assertTrue(getClientInstance(client4).getMetrics.mkString(",").equals(defaultMetrics + "," + metrics2))

    // Client 5 should have the metrics from subscription-3 and subscription-4
    assertTrue(getClientInstance(client5).getMetrics.mkString(",").equals(metrics3 + "," + metrics4))
  }


  @Test
  def testCleanupTtlEntries(): Unit = {
    val cache = ClientMetricsCache.getInstance
    val client1 = createClientInstance(CmClientInformation("testClient1", "clientId1", "Java", "11.1.0.1", "", ""))
    val client2 = createClientInstance(CmClientInformation("testClient2", "clientId2", "Python", "8.2.1", "", ""))
    val client3 = createClientInstance(CmClientInformation("testClient3", "clientId3", "C++", "12.1", "", ""))
    assertEquals(cache.getSize, 3)

    // Modify client3's timestamp to meet the TTL expiry limit.
    val ts = client3.getLastAccessTs - (Math.max(3 * client3.getPushIntervalMs, ClientMetricsCache.getTtlTs) + 10)
    client3.updateLastAccessTs(ts)
    ClientMetricsCache.setLastCleanupTs(ClientMetricsCache.getLastCleanupTs - (ClientMetricsCache.getCleanupInterval + 10))

    // Run the GC and wait until client3 entry is removed from the cache
    ClientMetricsCache.deleteExpiredEntries(true)
    TestUtils.waitUntilTrue(() => ClientMetricsCache.getInstance.getSize == 2, "Failed to run GC on Client Metrics Cache", 6000)

    // Make sure that client3 is removed from the cache.
    assertTrue(cache.get(client3.getId).isEmpty)

    // client1 and client2 should remain in the cache.
    assertTrue(!cache.get(client1.getId).isEmpty)
    assertTrue(!cache.get(client2.getId).isEmpty)
  }
}
