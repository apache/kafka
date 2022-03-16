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

import kafka.metrics.clientmetrics.ClientMetricsConfig
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

import java.util.Properties

class ClientMetricsConfigValidationTest {

  @Test
  def testClientMetricsConfigParameters(): Unit = {
    val groupName: String = "subscription-1"
    val metrics = "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
    val clientMatchingPattern = "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538"

    val props = new Properties()

    // Test-1: Missing parameters (add one after one until all the required params are added)
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))

    val interval = -1
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, interval.toString)
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))

    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, 2000.toString)
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))

    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics)
    ClientMetricsConfig.validateConfig(groupName, props)

    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, "client_software_name=*")
    assertThrows(classOf[InvalidConfigurationException], () => ClientMetricsConfig.validateConfig(groupName, props))

    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchingPattern)
    ClientMetricsConfig.validateConfig(groupName, props)

    props.put("INVALID_PARAMETER", "INVALID_ARGUMENT")
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))

    props.remove("INVALID_PARAMETER")
    ClientMetricsConfig.validateConfig(groupName, props)

    // TEST-2: Delete the metric subscription
    props.clear()
    props.put(ClientMetricsConfig.ClientMetrics.DeleteSubscription, "true")
    ClientMetricsConfig.validateConfig(groupName, props)

    // TEST-3: subscription with all metrics flag
    props.clear()
    props.put(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true")
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientMatchingPattern)
    assertThrows(classOf[IllegalArgumentException], () => ClientMetricsConfig.validateConfig(groupName, props))
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, 2000.toString)
    ClientMetricsConfig.validateConfig(groupName, props)
  }

}
