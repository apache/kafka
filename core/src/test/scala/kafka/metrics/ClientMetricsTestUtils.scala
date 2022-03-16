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

import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMatchingParams.{CLIENT_SOFTWARE_NAME, CLIENT_SOFTWARE_VERSION}
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionInfo
import kafka.metrics.clientmetrics.{ClientMetricsConfig, CmClientInformation, CmClientInstanceState}
import kafka.server.ClientMetricsManager
import org.apache.kafka.common.Uuid

import java.util.Properties

object ClientMetricsTestUtils {
  val defaultPushInterval = 30 * 1000 // 30 seconds

  val defaultMetrics =
    "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"

  val defaultClientMatchPatters =
    List(s"${CLIENT_SOFTWARE_NAME}=Java", s"${CLIENT_SOFTWARE_VERSION}=11.1.*")

  val cmInstance :ClientMetricsManager = ClientMetricsManager.getInstance
  def getCM = cmInstance

  def updateClientSubscription(subscriptionId :String, properties: Properties): Unit = {
    getCM.updateSubscription(subscriptionId, properties)
  }

  def createClientInstance(selector: CmClientInformation): CmClientInstanceState = {
    getCM.createClientInstance(Uuid.randomUuid(), selector)
  }

  def getDefaultProperties() :Properties = {
    val props = new Properties()
    props.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, defaultMetrics)
    props.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, defaultClientMatchPatters.mkString(","))
    props.put(ClientMetricsConfig.ClientMetrics.PushIntervalMs, defaultPushInterval.toString)
    props
  }

  def createCMSubscription(subscriptionId: String, overrideProps: Properties = null): SubscriptionInfo = {
    val props = getDefaultProperties()
    if (overrideProps != null) {
      overrideProps.entrySet().forEach(x => props.put(x.getKey, x.getValue))
    }
    updateClientSubscription(subscriptionId, props)
    ClientMetricsConfig.getClientSubscriptionInfo(subscriptionId)
  }
}
