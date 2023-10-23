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
package kafka.metrics.clientmetrics

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.{INT, LIST}
import org.apache.kafka.common.errors.InvalidRequestException

import java.util.Properties

/**
 * Client metric configuration related parameters and the supporting methods like validation and update methods
 * are defined in this class.
 * <p>
 * SubscriptionInfo: Contains the client metric subscription information. Supported operations from the CLI are
 * add/delete/update operations. Every subscription object contains the following parameters that are populated
 * during the creation of the subscription.
 * <p>
 * {
 * <ul>
 *   <li> subscriptionId: Name/ID supplied by CLI during the creation of the client metric subscription.
 *   <li> subscribedMetrics: List of metric prefixes
 *   <li> pushIntervalMs: A positive integer value >=0  tells the client that how often a client can push the metrics
 *   <li> matchingPatternsList: List of client matching patterns, that are used by broker to match the client instance
 *   with the subscription.
 * </ul>
 * }
 * <p>
 * At present, CLI can pass the following parameters in request to add/delete/update the client metrics
 * subscription:
 * <ul>
 *  <li> "metrics" value should be comma separated metrics list. An empty list means no metrics subscribed.
 *      A list containing just an empty string means all metrics subscribed.
 *      Ex: "org.apache.kafka.producer.partition.queue.,org.apache.kafka.producer.partition.latency"
 *  <li> "interval.ms" should be between 100 and 3600000 (1 hour). This is the interval at which the client
 *      should push the metrics to the broker.
 *  <li> "match" is a comma separated list of client match patterns, in case if there is no matching
 *      pattern specified then broker considers that as all match which means the associated metrics
 *      applies to all the clients. Ex: "client_software_name = Java, client_software_version = 11.1.*"
 * </ul>
 * For more information please look at kip-714:
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Clientmetricsconfiguration
 */
object ClientMetricsConfig {

  class SubscriptionInfo(subscriptionId: String,
                         subscribedMetrics: List[String],
                         pushIntervalMs: Int,
                         var matchingPatternsList: List[String]) {
    def getId: String = subscriptionId
    def getPushIntervalMs: Int = pushIntervalMs
    def getClientMatchingPatterns: Map[String, String] = ClientMetricsMetadata.parseMatchingPatterns(matchingPatternsList)
    def getSubscribedMetrics: List[String] = subscribedMetrics
  }

  object ClientMatchingParams {
    val CLIENT_ID = "client_id"
    val CLIENT_INSTANCE_ID = "client_instance_id"
    val CLIENT_SOFTWARE_NAME = "client_software_name"
    val CLIENT_SOFTWARE_VERSION = "client_software_version"
    val CLIENT_SOURCE_ADDRESS = "client_source_address"
    val CLIENT_SOURCE_PORT = "client_source_port"

    val matchersList = List(CLIENT_ID, CLIENT_INSTANCE_ID, CLIENT_SOFTWARE_NAME,
                            CLIENT_SOFTWARE_VERSION, CLIENT_SOURCE_ADDRESS, CLIENT_SOURCE_PORT)

    def isValidParam(param: String) = matchersList.contains(param)
  }

  object ClientMetrics {
    // Properties that are used to create the subscription for client_metrics.
    val SubscriptionMetrics = "metrics"
    val PushIntervalMs = "interval.ms"
    val ClientMatchPattern = "match"

    val DEFAULT_PUSH_INTERVAL = 30 * 1000 // 5 minutes

    // Definitions of accepted values
    val configDef = new ConfigDef()
      .define(SubscriptionMetrics, LIST, "", MEDIUM, "List of the subscribed metrics")
      .define(ClientMatchPattern, LIST, "", MEDIUM, "Pattern used to find the matching clients")
      .define(PushIntervalMs, INT, DEFAULT_PUSH_INTERVAL, MEDIUM, "Interval that a client can push the metrics")

    def names = configDef.names

    def validate(subscriptionId :String, properties :Properties): Unit = {
      if (subscriptionId.isEmpty) {
        throw new InvalidRequestException("subscriptionId can't be empty")
      }
      validateProperties(properties)
    }

    def validateProperties(properties: Properties) = {
      // Make sure that all the properties are valid.
      properties.keySet().forEach(x =>
        if (!names.contains(x))
          throw new InvalidRequestException(s"Unknown client metric configuration: $x")
      )

      // Make sure that push interval is between 100ms and 1 hour.
      if (properties.containsKey(PushIntervalMs)) {
        val pushIntervalMs = Integer.parseInt(properties.getProperty(PushIntervalMs))
        if (pushIntervalMs < 100 || pushIntervalMs > 3600000)
          throw new InvalidRequestException(s"Invalid parameter ${PushIntervalMs}")
      }

      // Make sure that client match patterns are valid by parsing them.
      if (properties.containsKey(ClientMatchPattern)) {
        val propsList: List[String] = properties.getProperty(ClientMatchPattern).split(",").toList
        ClientMetricsMetadata.parseMatchingPatterns(propsList)
      }
    }
  }

  def updateClientSubscription(subscriptionId :String, properties: Properties): Unit = {
    // TODO: Implement the update logic.
  }

  def validateConfig(subscriptionId :String, configs: Properties): Unit =  {
    ClientMetrics.validate(subscriptionId, configs)
  }
}
