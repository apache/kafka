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

import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics.{AllMetricsFlag, ClientMatchPattern, DeleteSubscription, PushIntervalMs, SubscriptionMetrics, configDef}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.{BOOLEAN, INT, LIST}
import org.apache.kafka.common.errors.InvalidRequestException

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer

/**
 * Client metric configuration related parameters and the supporting methods like validation and update methods
 * are defined in this class.
 *
 * SubscriptionInfo: Contains the client metric subscription information. Supported operations from the CLI are
 * add/delete/update operations. Every subscription object contains the following parameters that are populated
 * during the creation of the subscription through kafka-client-metrics.sh
 * {
 *   subscriptionId: Name/ID supplied by CLI during the creation of the client metric subscription.
 *   subscribedMetrics: List of metric prefixes
 *   pushIntervalMs: A positive integer value >=0  tells the client that how often a client can push the metrics
 *   matchingPatternsList: List of client matching patterns, that are used by broker to match the client instance
 *   with the subscription.
 * }
 *
 * At present, CLI can pass the following parameters in AlterConfigRequest to add/delete/update the client metric
 * subscription:
 *
 *  1. "client.metrics.subscription.metrics" value should be comma separated metrics list.
 *      Ex: "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
 *      OR "client.metrics.flag" must be set to 'true'
 *
 *  2. "client.metrics.push.interval.ms" should be greater than or equal to 0; a value 0 is considered as an indication
 *  to disable the metric collection from the client.
 *
 *  3. "client.metrics.subscription.client.match" is a comma separated list of client match patterns, in case if there
 *     is no matching pattern specified then broker considers that as all match which means the associated metrics
 *     applies to all the clients. Ex: "client_software_name = Java, client_software_version = 11.1.*"
 *     For exact parameters that a client can pass please look at the ClientMatchingParams
 *
 *  4. "client.metrics.delete.subscription" should be set to true to delete an existing client subscription. if it is
 *      set then there is no need for the other parameters
 *
 * For more information please look at kip-714:
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability
 */
object ClientMetricsConfig {

  class SubscriptionInfo(subscriptionId: String,
                         subscribedMetrics: List[String],
                         var matchingPatternsList: List[String],
                         pushIntervalMs: Int,
                         allMetricsSubscribed: Boolean = false) {
    def getId = subscriptionId
    def getPushIntervalMs = pushIntervalMs
    val clientMatchingPatterns = CmClientInformation.parseMatchingPatterns(matchingPatternsList)
    def getClientMatchingPatterns = clientMatchingPatterns
    def getSubscribedMetrics = subscribedMetrics
    def getAllMetricsSubscribed = allMetricsSubscribed
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
    // Properties
    val SubscriptionMetrics = "client.metrics.subscription.metrics"
    val ClientMatchPattern = "client.metrics.subscription.client.match"
    val PushIntervalMs = "client.metrics.push.interval.ms"
    val AllMetricsFlag = "client.metrics.all"
    val DeleteSubscription = "client.metrics.delete.subscription"

    val DEFAULT_PUSH_INTERVAL = 5 * 60 * 1000 // 5 minutes

    // Definitions
    val configDef = new ConfigDef()
      .define(SubscriptionMetrics, LIST, MEDIUM, "List of the subscribed metrics")
      .define(ClientMatchPattern, LIST, MEDIUM, "Pattern used to find the matching clients")
      .define(PushIntervalMs, INT, DEFAULT_PUSH_INTERVAL, MEDIUM, "Interval that a client can push the metrics")
      .define(AllMetricsFlag, BOOLEAN, false, MEDIUM, "If set to true all the metrics are included")
      .define(DeleteSubscription, BOOLEAN, false, MEDIUM, "If set to true metric subscription would be deleted")

    def names = configDef.names

    def validate(subscriptionId :String, properties :Properties): Unit = {
      if (subscriptionId.isEmpty) {
        throw new InvalidRequestException("subscriptionId can't be empty")
      }
      validateProperties(properties)
    }

    def validateProperties(properties :Properties) = {
      val names = configDef.names
      properties.keySet().forEach(x => require(names.contains(x), s"Unknown client metric configuration: $x"))

      // If the command is to delete the subscription then we do not expect any other parameters to be in the list.
      // Otherwise validate the rest of the parameters.
      if (!properties.containsKey(DeleteSubscription)) {
        require(properties.containsKey(PushIntervalMs), s"Missing parameter ${PushIntervalMs}")
        require(Integer.parseInt(properties.get(PushIntervalMs).toString) >= 0, s"Invalid parameter ${PushIntervalMs}")

        // If all metrics flag is specified then there is no need for having the metrics parameter
        if (!properties.containsKey(AllMetricsFlag)) {
          require(properties.containsKey(SubscriptionMetrics), s"Missing parameter ${SubscriptionMetrics}")
        }

        // Make sure that client match patterns are valid by parsing them.
        if (properties.containsKey(ClientMatchPattern)) {
          val propsList: List[String] = properties.getProperty(ClientMatchPattern).split(",").toList
          CmClientInformation.parseMatchingPatterns(propsList)
        }
      }
    }
  }

  private val subscriptionMap = new ConcurrentHashMap[String, SubscriptionInfo]

  def getClientSubscriptionInfo(subscriptionId :String): SubscriptionInfo  =  subscriptionMap.get(subscriptionId)
  def clearClientSubscriptions() = subscriptionMap.clear
  def getSubscriptionsCount = subscriptionMap.size
  def getClientSubscriptions = subscriptionMap.values

  private def toList(prop: Any): List[String] = {
    val value: util.List[_] = prop.asInstanceOf[util.List[_]]
    val valueList =  new ListBuffer[String]()
    value.forEach(x => valueList += x.asInstanceOf[String])
    valueList.toList
  }

  def updateClientSubscription(subscriptionId :String, properties: Properties): Unit = {
    val parsed = configDef.parse(properties)
    val javaFalse = java.lang.Boolean.FALSE
    val subscriptionDeleted = parsed.getOrDefault(DeleteSubscription, javaFalse).asInstanceOf[Boolean]
    if (subscriptionDeleted) {
      val deletedSubscription = subscriptionMap.remove(subscriptionId)
      ClientMetricsCache.getInstance.invalidate(deletedSubscription, null)
    } else {
      val clientMatchPattern = toList(parsed.get(ClientMatchPattern))
      val pushInterval = parsed.get(PushIntervalMs).asInstanceOf[Int]
      val allMetricsSubscribed = parsed.getOrDefault(AllMetricsFlag, javaFalse).asInstanceOf[Boolean]
      val metrics = if (allMetricsSubscribed) List("") else toList(parsed.get(SubscriptionMetrics))
      val newSubscription =
        new SubscriptionInfo(subscriptionId, metrics, clientMatchPattern, pushInterval, allMetricsSubscribed)
      val oldSubscription = subscriptionMap.put(subscriptionId, newSubscription)
      ClientMetricsCache.getInstance.invalidate(oldSubscription, newSubscription)
    }
  }

  def validateConfig(subscriptionId :String, configs: Properties): Unit =  {
    ClientMetrics.validate(subscriptionId, configs)
  }
}
