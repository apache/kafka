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

import kafka.Kafka.info
import kafka.metrics.clientmetrics.ClientMetricsConfig.ClientMetrics.DEFAULT_PUSH_INTERVAL
import kafka.metrics.clientmetrics.ClientMetricsConfig.SubscriptionInfo
import kafka.server.ClientMetricsManager.getCurrentTime
import org.apache.kafka.common.Uuid

import java.nio.charset.StandardCharsets
import java.util.zip.CRC32
import scala.collection.mutable.ListBuffer

/**
 * Client instance state that contains all the necessary information about the metric subscription for a client
 */
object CmClientInstanceState {

  def apply(instance: CmClientInstanceState,
            subscriptions: java.util.Collection[SubscriptionInfo]): CmClientInstanceState = {
    val newInstance = create(instance.getId, instance.getClientInfo, subscriptions)
    newInstance.updateLastAccessTs(instance.getLastAccessTs)
    newInstance
  }

  def apply(id: Uuid,
            clientInfo: CmClientInformation,
            subscriptions: java.util.Collection[SubscriptionInfo]): CmClientInstanceState = {
    create(id, clientInfo, subscriptions)
  }

  private def create(id: Uuid,
                     clientInfo: CmClientInformation,
                     subscriptions: java.util.Collection[SubscriptionInfo]): CmClientInstanceState = {

    var targetMetrics = new ListBuffer[String]()
    var pushInterval = DEFAULT_PUSH_INTERVAL
    val targetSubscriptions = new java.util.ArrayList[SubscriptionInfo]()
    var allMetricsSubscribed = false

    subscriptions.forEach(v =>
      if (clientInfo.isMatched(v.getClientMatchingPatterns)) {
        allMetricsSubscribed = allMetricsSubscribed | v.getAllMetricsSubscribed
        targetMetrics = targetMetrics ++ v.getSubscribedMetrics
        targetSubscriptions.add(v)
        pushInterval = Math.min(pushInterval, v.getPushIntervalMs)
      }
    )

    // if pushInterval == 0 means, metrics collection is disabled for this client so clear all the metrics
    // and just send the empty metrics list to the client.
    // Otherwise, if client matches with any subscription that has the property `allMetricsSubscribed`
    // which means there is no need for filtering the metrics, so as per KIP-714 protocol just send the
    // empty string as the contents of the list so that client would send all the metrics updates
    // Otherwise, just use the compiled metrics.
    if (pushInterval == 0) {
      info(s"Metrics collection is disabled for the client: ${id.toString}")
      targetMetrics.clear()
    } else if (allMetricsSubscribed) {
      targetMetrics.clear()
      targetMetrics.append("")
    }

    new CmClientInstanceState(id,  clientInfo, targetSubscriptions,
                              targetMetrics.toList, pushInterval, allMetricsSubscribed)
  }

}

class CmClientInstanceState private (clientInstanceId: Uuid,
                                     clientInfo: CmClientInformation,
                                     subscriptions: java.util.Collection[SubscriptionInfo],
                                     var metrics: List[String],
                                     pushIntervalMs: Int,
                                     allMetricsSubscribed: Boolean) {

  private var lastAccessTs: Long = getCurrentTime
  private val subscriptionId = computeSubscriptionId
  private var terminating: Boolean = false

  def getPushIntervalMs = pushIntervalMs
  def getLastAccessTs = lastAccessTs
  def getSubscriptionId =  subscriptionId
  def getId = clientInstanceId
  def getClientInfo = clientInfo
  def getSubscriptions = subscriptions
  def getMetrics = metrics
  def getAllMetricsSubscribed = allMetricsSubscribed
  def isClientTerminating = this.terminating
  def updateLastAccessTs(tsInMs: Long): Unit =  { this.lastAccessTs = tsInMs }
  def setTerminatingFlag(f: Boolean): Unit =  { this.terminating = f }

  // Whenever push-interval for a client is set to 0 means metric collection for this specific client is disabled.
  def isDisabledForMetricsCollection :Boolean =  getPushIntervalMs == 0

  // Computes the SubscriptionId as a unique identifier for a client instance's subscription set, the id is generated
  // by calculating a CRC32 of the configured metrics subscriptions including the PushIntervalMs,
  // XORed with the ClientInstanceId.
  private def computeSubscriptionId: Int = {
    val crc = new CRC32
    val metricsStr = metrics.toString() + pushIntervalMs.toString
    crc.update(metricsStr.getBytes(StandardCharsets.UTF_8))
    crc.getValue.toInt ^ clientInstanceId.hashCode
  }

  // Returns true, if the time elapsed since last message is >= configured pushInterval, only exception to this
  // rule is if client sends the isClientTerminating the flag, in that case broker accepts the message
  // regardless of the push interval, however broker can accept only one request with isClientTerminating flag set.
  // Whatever the reason if client keeps on sending the messages with isClientTerminating flag, subsequent requests
  // are rejected if they don't fall into the current PushInterval.
  def canAcceptPushRequest() : Boolean = {
    val timeElapsedSinceLastMsg = getCurrentTime - getLastAccessTs
    timeElapsedSinceLastMsg >= getPushIntervalMs
    //(timeElapsedSinceLastMsg >= getPushIntervalMs) || (clientTerminatingFlagSet && !this.isClientTerminating)
  }

  // Returns the current push interval if timeElapsed since last message > configured pushInterval
  // otherwise, returns the delta (pushInterval - timeElapsedSinceLastMsg)
  def getAdjustedPushInterval(): Int = {
    val timeElapsedSinceLastMsg: Int = (getCurrentTime - getLastAccessTs).toInt
    if (timeElapsedSinceLastMsg < pushIntervalMs)
      pushIntervalMs - timeElapsedSinceLastMsg
    else
      pushIntervalMs
  }

}


