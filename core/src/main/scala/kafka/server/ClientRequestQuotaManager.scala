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

import java.util.concurrent.TimeUnit
import kafka.network.RequestChannel
import kafka.utils.QuotaUtils
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.Rate
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.quota.ClientQuotaCallback

import scala.jdk.CollectionConverters._

object ClientRequestQuotaManager {
  val NanosToPercentagePerSecond: Double = 100.0 / TimeUnit.SECONDS.toNanos(1)
  // Since exemptSensor is for all clients and has a constant name, we do not expire exemptSensor and only
  // create once.
  val DefaultInactiveExemptSensorExpirationTimeSeconds: Long = Long.MaxValue

  private val ExemptSensorName = "exempt-" + QuotaType.Request
}

class ClientRequestQuotaManager(private val config: ClientQuotaManagerConfig,
                                private val metrics: Metrics,
                                private val time: Time,
                                private val threadNamePrefix: String,
                                private val quotaCallback: Option[ClientQuotaCallback])
    extends ClientQuotaManager(config, metrics, QuotaType.Request, time, threadNamePrefix, quotaCallback) {

  private val maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(this.config.quotaWindowSizeSeconds)
  private val exemptMetricName = metrics.metricName("exempt-request-time",
    QuotaType.Request.toString, "Tracking exempt-request-time utilization percentage")

  val exemptSensor: Sensor = getOrCreateSensor(ClientRequestQuotaManager.ExemptSensorName,
    ClientRequestQuotaManager.DefaultInactiveExemptSensorExpirationTimeSeconds,
    sensor => sensor.add(exemptMetricName, new Rate))

  def recordExempt(value: Double): Unit = {
    exemptSensor.record(value)
  }

  /**
    * Records that a user/clientId changed request processing time being throttled. If quota has been violated, return
    * throttle time in milliseconds. Throttle time calculation may be overridden by sub-classes.
    * @param request client request
    * @return Number of milliseconds to throttle in case of quota violation. Zero otherwise
    */
  def maybeRecordAndGetThrottleTimeMs(request: RequestChannel.Request, timeMs: Long): Int = {
    if (quotasEnabled) {
      request.recordNetworkThreadTimeCallback = Some(timeNanos => recordNoThrottle(
        request.session, request.header.clientId, nanosToPercentage(timeNanos)))
      recordAndGetThrottleTimeMs(request.session, request.header.clientId,
        nanosToPercentage(request.requestThreadTimeNanos), timeMs)
    } else {
      0
    }
  }

  def maybeRecordExempt(request: RequestChannel.Request): Unit = {
    if (quotasEnabled) {
      request.recordNetworkThreadTimeCallback = Some(timeNanos => recordExempt(nanosToPercentage(timeNanos)))
      recordExempt(nanosToPercentage(request.requestThreadTimeNanos))
    }
  }

  override protected def throttleTime(e: QuotaViolationException, timeMs: Long): Long = {
    QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs)
  }

  override protected def clientQuotaMetricName(quotaMetricTags: Map[String, String]): MetricName = {
    metrics.metricName("request-time", QuotaType.Request.toString,
      "Tracking request-time per user/client-id",
      quotaMetricTags.asJava)
  }

  private def nanosToPercentage(nanos: Long): Double =
    nanos * ClientRequestQuotaManager.NanosToPercentagePerSecond
}
