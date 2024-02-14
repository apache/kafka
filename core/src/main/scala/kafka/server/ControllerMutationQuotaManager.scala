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

import kafka.network.RequestChannel
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.QuotaViolationException
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.stats.Rate
import org.apache.kafka.common.metrics.stats.TokenBucket
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time
import org.apache.kafka.network.Session
import org.apache.kafka.server.quota.ClientQuotaCallback
import org.apache.kafka.server.config.ClientQuotaManagerConfig

import scala.jdk.CollectionConverters._

/**
 * The ControllerMutationQuota trait defines a quota for a given user/clientId pair. Such
 * quota is not meant to be cached forever but rather during the lifetime of processing
 * a request.
 */
trait ControllerMutationQuota {
  def isExceeded: Boolean
  def record(permits: Double): Unit
  def throttleTime: Int
}

/**
 * Default quota used when quota is disabled.
 */
object UnboundedControllerMutationQuota extends ControllerMutationQuota {
  override def isExceeded: Boolean = false
  override def record(permits: Double): Unit = ()
  override def throttleTime: Int = 0
}

/**
 * The AbstractControllerMutationQuota is the base class of StrictControllerMutationQuota and
 * PermissiveControllerMutationQuota.
 *
 * @param time @Time object to use
 */
abstract class AbstractControllerMutationQuota(private val time: Time) extends ControllerMutationQuota {
  protected var lastThrottleTimeMs = 0L
  protected var lastRecordedTimeMs = 0L

  protected def updateThrottleTime(e: QuotaViolationException, timeMs: Long): Unit = {
    lastThrottleTimeMs = ControllerMutationQuotaManager.throttleTimeMs(e, timeMs)
    lastRecordedTimeMs = timeMs
  }

  override def throttleTime: Int = {
    // If a throttle time has been recorded, we adjust it by deducting the time elapsed
    // between the recording and now. We do this because `throttleTime` may be called
    // long after having recorded it, especially when a request waits in the purgatory.
    val deltaTimeMs = time.milliseconds - lastRecordedTimeMs
    Math.max(0, lastThrottleTimeMs - deltaTimeMs).toInt
  }
}

/**
 * The StrictControllerMutationQuota defines a strict quota for a given user/clientId pair. The
 * quota is strict meaning that 1) it does not accept any mutations once the quota is exhausted
 * until it gets back to the defined rate; and 2) it does not throttle for any number of mutations
 * if quota is not already exhausted.
 *
 * @param time @Time object to use
 * @param quotaSensor @Sensor object with a defined quota for a given user/clientId pair
 */
class StrictControllerMutationQuota(private val time: Time,
                                    private val quotaSensor: Sensor)
    extends AbstractControllerMutationQuota(time) {

  override def isExceeded: Boolean = lastThrottleTimeMs > 0

  override def record(permits: Double): Unit = {
    val timeMs = time.milliseconds
    try {
      quotaSensor synchronized {
        quotaSensor.checkQuotas(timeMs)
        quotaSensor.record(permits, timeMs, false)
      }
    } catch {
      case e: QuotaViolationException =>
        updateThrottleTime(e, timeMs)
        throw new ThrottlingQuotaExceededException(lastThrottleTimeMs.toInt,
          Errors.THROTTLING_QUOTA_EXCEEDED.message)
    }
  }
}

/**
 * The PermissiveControllerMutationQuota defines a permissive quota for a given user/clientId pair.
 * The quota is permissive meaning that 1) it does accept any mutations even if the quota is
 * exhausted; and 2) it does throttle as soon as the quota is exhausted.
 *
 * @param time @Time object to use
 * @param quotaSensor @Sensor object with a defined quota for a given user/clientId pair
 */
class PermissiveControllerMutationQuota(private val time: Time,
                                        private val quotaSensor: Sensor)
    extends AbstractControllerMutationQuota(time) {

  override def isExceeded: Boolean = false

  override def record(permits: Double): Unit = {
    val timeMs = time.milliseconds
    try {
      quotaSensor.record(permits, timeMs, true)
    } catch {
      case e: QuotaViolationException =>
        updateThrottleTime(e, timeMs)
    }
  }
}

object ControllerMutationQuotaManager {

  /**
   * This calculates the amount of time needed to bring the TokenBucket within quota
   * assuming that no new metrics are recorded.
   *
   * Basically, if a value < 0 is observed, the time required to bring it to zero is
   * -value / refill rate (quota bound) * 1000.
   */
  def throttleTimeMs(e: QuotaViolationException, timeMs: Long): Long = {
    e.metric().measurable() match {
      case _: TokenBucket =>
        Math.round(-e.value() / e.bound() * 1000)
      case _ => throw new IllegalArgumentException(
        s"Metric ${e.metric().metricName()} is not a TokenBucket metric, value ${e.metric().measurable()}")
    }
  }
}

/**
 * The ControllerMutationQuotaManager is a specialized ClientQuotaManager used in the context
 * of throttling controller's operations/mutations.
 *
 * @param config @ClientQuotaManagerConfig quota configs
 * @param metrics @Metrics Metrics instance
 * @param time @Time object to use
 * @param threadNamePrefix The thread prefix to use
 * @param quotaCallback @ClientQuotaCallback ClientQuotaCallback to use
 */
class ControllerMutationQuotaManager(private val config: ClientQuotaManagerConfig,
                                     private val metrics: Metrics,
                                     private val time: Time,
                                     private val threadNamePrefix: String,
                                     private val quotaCallback: Option[ClientQuotaCallback])
    extends ClientQuotaManager(config, metrics, QuotaType.ControllerMutation, time, threadNamePrefix, quotaCallback) {

  override protected def clientQuotaMetricName(quotaMetricTags: Map[String, String]): MetricName = {
    metrics.metricName("tokens", QuotaType.ControllerMutation.toString,
      "Tracking remaining tokens in the token bucket per user/client-id",
      quotaMetricTags.asJava)
  }

  private def clientRateMetricName(quotaMetricTags: Map[String, String]): MetricName = {
    metrics.metricName("mutation-rate", QuotaType.ControllerMutation.toString,
      "Tracking mutation-rate per user/client-id",
      quotaMetricTags.asJava)
  }

  override protected def registerQuotaMetrics(metricTags: Map[String, String])(sensor: Sensor): Unit = {
    sensor.add(
      clientRateMetricName(metricTags),
      new Rate
    )
    sensor.add(
      clientQuotaMetricName(metricTags),
      new TokenBucket,
      getQuotaMetricConfig(metricTags)
    )
  }

  /**
   * Records that a user/clientId accumulated or would like to accumulate the provided amount at the
   * the specified time, returns throttle time in milliseconds. The quota is strict meaning that it
   * does not accept any mutations once the quota is exhausted until it gets back to the defined rate.
   *
   * @param session The session from which the user is extracted
   * @param clientId The client id
   * @param value The value to accumulate
   * @param timeMs The time at which to accumulate the value
   * @return The throttle time in milliseconds defines as the time to wait until the average
   *         rate gets back to the defined quota
   */
  override def recordAndGetThrottleTimeMs(session: Session, clientId: String, value: Double, timeMs: Long): Int = {
    val clientSensors = getOrCreateQuotaSensors(session, clientId)
    val quotaSensor = clientSensors.quotaSensor
    try {
      quotaSensor synchronized {
        quotaSensor.checkQuotas(timeMs)
        quotaSensor.record(value, timeMs, false)
      }
      0
    } catch {
      case e: QuotaViolationException =>
        val throttleTimeMs = ControllerMutationQuotaManager.throttleTimeMs(e, timeMs).toInt
        debug(s"Quota violated for sensor (${quotaSensor.name}). Delay time: ($throttleTimeMs)")
        throttleTimeMs
    }
  }

  /**
   * Returns a StrictControllerMutationQuota for the given user/clientId pair or
   * a UnboundedControllerMutationQuota$ if the quota is disabled.
   *
   * @param session The session from which the user is extracted
   * @param clientId The client id
   * @return ControllerMutationQuota
   */
  def newStrictQuotaFor(session: Session, clientId: String): ControllerMutationQuota = {
    if (quotasEnabled) {
      val clientSensors = getOrCreateQuotaSensors(session, clientId)
      new StrictControllerMutationQuota(time, clientSensors.quotaSensor)
    } else {
      UnboundedControllerMutationQuota
    }
  }

  def newStrictQuotaFor(request: RequestChannel.Request): ControllerMutationQuota =
    newStrictQuotaFor(request.session, request.header.clientId)

  /**
   * Returns a PermissiveControllerMutationQuota for the given user/clientId pair or
   * a UnboundedControllerMutationQuota$ if the quota is disabled.
   *
   * @param session The session from which the user is extracted
   * @param clientId The client id
   * @return ControllerMutationQuota
   */
  def newPermissiveQuotaFor(session: Session, clientId: String): ControllerMutationQuota = {
    if (quotasEnabled) {
      val clientSensors = getOrCreateQuotaSensors(session, clientId)
      new PermissiveControllerMutationQuota(time, clientSensors.quotaSensor)
    } else {
      UnboundedControllerMutationQuota
    }
  }

  def newPermissiveQuotaFor(request: RequestChannel.Request): ControllerMutationQuota =
    newPermissiveQuotaFor(request.session, request.header.clientId)

  /**
   * Returns a ControllerMutationQuota based on `strictSinceVersion`. It returns a strict
   * quota if the version is equal to or above of the `strictSinceVersion`, a permissive
   * quota if the version is below, and a unbounded quota if the quota is disabled.
   *
   * When the quota is strictly enforced. Any operation above the quota is not allowed
   * and rejected with a THROTTLING_QUOTA_EXCEEDED error.
   *
   * @param request The request to extract the user and the clientId from
   * @param strictSinceVersion The version since quota is strict
   * @return
   */
  def newQuotaFor(request: RequestChannel.Request, strictSinceVersion: Short): ControllerMutationQuota = {
    if (request.header.apiVersion() >= strictSinceVersion)
      newStrictQuotaFor(request)
    else
      newPermissiveQuotaFor(request)
  }
}
