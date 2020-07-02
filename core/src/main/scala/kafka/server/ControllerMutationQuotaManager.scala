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
import kafka.network.RequestChannel.Session
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.QuotaViolationException
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.Sensor.QuotaEnforcementType
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.quota.ClientQuotaCallback

import scala.jdk.CollectionConverters._

/**
 * The ControllerMutationQuota trait defines a quota for a given user/clientId pair. Such
 * quota is not meant to be cached forever but rather during the lifetime of processing
 * a request.
 */
trait ControllerMutationQuota {
  def isExceeded: Boolean
  def accept(permits: Double): Unit
  def throttleTime: Int
}

/**
 * Default quota used when quota is disabled.
 */
object UnboundedControllerMutationQuota extends ControllerMutationQuota {
  override def isExceeded: Boolean = false
  override def accept(permits: Double): Unit = ()
  override def throttleTime: Int = 0
}

/**
 * The StrictControllerMutationQuota defines a strict quota for a given user/clientId pair. The
 * quota is strict meaning that it does not accept any mutations once the quota is exhausted until
 * it gets back to the defined rate.
 *
 * @param time @Time object to use
 * @param quotaSensor @Sensor object with a defined quota for a given user/clientId pair
 */
class StrictControllerMutationQuota(private val time: Time,
                                    private val quotaSensor: Sensor) extends ControllerMutationQuota {

  private var lastThrottleTimeMs = 0L
  private var lastRecordedTimeMs = 0L

  override def isExceeded: Boolean = lastThrottleTimeMs > 0

  override def accept(permits: Double): Unit = {
    val timeMs = time.milliseconds
    try {
      quotaSensor.record(permits, timeMs, QuotaEnforcementType.STRICT)
    } catch {
      case e: QuotaViolationException =>
        lastThrottleTimeMs = ClientQuotaManager.throttleTime(e, timeMs)
        lastRecordedTimeMs = timeMs
        throw new ThrottlingQuotaExceededException(lastThrottleTimeMs.toInt,
          Errors.THROTTLING_QUOTA_EXCEEDED.message)
    }
  }

  override def throttleTime: Int = {
    // If a throttle time has been recorded, we adjust it by deducting the time elapsed
    // between the recording and now. We do this because `throttleTime` may be called
    // long after having recorded it (e.g. when creating topics).
    val deltaTimeMs = time.milliseconds - lastRecordedTimeMs
    Math.max(0, lastThrottleTimeMs - deltaTimeMs).toInt
  }
}

/**
 * The PermissiveControllerMutationQuota defines a permissive quota for a given user/clientId pair.
 * The quota is permissive meaning that it does accept any mutations even if the quota is exhausted.
 *
 * @param time @Time object to use
 * @param quotaSensor @Sensor object with a defined quota for a given user/clientId pair
 */
class PermissiveControllerMutationQuota(private val time: Time,
                                        private val quotaSensor: Sensor) extends ControllerMutationQuota {

  private var lastThrottleTimeMs = 0L
  private var lastRecordedTimeMs = 0L

  override def isExceeded: Boolean = false

  override def accept(permits: Double): Unit = {
    val timeMs = time.milliseconds
    try {
      quotaSensor.record(permits, timeMs, QuotaEnforcementType.PERMISSIVE)
    } catch {
      case e: QuotaViolationException =>
        lastThrottleTimeMs = ClientQuotaManager.throttleTime(e, timeMs)
        lastRecordedTimeMs = timeMs
    }
  }

  override def throttleTime: Int = {
    // If a throttle time has been recorded, we adjust it by deducting the time elapsed
    // between the recording and now. We do this because `throttleTime` may be called
    // long after having recorded it (e.g. when creating topics).
    val deltaTimeMs = time.milliseconds - lastRecordedTimeMs
    Math.max(0, lastThrottleTimeMs - deltaTimeMs).toInt
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
    extends ClientQuotaManager(config, metrics, QuotaType.ControllerMutation, QuotaEnforcementType.STRICT,
      time, threadNamePrefix, quotaCallback) {

  override protected def clientRateMetricName(quotaMetricTags: Map[String, String]): MetricName = {
    metrics.metricName("mutation-rate", QuotaType.ControllerMutation.toString,
      "Tracking mutation-rate per user/client-id",
      quotaMetricTags.asJava)
  }

  /**
   * Returns a StrictControllerMutationQuota for the given session/clientId pair or
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
   * Returns a PermissiveControllerMutationQuota for the given session/clientId pair or
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
   * quota if the version is bellow, and a unbounded quota if the quota is disabled.
   *
   * @param request The request to extract the session and the clientId from
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
