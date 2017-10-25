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

import java.util.concurrent.{ConcurrentHashMap, DelayQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.{Logging, ShutdownableThread}
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Avg, Rate, Total}
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.collection.JavaConverters._

/**
 * Represents the sensors aggregated per client
 * @param quotaEntity Quota entity representing <client-id>, <user> or <user, client-id>
 * @param quotaSensor @Sensor that tracks the quota
 * @param throttleTimeSensor @Sensor that tracks the throttle time
 */
case class ClientSensors(quotaEntity: QuotaEntity, quotaSensor: Sensor, throttleTimeSensor: Sensor)

/**
 * Configuration settings for quota management
 * @param quotaBytesPerSecondDefault The default bytes per second quota allocated to any client-id if
 *        dynamic defaults or user quotas are not set
 * @param numQuotaSamples The number of samples to retain in memory
 * @param quotaWindowSizeSeconds The time span of each sample
 *
 */
case class ClientQuotaManagerConfig(quotaBytesPerSecondDefault: Long =
                                        ClientQuotaManagerConfig.QuotaBytesPerSecondDefault,
                                    numQuotaSamples: Int =
                                        ClientQuotaManagerConfig.DefaultNumQuotaSamples,
                                    quotaWindowSizeSeconds: Int =
                                        ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds)

object ClientQuotaManagerConfig {
  val QuotaBytesPerSecondDefault = Long.MaxValue
  // Always have 10 whole windows + 1 current window
  val DefaultNumQuotaSamples = 11
  val DefaultQuotaWindowSizeSeconds = 1
  // Purge sensors after 1 hour of inactivity
  val InactiveSensorExpirationTimeSeconds  = 3600
  val QuotaRequestPercentDefault = Int.MaxValue.toDouble
  val NanosToPercentagePerSecond = 100.0 / TimeUnit.SECONDS.toNanos(1)

  val UnlimitedQuota = Quota.upperBound(Long.MaxValue)
  val DefaultClientIdQuotaId = QuotaId(None, Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
  val DefaultUserQuotaId = QuotaId(Some(ConfigEntityName.Default), None, None)
  val DefaultUserClientIdQuotaId = QuotaId(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
}

object QuotaTypes {
  val NoQuotas = 0
  val ClientIdQuotaEnabled = 1
  val UserQuotaEnabled = 2
  val UserClientIdQuotaEnabled = 4
}

case class QuotaId(sanitizedUser: Option[String], clientId: Option[String], sanitizedClientId: Option[String])

case class QuotaEntity(quotaId: QuotaId, sanitizedUser: String, clientId: String, sanitizedClientId: String, quota: Quota)

/**
 * Helper class that records per-client metrics. It is also responsible for maintaining Quota usage statistics
 * for all clients.
 * <p/>
 * Quotas can be set at <user, client-id>, user or client-id levels. For a given client connection,
 * the most specific quota matching the connection will be applied. For example, if both a <user, client-id>
 * and a user quota match a connection, the <user, client-id> quota will be used. Otherwise, user quota takes
 * precedence over client-id quota. The order of precedence is:
 * <ul>
 *   <li>/config/users/<user>/clients/<client-id>
 *   <li>/config/users/<user>/clients/<default>
 *   <li>/config/users/<user>
 *   <li>/config/users/<default>/clients/<client-id>
 *   <li>/config/users/<default>/clients/<default>
 *   <li>/config/users/<default>
 *   <li>/config/clients/<client-id>
 *   <li>/config/clients/<default>
 * </ul>
 * Quota limits including defaults may be updated dynamically. The implementation is optimized for the case
 * where a single level of quotas is configured.
 *
 * @param config @ClientQuotaManagerConfig quota configs
 * @param metrics @Metrics Metrics instance
 * @param quotaType Quota type of this quota manager
 * @param time @Time object to use
 */
class ClientQuotaManager(private val config: ClientQuotaManagerConfig,
                         private val metrics: Metrics,
                         private val quotaType: QuotaType,
                         private val time: Time) extends Logging {
  private val overriddenQuota = new ConcurrentHashMap[QuotaId, Quota]()
  private val staticConfigClientIdQuota = Quota.upperBound(config.quotaBytesPerSecondDefault)
  @volatile private var quotaTypesEnabled =
    if (config.quotaBytesPerSecondDefault == Long.MaxValue) QuotaTypes.NoQuotas
    else QuotaTypes.ClientIdQuotaEnabled
  private val lock = new ReentrantReadWriteLock()
  private val delayQueue = new DelayQueue[ThrottledResponse]()
  private val sensorAccessor = new SensorAccess(lock, metrics)
  val throttledRequestReaper = new ThrottledRequestReaper(delayQueue)

  private val delayQueueSensor = metrics.sensor(quotaType + "-delayQueue")
  delayQueueSensor.add(metrics.metricName("queue-size",
                                      quotaType.toString,
                                      "Tracks the size of the delay queue"), new Total())
  start() // Use start method to keep findbugs happy
  private def start() {
    throttledRequestReaper.start()
  }

  /**
   * Reaper thread that triggers callbacks on all throttled requests
   * @param delayQueue DelayQueue to dequeue from
   */
  class ThrottledRequestReaper(delayQueue: DelayQueue[ThrottledResponse]) extends ShutdownableThread(
    "ThrottledRequestReaper-%s".format(quotaType), false) {

    override def doWork(): Unit = {
      val response: ThrottledResponse = delayQueue.poll(1, TimeUnit.SECONDS)
      if (response != null) {
        // Decrement the size of the delay queue
        delayQueueSensor.record(-1)
        trace("Response throttled for: " + response.throttleTimeMs + " ms")
        response.execute()
      }
    }
  }

  /**
   * Returns true if any quotas are enabled for this quota manager. This is used
   * to determine if quota related metrics should be created.
   * Note: If any quotas (static defaults, dynamic defaults or quota overrides) have
   * been configured for this broker at any time for this quota type, quotasEnabled will
   * return true until the next broker restart, even if all quotas are subsequently deleted.
   */
  def quotasEnabled: Boolean = quotaTypesEnabled != QuotaTypes.NoQuotas

  /**
   * Records that a user/clientId changed some metric being throttled (produced/consumed bytes, request processing time etc.)
   * If quota has been violated, callback is invoked after a delay, otherwise the callback is invoked immediately.
   * Throttle time calculation may be overridden by sub-classes.
   * @param sanitizedUser user principal of client
   * @param clientId clientId that produced/fetched the data
   * @param value amount of data in bytes or request processing time as a percentage
   * @param callback Callback function. This will be triggered immediately if quota is not violated.
   *                 If there is a quota violation, this callback will be triggered after a delay
   * @return Number of milliseconds to delay the response in case of Quota violation.
   *         Zero otherwise
   */
  def maybeRecordAndThrottle(sanitizedUser: String, clientId: String, value: Double, callback: Int => Unit): Int = {
    if (quotasEnabled) {
      val clientSensors = getOrCreateQuotaSensors(sanitizedUser, clientId)
      recordAndThrottleOnQuotaViolation(clientSensors, value, callback)
    } else {
      // Don't record any metrics if quotas are not enabled at any level
      val throttleTimeMs = 0
      callback(throttleTimeMs)
      throttleTimeMs
    }
  }

  def recordAndThrottleOnQuotaViolation(clientSensors: ClientSensors, value: Double, callback: Int => Unit): Int = {
    var throttleTimeMs = 0
    try {
      clientSensors.quotaSensor.record(value)
      // trigger the callback immediately if quota is not violated
      callback(0)
    } catch {
      case _: QuotaViolationException =>
        // Compute the delay
        val clientQuotaEntity = clientSensors.quotaEntity
        val clientMetric = metrics.metrics().get(clientRateMetricName(clientQuotaEntity.sanitizedUser, clientQuotaEntity.clientId))
        throttleTimeMs = throttleTime(clientMetric, getQuotaMetricConfig(clientQuotaEntity.quota)).toInt
        clientSensors.throttleTimeSensor.record(throttleTimeMs)
        // If delayed, add the element to the delayQueue
        delayQueue.add(new ThrottledResponse(time, throttleTimeMs, callback))
        delayQueueSensor.record()
        logger.debug("Quota violated for sensor (%s). Delay time: (%d)".format(clientSensors.quotaSensor.name(), throttleTimeMs))
    }
    throttleTimeMs
  }

  /**
   * Records that a user/clientId changed some metric being throttled without checking for
   * quota violation. The aggregate value will subsequently be used for throttling when the
   * next request is processed.
   */
  def recordNoThrottle(clientSensors: ClientSensors, value: Double) {
    clientSensors.quotaSensor.record(value, time.milliseconds(), false)
  }

  /**
   * Determines the quota-id for the client with the specified user principal
   * and client-id and returns the quota entity that encapsulates the quota-id
   * and the associated quota override or default quota.
   *
   */
  private def quotaEntity(sanitizedUser: String, clientId: String, sanitizedClientId: String) : QuotaEntity = {
    quotaTypesEnabled match {
      case QuotaTypes.NoQuotas | QuotaTypes.ClientIdQuotaEnabled =>
        val quotaId = QuotaId(None, Some(clientId), Some(sanitizedClientId))
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultClientIdQuotaId)
          if (quota == null)
            quota = staticConfigClientIdQuota
        }
        QuotaEntity(quotaId, "", clientId, sanitizedClientId, quota)
      case QuotaTypes.UserQuotaEnabled =>
        val quotaId = QuotaId(Some(sanitizedUser), None, None)
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultUserQuotaId)
          if (quota == null)
            quota = ClientQuotaManagerConfig.UnlimitedQuota
        }
        QuotaEntity(quotaId, sanitizedUser, "", "", quota)
      case QuotaTypes.UserClientIdQuotaEnabled =>
        val quotaId = QuotaId(Some(sanitizedUser), Some(clientId), Some(sanitizedClientId))
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(QuotaId(Some(sanitizedUser), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default)))
          if (quota == null) {
            quota = overriddenQuota.get(QuotaId(Some(ConfigEntityName.Default), Some(clientId), Some(sanitizedClientId)))
            if (quota == null) {
              quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultUserClientIdQuotaId)
              if (quota == null)
                quota = ClientQuotaManagerConfig.UnlimitedQuota
            }
          }
        }
        QuotaEntity(quotaId, sanitizedUser, clientId, sanitizedClientId, quota)
      case _ =>
        quotaEntityWithMultipleQuotaLevels(sanitizedUser, clientId, sanitizedClientId)
    }
  }

  private def quotaEntityWithMultipleQuotaLevels(sanitizedUser: String, clientId: String, sanitizerClientId: String) : QuotaEntity = {
    val userClientQuotaId = QuotaId(Some(sanitizedUser), Some(clientId), Some(sanitizerClientId))

    val userQuotaId = QuotaId(Some(sanitizedUser), None, None)
    val clientQuotaId = QuotaId(None, Some(clientId), Some(sanitizerClientId))
    var quotaId = userClientQuotaId
    var quotaConfigId = userClientQuotaId
    // 1) /config/users/<user>/clients/<client-id>
    var quota = overriddenQuota.get(quotaConfigId)
    if (quota == null) {
      // 2) /config/users/<user>/clients/<default>
      quotaId = userClientQuotaId
      quotaConfigId = QuotaId(Some(sanitizedUser), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
      quota = overriddenQuota.get(quotaConfigId)

      if (quota == null) {
        // 3) /config/users/<user>
        quotaId = userQuotaId
        quotaConfigId = quotaId
        quota = overriddenQuota.get(quotaConfigId)

        if (quota == null) {
          // 4) /config/users/<default>/clients/<client-id>
          quotaId = userClientQuotaId
          quotaConfigId = QuotaId(Some(ConfigEntityName.Default), Some(clientId), Some(sanitizerClientId))
          quota = overriddenQuota.get(quotaConfigId)

          if (quota == null) {
            // 5) /config/users/<default>/clients/<default>
            quotaId = userClientQuotaId
            quotaConfigId = QuotaId(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
            quota = overriddenQuota.get(quotaConfigId)

            if (quota == null) {
              // 6) /config/users/<default>
              quotaId = userQuotaId
              quotaConfigId = QuotaId(Some(ConfigEntityName.Default), None, None)
              quota = overriddenQuota.get(quotaConfigId)

              if (quota == null) {
                // 7) /config/clients/<client-id>
                quotaId = clientQuotaId
                quotaConfigId = QuotaId(None, Some(clientId), Some(sanitizerClientId))
                quota = overriddenQuota.get(quotaConfigId)

                if (quota == null) {
                  // 8) /config/clients/<default>
                  quotaId = clientQuotaId
                  quotaConfigId = QuotaId(None, Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
                  quota = overriddenQuota.get(quotaConfigId)

                  if (quota == null) {
                    quotaId = clientQuotaId
                    quotaConfigId = null
                    quota = staticConfigClientIdQuota
                  }
                }
              }
            }
          }
        }
      }
    }
    val quotaUser = if (quotaId == clientQuotaId) "" else sanitizedUser
    val quotaClientId = if (quotaId == userQuotaId) "" else clientId
    QuotaEntity(quotaId, quotaUser, quotaClientId, sanitizerClientId, quota)
  }

  /**
   * Returns the quota for the client with the specified (non-encoded) user principal and client-id.
   * 
   * Note: this method is expensive, it is meant to be used by tests only
   */
  def quota(user: String, clientId: String) = {
    quotaEntity(Sanitizer.sanitize(user), clientId, Sanitizer.sanitize(clientId)).quota
  }

  /*
   * This calculates the amount of time needed to bring the metric within quota
   * assuming that no new metrics are recorded.
   *
   * Basically, if O is the observed rate and T is the target rate over a window of W, to bring O down to T,
   * we need to add a delay of X to W such that O * W / (W + X) = T.
   * Solving for X, we get X = (O - T)/T * W.
   */
  protected def throttleTime(clientMetric: KafkaMetric, config: MetricConfig): Long = {
    val rateMetric: Rate = measurableAsRate(clientMetric.metricName(), clientMetric.measurable())
    val quota = config.quota()
    val difference = clientMetric.value() - quota.bound
    // Use the precise window used by the rate calculation
    val throttleTimeMs = difference / quota.bound * rateMetric.windowSize(config, time.milliseconds())
    throttleTimeMs.round
  }

  // Casting to Rate because we only use Rate in Quota computation
  private def measurableAsRate(name: MetricName, measurable: Measurable): Rate = {
    measurable match {
      case r: Rate => r
      case _ => throw new IllegalArgumentException(s"Metric $name is not a Rate metric, value $measurable")
    }
  }

  /*
   * This function either returns the sensors for a given client id or creates them if they don't exist
   * First sensor of the tuple is the quota enforcement sensor. Second one is the throttle time sensor
   */
  def getOrCreateQuotaSensors(sanitizedUser: String, clientId: String): ClientSensors = {
    val sanitizedClientId = Sanitizer.sanitize(clientId)
    val clientQuotaEntity = quotaEntity(sanitizedUser, clientId, sanitizedClientId)
    // Names of the sensors to access
    ClientSensors(
      clientQuotaEntity,
      sensorAccessor.getOrCreate(
        getQuotaSensorName(clientQuotaEntity.quotaId),
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        clientRateMetricName(clientQuotaEntity.sanitizedUser, clientQuotaEntity.clientId),
        Some(getQuotaMetricConfig(clientQuotaEntity.quota)),
        new Rate
      ),
      sensorAccessor.getOrCreate(getThrottleTimeSensorName(clientQuotaEntity.quotaId),
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        throttleMetricName(clientQuotaEntity),
        None,
        new Avg
      )
    )
  }

  private def getThrottleTimeSensorName(quotaId: QuotaId): String = quotaType + "ThrottleTime-" + quotaId.sanitizedUser.getOrElse("") + ':' + quotaId.clientId.getOrElse("")

  private def getQuotaSensorName(quotaId: QuotaId): String = quotaType + "-" + quotaId.sanitizedUser.getOrElse("") + ':' + quotaId.clientId.getOrElse("")

  protected def getQuotaMetricConfig(quota: Quota): MetricConfig = {
    new MetricConfig()
            .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
            .samples(config.numQuotaSamples)
            .quota(quota)
  }

  protected def getOrCreateSensor(sensorName: String, metricName: MetricName): Sensor = {
    sensorAccessor.getOrCreate(
        sensorName,
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        metricName,
        None,
        new Rate
      )
  }

  /**
   * Overrides quotas for <user>, <client-id> or <user, client-id> or the dynamic defaults
   * for any of these levels.
   * @param sanitizedUser user to override if quota applies to <user> or <user, client-id>
   * @param clientId client to override if quota applies to <client-id> or <user, client-id>
   * @param sanitizedClientId sanitized client ID to override if quota applies to <client-id> or <user, client-id>
   * @param quota custom quota to apply or None if quota override is being removed
   */
  def updateQuota(sanitizedUser: Option[String], clientId: Option[String], sanitizedClientId: Option[String], quota: Option[Quota]) {
    /*
     * Acquire the write lock to apply changes in the quota objects.
     * This method changes the quota in the overriddenQuota map and applies the update on the actual KafkaMetric object (if it exists).
     * If the KafkaMetric hasn't been created, the most recent value will be used from the overriddenQuota map.
     * The write lock prevents quota update and creation at the same time. It also guards against concurrent quota change
     * notifications
     */
    lock.writeLock().lock()
    try {
      val quotaId = QuotaId(sanitizedUser, clientId, sanitizedClientId)
      val userInfo = sanitizedUser match {
        case Some(ConfigEntityName.Default) => "default user "
        case Some(user) => "user " + user + " "
        case None => ""
      }
      val clientIdInfo = clientId match {
        case Some(ConfigEntityName.Default) => "default client-id"
        case Some(id) => "client-id " + id
        case None => ""
      }
      quota match {
        case Some(newQuota) =>
          logger.info(s"Changing ${quotaType} quota for ${userInfo}${clientIdInfo} to $newQuota.bound}")
          overriddenQuota.put(quotaId, newQuota)
          (sanitizedUser, clientId) match {
            case (Some(_), Some(_)) => quotaTypesEnabled |= QuotaTypes.UserClientIdQuotaEnabled
            case (Some(_), None) => quotaTypesEnabled |= QuotaTypes.UserQuotaEnabled
            case (None, Some(_)) => quotaTypesEnabled |= QuotaTypes.ClientIdQuotaEnabled
            case (None, None) =>
          }
        case None =>
          logger.info(s"Removing ${quotaType} quota for ${userInfo}${clientIdInfo}")
          overriddenQuota.remove(quotaId)
      }

      val quotaMetricName = clientRateMetricName(sanitizedUser.getOrElse(""), clientId.getOrElse(""))
      val allMetrics = metrics.metrics()

      // If multiple-levels of quotas are defined or if this is a default quota update, traverse metrics
      // to find all affected values. Otherwise, update just the single matching one.
      val singleUpdate = quotaTypesEnabled match {
        case QuotaTypes.NoQuotas | QuotaTypes.ClientIdQuotaEnabled | QuotaTypes.UserQuotaEnabled | QuotaTypes.UserClientIdQuotaEnabled =>
          !sanitizedUser.filter(_ == ConfigEntityName.Default).isDefined && !clientId.filter(_ == ConfigEntityName.Default).isDefined
        case _ => false
      }
      if (singleUpdate) {
          // Change the underlying metric config if the sensor has been created
          val metric = allMetrics.get(quotaMetricName)
          if (metric != null) {
            val metricConfigEntity = quotaEntity(sanitizedUser.getOrElse(""), clientId.getOrElse(""), sanitizedClientId.getOrElse(""))
            val newQuota = metricConfigEntity.quota
            logger.info(s"Sensor for ${userInfo}${clientIdInfo} already exists. Changing quota to ${newQuota.bound()} in MetricConfig")
            metric.config(getQuotaMetricConfig(newQuota))
          }
      } else {
          allMetrics.asScala.filterKeys(n => n.name == quotaMetricName.name && n.group == quotaMetricName.group).foreach {
            case (metricName, metric) =>
              val userTag = if (metricName.tags.containsKey("user")) metricName.tags.get("user") else ""
              val clientIdTag = if (metricName.tags.containsKey("client-id")) metricName.tags.get("client-id") else ""
              val metricConfigEntity = quotaEntity(userTag, clientIdTag, Sanitizer.sanitize(clientIdTag))
              if (metricConfigEntity.quota != metric.config.quota) {
                val newQuota = metricConfigEntity.quota
                logger.info(s"Sensor for quota-id ${metricConfigEntity.quotaId} already exists. Setting quota to ${newQuota.bound} in MetricConfig")
                metric.config(getQuotaMetricConfig(newQuota))
              }
          }
      }

    } finally {
      lock.writeLock().unlock()
    }
  }

  protected def clientRateMetricName(sanitizedUser: String, clientId: String): MetricName = {
    metrics.metricName("byte-rate", quotaType.toString,
                   "Tracking byte-rate per user/client-id",
                   "user", sanitizedUser,
                   "client-id", clientId)
  }

  private def throttleMetricName(quotaEntity: QuotaEntity): MetricName = {
    metrics.metricName("throttle-time",
                       quotaType.toString,
                       "Tracking average throttle-time per user/client-id",
                       "user", quotaEntity.sanitizedUser,
                       "client-id", quotaEntity.clientId)
  }

  def shutdown() = {
    throttledRequestReaper.shutdown()
  }
}
