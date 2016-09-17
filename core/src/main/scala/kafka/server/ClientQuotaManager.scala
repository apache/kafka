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

import java.net.{URLEncoder, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentHashMap, DelayQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.{ShutdownableThread, Logging}
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Total, Rate, Avg}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConversions._

/**
 * Represents the sensors aggregated per client
 * @param quotaSensor @Sensor that tracks the quota
 * @param throttleTimeSensor @Sensor that tracks the throttle time
 */
private case class ClientSensors(quotaSensor: Sensor, throttleTimeSensor: Sensor)

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

  val UnlimitedQuota = Quota.upperBound(Long.MaxValue)
  val DefaultClientIdQuotaId = QuotaId(None, Some(ConfigEntityName.Default))
  val DefaultUserQuotaId = QuotaId(Some(ConfigEntityName.Default), None)
  val DefaultUserClientIdQuotaId = QuotaId(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
}

object QuotaTypes {
  val NoQuotas = 0
  val ClientIdQuotaEnabled = 1
  val UserQuotaEnabled = 2
  val UserClientIdQuotaEnabled = 4
}

object QuotaId {

  /**
   * Sanitizes user principal to a safe value for use in MetricName
   * and as Zookeeper node name
   */
  def sanitize(user: String): String = {
    val encoded = URLEncoder.encode(user, StandardCharsets.UTF_8.name)
    val builder = new StringBuilder
    for (i <- 0 until encoded.length) {
      encoded.charAt(i) match {
        case '*' => builder.append("%2A") // Metric ObjectName treats * as pattern
        case '+' => builder.append("%20") // Space URL-encoded as +, replace with percent encoding
        case c => builder.append(c)
      }
    }
    builder.toString
  }

  /**
   * Decodes sanitized user principal
   */
  def desanitize(sanitizedUser: String): String = {
    URLDecoder.decode(sanitizedUser, StandardCharsets.UTF_8.name)
  }
}

case class QuotaId(sanitizedUser: Option[String], clientId: Option[String])

case class QuotaEntity(quotaId: QuotaId, sanitizedUser: String, clientId: String, quota: Quota)

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
 * @param apiKey API Key for the request
 * @param time @Time object to use
 */
class ClientQuotaManager(private val config: ClientQuotaManagerConfig,
                         private val metrics: Metrics,
                         private val apiKey: QuotaType,
                         private val time: Time) extends Logging {
  private val overriddenQuota = new ConcurrentHashMap[QuotaId, Quota]()
  private val staticConfigClientIdQuota = Quota.upperBound(config.quotaBytesPerSecondDefault)
  private var quotaTypesEnabled = if (config.quotaBytesPerSecondDefault == Long.MaxValue) QuotaTypes.NoQuotas else QuotaTypes.ClientIdQuotaEnabled
  private val lock = new ReentrantReadWriteLock()
  private val delayQueue = new DelayQueue[ThrottledResponse]()
  private val sensorAccessor = new SensorAccess
  val throttledRequestReaper = new ThrottledRequestReaper(delayQueue)
  throttledRequestReaper.start()

  private val delayQueueSensor = metrics.sensor(apiKey + "-delayQueue")
  delayQueueSensor.add(metrics.metricName("queue-size",
                                      apiKey.toString,
                                      "Tracks the size of the delay queue"), new Total())

  /**
   * Reaper thread that triggers callbacks on all throttled requests
   * @param delayQueue DelayQueue to dequeue from
   */
  class ThrottledRequestReaper(delayQueue: DelayQueue[ThrottledResponse]) extends ShutdownableThread(
    "ThrottledRequestReaper-%s".format(apiKey), false) {

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
   * Records that a clientId changed some metric being throttled (produced/consumed bytes, QPS etc.)
   * @param clientId clientId that produced the data
   * @param value amount of data written in bytes
   * @param callback Callback function. This will be triggered immediately if quota is not violated.
   *                 If there is a quota violation, this callback will be triggered after a delay
   * @return Number of milliseconds to delay the response in case of Quota violation.
   *         Zero otherwise
   */
  def recordAndMaybeThrottle(sanitizedUser: String, clientId: String, value: Int, callback: Int => Unit): Int = {
    val clientQuotaEntity = quotaEntity(sanitizedUser, clientId)
    val clientSensors = getOrCreateQuotaSensors(clientQuotaEntity)
    var throttleTimeMs = 0
    try {
      clientSensors.quotaSensor.record(value)
      // trigger the callback immediately if quota is not violated
      callback(0)
    } catch {
      case qve: QuotaViolationException =>
        // Compute the delay
        val clientMetric = metrics.metrics().get(clientRateMetricName(clientQuotaEntity.sanitizedUser, clientQuotaEntity.clientId))
        throttleTimeMs = throttleTime(clientMetric, getQuotaMetricConfig(clientQuotaEntity.quota))
        clientSensors.throttleTimeSensor.record(throttleTimeMs)
        // If delayed, add the element to the delayQueue
        delayQueue.add(new ThrottledResponse(time, throttleTimeMs, callback))
        delayQueueSensor.record()
        logger.debug("Quota violated for sensor (%s). Delay time: (%d)".format(clientSensors.quotaSensor.name(), throttleTimeMs))
    }
    throttleTimeMs
  }

  /**
   * Determines the quota-id for the client with the specified user principal
   * and client-id and returns the quota entity that encapsulates the quota-id
   * and the associated quota override or default quota.
   *
   */
  private def quotaEntity(sanitizedUser: String, clientId: String) : QuotaEntity = {
    quotaTypesEnabled match {
      case QuotaTypes.NoQuotas | QuotaTypes.ClientIdQuotaEnabled =>
        val quotaId = QuotaId(None, Some(clientId))
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultClientIdQuotaId)
          if (quota == null)
            quota = staticConfigClientIdQuota
        }
        QuotaEntity(quotaId, "", clientId, quota)
      case QuotaTypes.UserQuotaEnabled =>
        val quotaId = QuotaId(Some(sanitizedUser), None)
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultUserQuotaId)
          if (quota == null)
            quota = ClientQuotaManagerConfig.UnlimitedQuota
        }
        QuotaEntity(quotaId, sanitizedUser, "", quota)
      case QuotaTypes.UserClientIdQuotaEnabled =>
        val quotaId = QuotaId(Some(sanitizedUser), Some(clientId))
        var quota = overriddenQuota.get(quotaId)
        if (quota == null) {
          quota = overriddenQuota.get(QuotaId(Some(sanitizedUser), Some(ConfigEntityName.Default)))
          if (quota == null) {
            quota = overriddenQuota.get(QuotaId(Some(ConfigEntityName.Default), Some(clientId)))
            if (quota == null) {
              quota = overriddenQuota.get(ClientQuotaManagerConfig.DefaultUserClientIdQuotaId)
              if (quota == null)
                quota = ClientQuotaManagerConfig.UnlimitedQuota
            }
          }
        }
        QuotaEntity(quotaId, sanitizedUser, clientId, quota)
      case _ =>
        quotaEntityWithMultipleQuotaLevels(sanitizedUser, clientId)
    }
  }

  private def quotaEntityWithMultipleQuotaLevels(sanitizedUser: String, clientId: String) : QuotaEntity = {
    val userClientQuotaId = QuotaId(Some(sanitizedUser), Some(clientId))

    val userQuotaId = QuotaId(Some(sanitizedUser), None)
    val clientQuotaId = QuotaId(None, Some(clientId))
    var quotaId = userClientQuotaId
    var quotaConfigId = userClientQuotaId
    // 1) /config/users/<user>/clients/<client-id>
    var quota = overriddenQuota.get(quotaConfigId)
    if (quota == null) {
      // 2) /config/users/<user>/clients/<default>
      quotaId = userClientQuotaId
      quotaConfigId = QuotaId(Some(sanitizedUser), Some(ConfigEntityName.Default))
      quota = overriddenQuota.get(quotaConfigId)

      if (quota == null) {
        // 3) /config/users/<user>
        quotaId = userQuotaId
        quotaConfigId = quotaId
        quota = overriddenQuota.get(quotaConfigId)

        if (quota == null) {
          // 4) /config/users/<default>/clients/<client-id>
          quotaId = userClientQuotaId
          quotaConfigId = QuotaId(Some(ConfigEntityName.Default), Some(clientId))
          quota = overriddenQuota.get(quotaConfigId)

          if (quota == null) {
            // 5) /config/users/<default>/clients/<default>
            quotaId = userClientQuotaId
            quotaConfigId = QuotaId(Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
            quota = overriddenQuota.get(quotaConfigId)

            if (quota == null) {
              // 6) /config/users/<default>
              quotaId = userQuotaId
              quotaConfigId = QuotaId(Some(ConfigEntityName.Default), None)
              quota = overriddenQuota.get(quotaConfigId)

              if (quota == null) {
                // 7) /config/clients/<client-id>
                quotaId = clientQuotaId
                quotaConfigId = QuotaId(None, Some(clientId))
                quota = overriddenQuota.get(quotaConfigId)

                if (quota == null) {
                  // 8) /config/clients/<default>
                  quotaId = clientQuotaId
                  quotaConfigId = QuotaId(None, Some(ConfigEntityName.Default))
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
    QuotaEntity(quotaId, quotaUser, quotaClientId, quota)
  }

  /**
   * Returns the quota for the client with the specified (non-encoded) user principal and client-id.
   */
  def quota(user: String, clientId: String) = {
    quotaEntity(QuotaId.sanitize(user), clientId).quota
  }

  /*
   * This calculates the amount of time needed to bring the metric within quota
   * assuming that no new metrics are recorded.
   *
   * Basically, if O is the observed rate and T is the target rate over a window of W, to bring O down to T,
   * we need to add a delay of X to W such that O * W / (W + X) = T.
   * Solving for X, we get X = (O - T)/T * W.
   */
  private def throttleTime(clientMetric: KafkaMetric, config: MetricConfig): Int = {
    val rateMetric: Rate = measurableAsRate(clientMetric.metricName(), clientMetric.measurable())
    val quota = config.quota()
    val difference = clientMetric.value() - quota.bound
    // Use the precise window used by the rate calculation
    val throttleTimeMs = difference / quota.bound * rateMetric.windowSize(config, time.milliseconds())
    throttleTimeMs.round.toInt
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
  private def getOrCreateQuotaSensors(quotaEntity: QuotaEntity): ClientSensors = {
    // Names of the sensors to access
    ClientSensors(
      sensorAccessor.getOrCreate(
        getQuotaSensorName(quotaEntity.quotaId),
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        lock, metrics,
        () => clientRateMetricName(quotaEntity.sanitizedUser, quotaEntity.clientId),
        () => getQuotaMetricConfig(quotaEntity.quota),
        () => new Rate()
      ),
      sensorAccessor.getOrCreate(getThrottleTimeSensorName(quotaEntity.quotaId),
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        lock,
        metrics,
        () => throttleMetricName(quotaEntity),
        () => null,
        () => new Avg()
      )
    )
  }

  private def getThrottleTimeSensorName(quotaId: QuotaId): String = apiKey + "ThrottleTime-" + quotaId.sanitizedUser.getOrElse("") + ':' + quotaId.clientId.getOrElse("")

  private def getQuotaSensorName(quotaId: QuotaId): String = apiKey + "-" + quotaId.sanitizedUser.getOrElse("") + ':' + quotaId.clientId.getOrElse("")

  private def getQuotaMetricConfig(quota: Quota): MetricConfig = {
    new MetricConfig()
            .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
            .samples(config.numQuotaSamples)
            .quota(quota)
  }

  /**
   * Overrides quotas for <user>, <client-id> or <user, client-id> or the dynamic defaults
   * for any of these levels.
   * @param sanitizedUser user to override if quota applies to <user> or <user, client-id>
   * @param clientId client to override if quota applies to <client-id> or <user, client-id>
   * @param quota custom quota to apply or None if quota override is being removed
   */
  def updateQuota(sanitizedUser: Option[String], clientId: Option[String], quota: Option[Quota]) {
    /*
     * Acquire the write lock to apply changes in the quota objects.
     * This method changes the quota in the overriddenQuota map and applies the update on the actual KafkaMetric object (if it exists).
     * If the KafkaMetric hasn't been created, the most recent value will be used from the overriddenQuota map.
     * The write lock prevents quota update and creation at the same time. It also guards against concurrent quota change
     * notifications
     */
    lock.writeLock().lock()
    try {
      val quotaId = QuotaId(sanitizedUser, clientId)
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
          logger.info(s"Changing ${apiKey} quota for ${userInfo}${clientIdInfo} to ${newQuota.bound}")
          overriddenQuota.put(quotaId, newQuota)
          (sanitizedUser, clientId) match {
            case (Some(u), Some(c)) => quotaTypesEnabled |= QuotaTypes.UserClientIdQuotaEnabled
            case (Some(u), None) => quotaTypesEnabled |= QuotaTypes.UserQuotaEnabled
            case (None, Some(c)) => quotaTypesEnabled |= QuotaTypes.ClientIdQuotaEnabled
            case (None, None) =>
          }
        case None =>
          logger.info(s"Removing ${apiKey} quota for ${userInfo}${clientIdInfo}")
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
            val metricConfigEntity = quotaEntity(sanitizedUser.getOrElse(""), clientId.getOrElse(""))
            val newQuota = metricConfigEntity.quota
            logger.info(s"Sensor for ${userInfo}${clientIdInfo} already exists. Changing quota to ${newQuota.bound()} in MetricConfig")
            metric.config(getQuotaMetricConfig(newQuota))
          }
      } else {
          allMetrics.filterKeys(n => n.name == quotaMetricName.name && n.group == quotaMetricName.group).foreach {
            case (metricName, metric) =>
              val userTag = if (metricName.tags.containsKey("user")) metricName.tags.get("user") else ""
              val clientIdTag = if (metricName.tags.containsKey("client-id")) metricName.tags.get("client-id") else ""
              val metricConfigEntity = quotaEntity(userTag, clientIdTag)
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

  private def clientRateMetricName(sanitizedUser: String, clientId: String): MetricName = {
    metrics.metricName("byte-rate", apiKey.toString,
                   "Tracking byte-rate per user/client-id",
                   "user", sanitizedUser,
                   "client-id", clientId)
  }

  private def throttleMetricName(quotaEntity: QuotaEntity): MetricName = {
    metrics.metricName("throttle-time",
                       apiKey.toString,
                       "Tracking average throttle-time per user/client-id",
                       "user", quotaEntity.sanitizedUser,
                       "client-id", quotaEntity.clientId)
  }

  def shutdown() = {
    throttledRequestReaper.shutdown()
  }
}
