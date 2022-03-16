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

import java.{lang, util}
import java.util.concurrent.{ConcurrentHashMap, DelayQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.network.RequestChannel
import kafka.network.RequestChannel._
import kafka.server.ClientQuotaManager._
import kafka.utils.{Logging, QuotaUtils, ShutdownableThread}
import org.apache.kafka.common.{Cluster, MetricName}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.{Avg, CumulativeSum, Rate}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}
import org.apache.kafka.server.quota.{ClientQuotaCallback, ClientQuotaEntity, ClientQuotaType}

import scala.jdk.CollectionConverters._

/**
 * Represents the sensors aggregated per client
 * @param metricTags Quota metric tags for the client
 * @param quotaSensor @Sensor that tracks the quota
 * @param throttleTimeSensor @Sensor that tracks the throttle time
 */
case class ClientSensors(metricTags: Map[String, String], quotaSensor: Sensor, throttleTimeSensor: Sensor)

/**
 * Configuration settings for quota management
 * @param numQuotaSamples The number of samples to retain in memory
 * @param quotaWindowSizeSeconds The time span of each sample
 *
 */
case class ClientQuotaManagerConfig(numQuotaSamples: Int =
                                        ClientQuotaManagerConfig.DefaultNumQuotaSamples,
                                    quotaWindowSizeSeconds: Int =
                                        ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds)

object ClientQuotaManagerConfig {
  // Always have 10 whole windows + 1 current window
  val DefaultNumQuotaSamples = 11
  val DefaultQuotaWindowSizeSeconds = 1
}

object QuotaTypes {
  val NoQuotas = 0
  val ClientIdQuotaEnabled = 1
  val UserQuotaEnabled = 2
  val UserClientIdQuotaEnabled = 4
  val CustomQuotas = 8 // No metric update optimizations are used with custom quotas
}

object ClientQuotaManager {
  // Purge sensors after 1 hour of inactivity
  val InactiveSensorExpirationTimeSeconds = 3600

  val DefaultClientIdQuotaEntity = KafkaQuotaEntity(None, Some(DefaultClientIdEntity))
  val DefaultUserQuotaEntity = KafkaQuotaEntity(Some(DefaultUserEntity), None)
  val DefaultUserClientIdQuotaEntity = KafkaQuotaEntity(Some(DefaultUserEntity), Some(DefaultClientIdEntity))

  sealed trait BaseUserEntity extends ClientQuotaEntity.ConfigEntity

  case class UserEntity(sanitizedUser: String) extends BaseUserEntity {
    override def entityType: ClientQuotaEntity.ConfigEntityType = ClientQuotaEntity.ConfigEntityType.USER
    override def name: String = Sanitizer.desanitize(sanitizedUser)
    override def toString: String = s"user $sanitizedUser"
  }

  case class ClientIdEntity(clientId: String) extends ClientQuotaEntity.ConfigEntity {
    override def entityType: ClientQuotaEntity.ConfigEntityType = ClientQuotaEntity.ConfigEntityType.CLIENT_ID
    override def name: String = clientId
    override def toString: String = s"client-id $clientId"
  }

  case object DefaultUserEntity extends BaseUserEntity {
    override def entityType: ClientQuotaEntity.ConfigEntityType = ClientQuotaEntity.ConfigEntityType.DEFAULT_USER
    override def name: String = ConfigEntityName.Default
    override def toString: String = "default user"
  }

  case object DefaultClientIdEntity extends ClientQuotaEntity.ConfigEntity {
    override def entityType: ClientQuotaEntity.ConfigEntityType = ClientQuotaEntity.ConfigEntityType.DEFAULT_CLIENT_ID
    override def name: String = ConfigEntityName.Default
    override def toString: String = "default client-id"
  }

  case class KafkaQuotaEntity(userEntity: Option[BaseUserEntity],
                              clientIdEntity: Option[ClientQuotaEntity.ConfigEntity]) extends ClientQuotaEntity {
    override def configEntities: util.List[ClientQuotaEntity.ConfigEntity] =
      (userEntity.toList ++ clientIdEntity.toList).asJava

    def sanitizedUser: String = userEntity.map {
      case entity: UserEntity => entity.sanitizedUser
      case DefaultUserEntity => ConfigEntityName.Default
    }.getOrElse("")

    def clientId: String = clientIdEntity.map(_.name).getOrElse("")

    override def toString: String = {
      val user = userEntity.map(_.toString).getOrElse("")
      val clientId = clientIdEntity.map(_.toString).getOrElse("")
      s"$user $clientId".trim
    }
  }

  object DefaultTags {
    val User = "user"
    val ClientId = "client-id"
  }

  /**
   * This calculates the amount of time needed to bring the metric within quota
   * assuming that no new metrics are recorded.
   *
   * Basically, if O is the observed rate and T is the target rate over a window of W, to bring O down to T,
   * we need to add a delay of X to W such that O * W / (W + X) = T.
   * Solving for X, we get X = (O - T)/T * W.
   */
  def throttleTime(e: QuotaViolationException, timeMs: Long): Long = {
    val difference = e.value - e.bound
    // Use the precise window used by the rate calculation
    val throttleTimeMs = difference / e.bound * windowSize(e.metric, timeMs)
    Math.round(throttleTimeMs)
  }

  private def windowSize(metric: KafkaMetric, timeMs: Long): Long =
    measurableAsRate(metric.metricName, metric.measurable).windowSize(metric.config, timeMs)

  // Casting to Rate because we only use Rate in Quota computation
  private def measurableAsRate(name: MetricName, measurable: Measurable): Rate = {
    measurable match {
      case r: Rate => r
      case _ => throw new IllegalArgumentException(s"Metric $name is not a Rate metric, value $measurable")
    }
  }
}

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
 * @param threadNamePrefix The thread prefix to use
 * @param clientQuotaCallback An optional @ClientQuotaCallback
 */
class ClientQuotaManager(private val config: ClientQuotaManagerConfig,
                         private val metrics: Metrics,
                         private val quotaType: QuotaType,
                         private val time: Time,
                         private val threadNamePrefix: String,
                         private val clientQuotaCallback: Option[ClientQuotaCallback] = None) extends Logging {

  private val lock = new ReentrantReadWriteLock()
  private val sensorAccessor = new SensorAccess(lock, metrics)
  private val quotaCallback = clientQuotaCallback.getOrElse(new DefaultQuotaCallback)
  private val clientQuotaType = QuotaType.toClientQuotaType(quotaType)

  @volatile
  private var quotaTypesEnabled = clientQuotaCallback match {
    case Some(_) => QuotaTypes.CustomQuotas
    case None => QuotaTypes.NoQuotas
  }

  private val delayQueueSensor = metrics.sensor(quotaType.toString + "-delayQueue")
  delayQueueSensor.add(metrics.metricName("queue-size", quotaType.toString,
    "Tracks the size of the delay queue"), new CumulativeSum())

  private val delayQueue = new DelayQueue[ThrottledChannel]()
  private[server] val throttledChannelReaper = new ThrottledChannelReaper(delayQueue, threadNamePrefix)
  start() // Use start method to keep spotbugs happy
  private def start(): Unit = {
    throttledChannelReaper.start()
  }

  /**
   * Reaper thread that triggers channel unmute callbacks on all throttled channels
   * @param delayQueue DelayQueue to dequeue from
   */
  class ThrottledChannelReaper(delayQueue: DelayQueue[ThrottledChannel], prefix: String) extends ShutdownableThread(
    s"${prefix}ThrottledChannelReaper-$quotaType", false) {

    override def doWork(): Unit = {
      val throttledChannel: ThrottledChannel = delayQueue.poll(1, TimeUnit.SECONDS)
      if (throttledChannel != null) {
        // Decrement the size of the delay queue
        delayQueueSensor.record(-1)
        // Notify the socket server that throttling is done for this channel, so that it can try to unmute the channel.
        throttledChannel.notifyThrottlingDone()
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
   * See {recordAndGetThrottleTimeMs}.
   */
  def maybeRecordAndGetThrottleTimeMs(request: RequestChannel.Request, value: Double, timeMs: Long): Int = {
    maybeRecordAndGetThrottleTimeMs(request.session, request.header.clientId, value, timeMs)
  }

  /**
   * See {recordAndGetThrottleTimeMs}.
   */
  def maybeRecordAndGetThrottleTimeMs(session: Session, clientId: String, value: Double, timeMs: Long): Int = {
    // Record metrics only if quotas are enabled.
    if (quotasEnabled) {
      recordAndGetThrottleTimeMs(session, clientId, value, timeMs)
    } else {
      0
    }
  }

  /**
   * Records that a user/clientId accumulated or would like to accumulate the provided amount at the
   * the specified time, returns throttle time in milliseconds.
   *
   * @param session The session from which the user is extracted
   * @param clientId The client id
   * @param value The value to accumulate
   * @param timeMs The time at which to accumulate the value
   * @return The throttle time in milliseconds defines as the time to wait until the average
   *         rate gets back to the defined quota
   */
  def recordAndGetThrottleTimeMs(session: Session, clientId: String, value: Double, timeMs: Long): Int = {
    val clientSensors = getOrCreateQuotaSensors(session, clientId)
    try {
      clientSensors.quotaSensor.record(value, timeMs, true)
      0
    } catch {
      case e: QuotaViolationException =>
        val throttleTimeMs = throttleTime(e, timeMs).toInt
        debug(s"Quota violated for sensor (${clientSensors.quotaSensor.name}). Delay time: ($throttleTimeMs)")
        throttleTimeMs
    }
  }

  /**
   * Records that a user/clientId changed some metric being throttled without checking for
   * quota violation. The aggregate value will subsequently be used for throttling when the
   * next request is processed.
   */
  def recordNoThrottle(session: Session, clientId: String, value: Double): Unit = {
    val clientSensors = getOrCreateQuotaSensors(session, clientId)
    clientSensors.quotaSensor.record(value, time.milliseconds(), false)
  }

  /**
   * "Unrecord" the given value that has already been recorded for the given user/client by recording a negative value
   * of the same quantity.
   *
   * For a throttled fetch, the broker should return an empty response and thus should not record the value. Ideally,
   * we would like to compute the throttle time before actually recording the value, but the current Sensor code
   * couples value recording and quota checking very tightly. As a workaround, we will unrecord the value for the fetch
   * in case of throttling. Rate keeps the sum of values that fall in each time window, so this should bring the
   * overall sum back to the previous value.
   */
  def unrecordQuotaSensor(request: RequestChannel.Request, value: Double, timeMs: Long): Unit = {
    val clientSensors = getOrCreateQuotaSensors(request.session, request.header.clientId)
    clientSensors.quotaSensor.record(value * (-1), timeMs, false)
  }

  /**
   * Returns maximum value that could be recorded without guaranteed throttling.
   * Recording any larger value will always be throttled, even if no other values were recorded in the quota window.
   * This is used for deciding the maximum bytes that can be fetched at once
   */
  def getMaxValueInQuotaWindow(session: Session, clientId: String): Double = {
    if (quotasEnabled) {
      val clientSensors = getOrCreateQuotaSensors(session, clientId)
      Option(quotaCallback.quotaLimit(clientQuotaType, clientSensors.metricTags.asJava))
        .map(_.toDouble * (config.numQuotaSamples - 1) * config.quotaWindowSizeSeconds)
        .getOrElse(Double.MaxValue)
    } else {
      Double.MaxValue
    }
  }

  /**
   * Throttle a client by muting the associated channel for the given throttle time.
   *
   * @param request client request
   * @param throttleTimeMs Duration in milliseconds for which the channel is to be muted.
   * @param throttleCallback Callback for channel throttling
   */
  def throttle(
    request: RequestChannel.Request,
    throttleCallback: ThrottleCallback,
    throttleTimeMs: Int
  ): Unit = {
    if (throttleTimeMs > 0) {
      val clientSensors = getOrCreateQuotaSensors(request.session, request.headerForLoggingOrThrottling().clientId)
      clientSensors.throttleTimeSensor.record(throttleTimeMs)
      val throttledChannel = new ThrottledChannel(time, throttleTimeMs, throttleCallback)
      delayQueue.add(throttledChannel)
      delayQueueSensor.record()
      debug("Channel throttled for sensor (%s). Delay time: (%d)".format(clientSensors.quotaSensor.name(), throttleTimeMs))
    }
  }

  /**
   * Returns the quota for the client with the specified (non-encoded) user principal and client-id.
   *
   * Note: this method is expensive, it is meant to be used by tests only
   */
  def quota(user: String, clientId: String): Quota = {
    val userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user)
    quota(userPrincipal, clientId)
  }

  /**
   * Returns the quota for the client with the specified user principal and client-id.
   *
   * Note: this method is expensive, it is meant to be used by tests only
   */
  def quota(userPrincipal: KafkaPrincipal, clientId: String): Quota = {
    val metricTags = quotaCallback.quotaMetricTags(clientQuotaType, userPrincipal, clientId)
    Quota.upperBound(quotaLimit(metricTags))
  }

  private def quotaLimit(metricTags: util.Map[String, String]): Double = {
    Option(quotaCallback.quotaLimit(clientQuotaType, metricTags)).map(_.toDouble).getOrElse(Long.MaxValue)
  }

  /**
   * This calculates the amount of time needed to bring the metric within quota
   * assuming that no new metrics are recorded.
   *
   * See {QuotaUtils.throttleTime} for the details.
   */
  protected def throttleTime(e: QuotaViolationException, timeMs: Long): Long = {
    QuotaUtils.throttleTime(e, timeMs)
  }

  /**
   * This function either returns the sensors for a given client id or creates them if they don't exist
   * First sensor of the tuple is the quota enforcement sensor. Second one is the throttle time sensor
   */
  def getOrCreateQuotaSensors(session: Session, clientId: String): ClientSensors = {
    // Use cached sanitized principal if using default callback
    val metricTags = quotaCallback match {
      case callback: DefaultQuotaCallback => callback.quotaMetricTags(session.sanitizedUser, clientId)
      case _ => quotaCallback.quotaMetricTags(clientQuotaType, session.principal, clientId).asScala.toMap
    }
    // Names of the sensors to access
    val sensors = ClientSensors(
      metricTags,
      sensorAccessor.getOrCreate(
        getQuotaSensorName(metricTags),
        ClientQuotaManager.InactiveSensorExpirationTimeSeconds,
        registerQuotaMetrics(metricTags)
      ),
      sensorAccessor.getOrCreate(
        getThrottleTimeSensorName(metricTags),
        ClientQuotaManager.InactiveSensorExpirationTimeSeconds,
        sensor => sensor.add(throttleMetricName(metricTags), new Avg)
      )
    )
    if (quotaCallback.quotaResetRequired(clientQuotaType))
      updateQuotaMetricConfigs()
    sensors
  }

  protected def registerQuotaMetrics(metricTags: Map[String, String])(sensor: Sensor): Unit = {
    sensor.add(
      clientQuotaMetricName(metricTags),
      new Rate,
      getQuotaMetricConfig(metricTags)
    )
  }

  private def metricTagsToSensorSuffix(metricTags: Map[String, String]): String =
    metricTags.values.mkString(":")

  private def getThrottleTimeSensorName(metricTags: Map[String, String]): String =
    s"${quotaType}ThrottleTime-${metricTagsToSensorSuffix(metricTags)}"

  private def getQuotaSensorName(metricTags: Map[String, String]): String =
    s"$quotaType-${metricTagsToSensorSuffix(metricTags)}"

  protected def getQuotaMetricConfig(metricTags: Map[String, String]): MetricConfig = {
    getQuotaMetricConfig(quotaLimit(metricTags.asJava))
  }

  private def getQuotaMetricConfig(quotaLimit: Double): MetricConfig = {
    new MetricConfig()
      .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
      .samples(config.numQuotaSamples)
      .quota(new Quota(quotaLimit, true))
  }

  protected def getOrCreateSensor(sensorName: String, metricName: MetricName): Sensor = {
    sensorAccessor.getOrCreate(
      sensorName,
      ClientQuotaManager.InactiveSensorExpirationTimeSeconds,
      sensor => sensor.add(metricName, new Rate)
    )
  }

  /**
   * Overrides quotas for <user>, <client-id> or <user, client-id> or the dynamic defaults
   * for any of these levels.
   *
   * @param sanitizedUser     user to override if quota applies to <user> or <user, client-id>
   * @param clientId          client to override if quota applies to <client-id> or <user, client-id>
   * @param sanitizedClientId sanitized client ID to override if quota applies to <client-id> or <user, client-id>
   * @param quota             custom quota to apply or None if quota override is being removed
   */
  def updateQuota(sanitizedUser: Option[String], clientId: Option[String], sanitizedClientId: Option[String], quota: Option[Quota]): Unit = {
    /*
     * Acquire the write lock to apply changes in the quota objects.
     * This method changes the quota in the overriddenQuota map and applies the update on the actual KafkaMetric object (if it exists).
     * If the KafkaMetric hasn't been created, the most recent value will be used from the overriddenQuota map.
     * The write lock prevents quota update and creation at the same time. It also guards against concurrent quota change
     * notifications
     */
    lock.writeLock().lock()
    try {
      val userEntity = sanitizedUser.map {
        case ConfigEntityName.Default => DefaultUserEntity
        case user => UserEntity(user)
      }
      val clientIdEntity = sanitizedClientId.map {
        case ConfigEntityName.Default => DefaultClientIdEntity
        case _ => ClientIdEntity(clientId.getOrElse(throw new IllegalStateException("Client-id not provided")))
      }
      val quotaEntity = KafkaQuotaEntity(userEntity, clientIdEntity)

      if (userEntity.nonEmpty) {
        if (quotaEntity.clientIdEntity.nonEmpty)
          quotaTypesEnabled |= QuotaTypes.UserClientIdQuotaEnabled
        else
          quotaTypesEnabled |= QuotaTypes.UserQuotaEnabled
      } else if (clientIdEntity.nonEmpty)
        quotaTypesEnabled |= QuotaTypes.ClientIdQuotaEnabled

      quota match {
        case Some(newQuota) => quotaCallback.updateQuota(clientQuotaType, quotaEntity, newQuota.bound)
        case None => quotaCallback.removeQuota(clientQuotaType, quotaEntity)
      }
      val updatedEntity = if (userEntity.contains(DefaultUserEntity) || clientIdEntity.contains(DefaultClientIdEntity))
        None // more than one entity may need updating, so `updateQuotaMetricConfigs` will go through all metrics
      else
        Some(quotaEntity)
      updateQuotaMetricConfigs(updatedEntity)

    } finally {
      lock.writeLock().unlock()
    }
  }

  /**
   * Updates metrics configs. This is invoked when quota configs are updated in ZooKeeper
   * or when partitions leaders change and custom callbacks that implement partition-based quotas
   * have updated quotas.
   *
   * @param updatedQuotaEntity If set to one entity and quotas have only been enabled at one
   *    level, then an optimized update is performed with a single metric update. If None is provided,
   *    or if custom callbacks are used or if multi-level quotas have been enabled, all metric configs
   *    are checked and updated if required.
   */
  def updateQuotaMetricConfigs(updatedQuotaEntity: Option[KafkaQuotaEntity] = None): Unit = {
    val allMetrics = metrics.metrics()

    // If using custom quota callbacks or if multiple-levels of quotas are defined or
    // if this is a default quota update, traverse metrics to find all affected values.
    // Otherwise, update just the single matching one.
    val singleUpdate = quotaTypesEnabled match {
      case QuotaTypes.NoQuotas | QuotaTypes.ClientIdQuotaEnabled | QuotaTypes.UserQuotaEnabled | QuotaTypes.UserClientIdQuotaEnabled =>
        updatedQuotaEntity.nonEmpty
      case _ => false
    }
    if (singleUpdate) {
      val quotaEntity = updatedQuotaEntity.getOrElse(throw new IllegalStateException("Quota entity not specified"))
      val user = quotaEntity.sanitizedUser
      val clientId = quotaEntity.clientId
      val metricTags = Map(DefaultTags.User -> user, DefaultTags.ClientId -> clientId)

      val quotaMetricName = clientQuotaMetricName(metricTags)
      // Change the underlying metric config if the sensor has been created
      val metric = allMetrics.get(quotaMetricName)
      if (metric != null) {
        Option(quotaLimit(metricTags.asJava)).foreach { newQuota =>
          info(s"Sensor for $quotaEntity already exists. Changing quota to $newQuota in MetricConfig")
          metric.config(getQuotaMetricConfig(newQuota))
        }
      }
    } else {
      val quotaMetricName = clientQuotaMetricName(Map.empty)
      allMetrics.forEach { (metricName, metric) =>
        if (metricName.name == quotaMetricName.name && metricName.group == quotaMetricName.group) {
          val metricTags = metricName.tags
          Option(quotaLimit(metricTags)).foreach { newQuota =>
            if (newQuota != metric.config.quota.bound) {
              info(s"Sensor for quota-id $metricTags already exists. Setting quota to $newQuota in MetricConfig")
              metric.config(getQuotaMetricConfig(newQuota))
            }
          }
        }
      }
    }
  }

  /**
   * Returns the MetricName of the metric used for the quota. The name is used to create the
   * metric but also to find the metric when the quota is changed.
   */
  protected def clientQuotaMetricName(quotaMetricTags: Map[String, String]): MetricName = {
    metrics.metricName("byte-rate", quotaType.toString,
      "Tracking byte-rate per user/client-id",
      quotaMetricTags.asJava)
  }

  private def throttleMetricName(quotaMetricTags: Map[String, String]): MetricName = {
    metrics.metricName("throttle-time",
      quotaType.toString,
      "Tracking average throttle-time per user/client-id",
      quotaMetricTags.asJava)
  }

  def initiateShutdown(): Unit = {
    throttledChannelReaper.initiateShutdown()
    // improve shutdown time by waking up any ShutdownableThread(s) blocked on poll by sending a no-op
    delayQueue.add(new ThrottledChannel(time, 0, new ThrottleCallback {
      override def startThrottling(): Unit = {}
      override def endThrottling(): Unit = {}
    }))
  }

  def shutdown(): Unit = {
    initiateShutdown()
    throttledChannelReaper.awaitShutdown()
  }

  class DefaultQuotaCallback extends ClientQuotaCallback {
    private val overriddenQuotas = new ConcurrentHashMap[ClientQuotaEntity, Quota]()

    override def configure(configs: util.Map[String, _]): Unit = {}

    override def quotaMetricTags(quotaType: ClientQuotaType, principal: KafkaPrincipal, clientId: String): util.Map[String, String] = {
      quotaMetricTags(Sanitizer.sanitize(principal.getName), clientId).asJava
    }

    override def quotaLimit(quotaType: ClientQuotaType, metricTags: util.Map[String, String]): lang.Double = {
      val sanitizedUser = metricTags.get(DefaultTags.User)
      val clientId = metricTags.get(DefaultTags.ClientId)
      var quota: Quota = null

      if (sanitizedUser != null && clientId != null) {
        val userEntity = Some(UserEntity(sanitizedUser))
        val clientIdEntity = Some(ClientIdEntity(clientId))
        if (!sanitizedUser.isEmpty && !clientId.isEmpty) {
          // /config/users/<user>/clients/<client-id>
          quota = overriddenQuotas.get(KafkaQuotaEntity(userEntity, clientIdEntity))
          if (quota == null) {
            // /config/users/<user>/clients/<default>
            quota = overriddenQuotas.get(KafkaQuotaEntity(userEntity, Some(DefaultClientIdEntity)))
          }
          if (quota == null) {
            // /config/users/<default>/clients/<client-id>
            quota = overriddenQuotas.get(KafkaQuotaEntity(Some(DefaultUserEntity), clientIdEntity))
          }
          if (quota == null) {
            // /config/users/<default>/clients/<default>
            quota = overriddenQuotas.get(DefaultUserClientIdQuotaEntity)
          }
        } else if (!sanitizedUser.isEmpty) {
          // /config/users/<user>
          quota = overriddenQuotas.get(KafkaQuotaEntity(userEntity, None))
          if (quota == null) {
            // /config/users/<default>
            quota = overriddenQuotas.get(DefaultUserQuotaEntity)
          }
        } else if (!clientId.isEmpty) {
          // /config/clients/<client-id>
          quota = overriddenQuotas.get(KafkaQuotaEntity(None, clientIdEntity))
          if (quota == null) {
            // /config/clients/<default>
            quota = overriddenQuotas.get(DefaultClientIdQuotaEntity)
          }
        }
      }
      if (quota == null) null else quota.bound
    }

    override def updateClusterMetadata(cluster: Cluster): Boolean = {
      // Default quota callback does not use any cluster metadata
      false
    }

    override def updateQuota(quotaType: ClientQuotaType, entity: ClientQuotaEntity, newValue: Double): Unit = {
      val quotaEntity = entity.asInstanceOf[KafkaQuotaEntity]
      info(s"Changing $quotaType quota for $quotaEntity to $newValue")
      overriddenQuotas.put(quotaEntity, new Quota(newValue, true))
    }

    override def removeQuota(quotaType: ClientQuotaType, entity: ClientQuotaEntity): Unit = {
      val quotaEntity = entity.asInstanceOf[KafkaQuotaEntity]
      info(s"Removing $quotaType quota for $quotaEntity")
      overriddenQuotas.remove(quotaEntity)
    }

    override def quotaResetRequired(quotaType: ClientQuotaType): Boolean = false

    def quotaMetricTags(sanitizedUser: String, clientId: String) : Map[String, String] = {
      val (userTag, clientIdTag) = quotaTypesEnabled match {
        case QuotaTypes.NoQuotas | QuotaTypes.ClientIdQuotaEnabled =>
          ("", clientId)
        case QuotaTypes.UserQuotaEnabled =>
          (sanitizedUser, "")
        case QuotaTypes.UserClientIdQuotaEnabled =>
          (sanitizedUser, clientId)
        case _ =>
          val userEntity = Some(UserEntity(sanitizedUser))
          val clientIdEntity = Some(ClientIdEntity(clientId))

          var metricTags = (sanitizedUser, clientId)
          // 1) /config/users/<user>/clients/<client-id>
          if (!overriddenQuotas.containsKey(KafkaQuotaEntity(userEntity, clientIdEntity))) {
            // 2) /config/users/<user>/clients/<default>
            metricTags = (sanitizedUser, clientId)
            if (!overriddenQuotas.containsKey(KafkaQuotaEntity(userEntity, Some(DefaultClientIdEntity)))) {
              // 3) /config/users/<user>
              metricTags = (sanitizedUser, "")
              if (!overriddenQuotas.containsKey(KafkaQuotaEntity(userEntity, None))) {
                // 4) /config/users/<default>/clients/<client-id>
                metricTags = (sanitizedUser, clientId)
                if (!overriddenQuotas.containsKey(KafkaQuotaEntity(Some(DefaultUserEntity), clientIdEntity))) {
                  // 5) /config/users/<default>/clients/<default>
                  metricTags = (sanitizedUser, clientId)
                  if (!overriddenQuotas.containsKey(DefaultUserClientIdQuotaEntity)) {
                    // 6) /config/users/<default>
                    metricTags = (sanitizedUser, "")
                    if (!overriddenQuotas.containsKey(DefaultUserQuotaEntity)) {
                      // 7) /config/clients/<client-id>
                      // 8) /config/clients/<default>
                      metricTags = ("", clientId)
                    }
                  }
                }
              }
            }
          }
          metricTags
      }
      Map(DefaultTags.User -> userTag, DefaultTags.ClientId -> clientIdTag)
    }

    override def close(): Unit = {}
  }
}
