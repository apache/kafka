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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import kafka.common.TopicAndPartition
import kafka.server.Constants._
import kafka.server.ReplicationQuotaManagerConfig._
import kafka.utils.CoreUtils._
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.kafka.common.metrics.stats.{Rate, SimpleRate}
import org.apache.kafka.common.utils.Time

/**
  * Configuration settings for quota management
  *
  * @param quotaBytesPerSecondDefault The default bytes per second quota allocated to internal replication
  * @param numQuotaSamples            The number of samples to retain in memory
  * @param quotaWindowSizeSeconds     The time span of each sample
  *
  */
case class ReplicationQuotaManagerConfig(quotaBytesPerSecondDefault: Long = QuotaBytesPerSecondDefault,
                                         numQuotaSamples: Int = DefaultNumQuotaSamples,
                                         quotaWindowSizeSeconds: Int = DefaultQuotaWindowSizeSeconds)

object ReplicationQuotaManagerConfig {
  val QuotaBytesPerSecondDefault = Long.MaxValue
  // Always have 10 whole windows + 1 current window
  val DefaultNumQuotaSamples = 11
  val DefaultQuotaWindowSizeSeconds = 1
  // Purge sensors after 1 hour of inactivity
  val InactiveSensorExpirationTimeSeconds = 3600
}

trait ReadOnlyQuota {
  def bound(): Int

  def isThrottled(topicAndPartition: TopicAndPartition): Boolean

  def isQuotaExceededBy(bytes: Int): Boolean
}

object Constants {
  val allReplicas = Seq[Int](-1)
}

/**
  * Tracks replication metrics and comparing them to any quotas for throttled partitions.
  *
  * @param config          The quota configs
  * @param metrics         The Metrics instance
  * @param replicationType The name / key for this quota manager, typically leader or follower
  * @param time            Time object to use
  */
class ReplicationQuotaManager(val config: ReplicationQuotaManagerConfig,
                              private val metrics: Metrics,
                              private val replicationType: String,
                              private val time: Time) extends Logging with ReadOnlyQuota {

  private val lock = new ReentrantReadWriteLock()
  private val throttledPartitions = new ConcurrentHashMap[String, Seq[Int]]()
  private var quota: Quota = null
  private val sensor = createSensor(quota, replicationType)

  private def rateMetricName(): MetricName = metrics.metricName("byte-rate", replicationType, s"Tracking byte-rate for $replicationType")

  /**
    * Update the quota
    *
    * @param quota
    */
  def updateQuota(quota: Quota) = {
    inWriteLock(lock) {
      this.quota = quota
      if (metrics.metrics.containsKey(rateMetricName))
        metrics.metrics.get(rateMetricName).config(getQuotaMetricConfig(quota))
    }
  }

  /**
    * Retrieve the current quota object
    *
    * @return
    */
  def getQuota(): Quota = {
    inReadLock(lock) {
      quota
    }
  }

  /**
    * Check if the quota is currently exceeded
    *
    * @return
    */
  def isQuotaExceeded(): Boolean = isQuotaExceededBy(0)

  /**
    * Check if the quota will be exceeded by the passed value. This method does not record anything, but is equivalent to
    * increasing the quota by the passed value then checking if it has been exceeded.
    */
  override def isQuotaExceededBy(value: Int): Boolean = {
    try {
      sensor.checkQuotaWithDelta(value)
    } catch {
      case qve: QuotaViolationException =>
        logger.info("%s: Quota violated for sensor (%s), metric: (%s), metric-value: (%f), bound: (%f), proposedValue (%s)".format(replicationType, sensor.name(), qve.metricName, qve.value, qve.bound, value))
        return true
    }
    false
  }

  /**
    * Returns the bound of the configured quota
    *
    * @return
    */
  override def bound(): Int = {
    if (getQuota != null)
      getQuota.bound().toInt
    else
      Int.MaxValue
  }

  /**
    * Is the passed partition throttled by this ReplicationQuotaManager
    *
    * @param topicPartition the partition to check
    * @return
    */
  override def isThrottled(topicPartition: TopicAndPartition): Boolean = {
    val partitions = throttledPartitions.get(topicPartition.topic)
    if (partitions != null)
      partitions.contains(topicPartition.partition) || partitions == allReplicas
    else false
  }

  /**
    * Add the passed value to the throttled rate. This method ignores the quota with
    * the value being added to the rate even if the quota is exceeded
    *
    * @param value
    */
  def record(value: Int) = {
    logger.info("Updating sensor with throttled bytes for " + replicationType + " value " + value)
    try {
      sensor.record(value)
    } catch {
      case qve: QuotaViolationException =>
        logger.info("record: Quota violated, but ignored, for sensor (%s), metric: (%s), metric-value: (%f), bound: (%f), recordedValue (%s)".format(sensor.name(), qve.metricName, qve.value, qve.bound, value))
      //TODO should we strap a metric around this violation?? as it means we're not hitting target.
    }
  }

  /**
    * Update the set of throttled partitions for this QuotaManager. The partitions passed, for
    * any single topic, will replace any previous
    *
    * @param topic
    * @param partitions the set of throttled partitions
    * @return
    */
  def markReplicasAsThrottled(topic: String, partitions: Seq[Int]): Unit = {
    throttledPartitions.put(topic, partitions)
  }

  /**
    * Mark all replicas for this topic as throttled
    *
    * @param topic
    * @return
    */
  def markReplicasAsThrottled(topic: String): Unit = {
    markReplicasAsThrottled(topic, allReplicas)
  }

  /**
    * Returns the current rate of throttled replication
    *
    * @return
    */
  def rate(): Double = sensor.metrics.get(0).value

  private def createSensor(quota: Quota, name: String): Sensor = {
    var sensor = metrics.getSensor(name)
    if (sensor == null) {
      sensor = metrics.sensor(replicationType, getQuotaMetricConfig(quota), InactiveSensorExpirationTimeSeconds)
      sensor.add(rateMetricName(), newRateInstance())
    } else
      warn(s"We should not be creating a replication quota sensor of the same type [$name] twice")
    sensor
  }

  private def getQuotaMetricConfig(quota: Quota): MetricConfig = {
    new MetricConfig()
      .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
      .samples(config.numQuotaSamples)
      .quota(quota)
  }

  protected def newRateInstance(): Rate = new SimpleRate()
}
