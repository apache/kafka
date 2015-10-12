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

import java.util.concurrent.{DelayQueue, TimeUnit}

import kafka.utils.{ShutdownableThread, Logging}
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Total, Rate, Avg}
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.utils.Time

/**
 * Represents the sensors aggregated per client
 * @param quotaSensor @Sensor that tracks the quota
 * @param throttleTimeSensor @Sensor that tracks the throttle time
 */
private case class ClientSensors(quotaSensor: Sensor, throttleTimeSensor: Sensor)

/**
 * Configuration settings for quota management
 * @param quotaBytesPerSecondDefault The default bytes per second quota allocated to any client
 * @param quotaBytesPerSecondOverrides The comma separated overrides per client. "c1=X,c2=Y"
 * @param numQuotaSamples The number of samples to retain in memory
 * @param quotaWindowSizeSeconds The time span of each sample
 *
 */
case class ClientQuotaManagerConfig(quotaBytesPerSecondDefault: Long =
                                        ClientQuotaManagerConfig.QuotaBytesPerSecondDefault,
                                    quotaBytesPerSecondOverrides: String =
                                        ClientQuotaManagerConfig.QuotaBytesPerSecondOverrides,
                                    numQuotaSamples: Int =
                                        ClientQuotaManagerConfig.DefaultNumQuotaSamples,
                                    quotaWindowSizeSeconds: Int =
                                        ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds)

object ClientQuotaManagerConfig {
  val QuotaBytesPerSecondDefault = Long.MaxValue
  val QuotaBytesPerSecondOverrides = ""
  // Always have 10 whole windows + 1 current window
  val DefaultNumQuotaSamples = 11
  val DefaultQuotaWindowSizeSeconds = 1
  val MaxThrottleTimeSeconds = 30
  // Purge sensors after 1 hour of inactivity
  val InactiveSensorExpirationTimeSeconds  = 3600
}

/**
 * Helper class that records per-client metrics. It is also responsible for maintaining Quota usage statistics
 * for all clients.
 * @param config @ClientQuotaManagerConfig quota configs
 * @param metrics @Metrics Metrics instance
 * @param apiKey API Key for the request
 * @param time @Time object to use
 */
class ClientQuotaManager(private val config: ClientQuotaManagerConfig,
                         private val metrics: Metrics,
                         private val apiKey: String,
                         private val time: Time) extends Logging {
  private val overriddenQuota = initQuotaMap(config.quotaBytesPerSecondOverrides)
  private val defaultQuota = Quota.lessThan(config.quotaBytesPerSecondDefault)
  private val lock = new ReentrantReadWriteLock()
  private val delayQueue = new DelayQueue[ThrottledResponse]()
  val throttledRequestReaper = new ThrottledRequestReaper(delayQueue)
  throttledRequestReaper.start()

  private val delayQueueSensor = metrics.sensor(apiKey + "-delayQueue")
  delayQueueSensor.add(new MetricName("queue-size",
                                      apiKey,
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
  def recordAndMaybeThrottle(clientId: String, value: Int, callback: Int => Unit): Int = {
    val clientSensors = getOrCreateQuotaSensors(clientId)
    var throttleTimeMs = 0
    try {
      clientSensors.quotaSensor.record(value)
      // trigger the callback immediately if quota is not violated
      callback(0)
    } catch {
      case qve: QuotaViolationException =>
        // Compute the delay
        val clientMetric = metrics.metrics().get(clientRateMetricName(clientId))
        throttleTimeMs = throttleTime(clientMetric, getQuotaMetricConfig(quota(clientId)))
        delayQueue.add(new ThrottledResponse(time, throttleTimeMs, callback))
        delayQueueSensor.record()
        // If delayed, add the element to the delayQueue
        logger.debug("Quota violated for sensor (%s). Delay time: (%d)".format(clientSensors.quotaSensor.name(), throttleTimeMs))
    }
    // If the request is not throttled, a throttleTime of 0 ms is recorded
    clientSensors.throttleTimeSensor.record(throttleTimeMs)
    throttleTimeMs
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

  /**
   * Returns the consumer quota for the specified clientId
   * @return
   */
  private[server] def quota(clientId: String): Quota = overriddenQuota.getOrElse(clientId, defaultQuota)

  /*
   * This function either returns the sensors for a given client id or creates them if they don't exist
   * First sensor of the tuple is the quota enforcement sensor. Second one is the throttle time sensor
   */
  private def getOrCreateQuotaSensors(clientId: String): ClientSensors = {

    // Names of the sensors to access
    val quotaSensorName = apiKey + "-" + clientId
    val throttleTimeSensorName = apiKey + "ThrottleTime-" + clientId
    var quotaSensor: Sensor = null
    var throttleTimeSensor: Sensor = null

    /* Acquire the read lock to fetch the sensors. It is safe to call getSensor from multiple threads.
     * The read lock allows a thread to create a sensor in isolation. The thread creating the sensor
     * will acquire the write lock and prevent the sensors from being read while they are being created.
     * It should be sufficient to simply check if the sensor is null without acquiring a read lock but the
     * sensor being present doesn't mean that it is fully initialized i.e. all the Metrics may not have been added.
     * This read lock waits until the writer thread has released its lock i.e. fully initialized the sensor
     * at which point it is safe to read
     */
    lock.readLock().lock()
    try {
      quotaSensor = metrics.getSensor(quotaSensorName)
      throttleTimeSensor = metrics.getSensor(throttleTimeSensorName)
    }
    finally {
      lock.readLock().unlock()
    }

    /* If the sensor is null, try to create it else return the created sensor
     * Also if quota sensor is null, the throttle time sensor must be null
     */
    if (quotaSensor == null) {
      /* Acquire a write lock because the sensor may not have been created and we only want one thread to create it.
       * Note that multiple threads may acquire the write lock if they all see a null sensor initially
       * In this case, the writer checks the sensor after acquiring the lock again.
       * This is safe from Double Checked Locking because the references are read
       * after acquiring read locks and hence they cannot see a partially published reference
       */
      lock.writeLock().lock()
      try {
        quotaSensor = metrics.getSensor(quotaSensorName)
        if (quotaSensor == null) {
          // create the throttle time sensor also. Use default metric config
          throttleTimeSensor = metrics.sensor(throttleTimeSensorName,
                                              null,
                                              ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds)
          throttleTimeSensor.add(new MetricName("throttle-time",
                                                apiKey,
                                                "Tracking average throttle-time per client",
                                                "client-id",
                                                clientId), new Avg())

          quotaSensor = metrics.sensor(quotaSensorName,
                                       getQuotaMetricConfig(quota(clientId)),
                                       ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds)
          quotaSensor.add(clientRateMetricName(clientId), new Rate())
        }
      } finally {
        lock.writeLock().unlock()
      }
    }
    // return the read or created sensors
    ClientSensors(quotaSensor, throttleTimeSensor)
  }

  private def getQuotaMetricConfig(quota: Quota): MetricConfig = {
    new MetricConfig()
            .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
            .samples(config.numQuotaSamples)
            .quota(quota)
  }

  /* Construct a Map of (clientId -> Quota)
   * The input config is specified as a comma-separated K=V pairs
   */
  private def initQuotaMap(input: String): Map[String, Quota] = {
    // If empty input, return an empty map
    if (input.trim.length == 0)
      Map[String, Quota]()
    else
      input.split(",").map(entry => {
        val trimmedEntry = entry.trim
        val pair: Array[String] = trimmedEntry.split("=")
        if (pair.length != 2)
          throw new IllegalArgumentException("Incorrectly formatted override entry (%s). Format is k1=v1,k2=v2".format(entry))
        pair(0) -> new Quota(pair(1).toDouble, true)
      }).toMap
  }

  private def clientRateMetricName(clientId: String): MetricName = {
    new MetricName("byte-rate", apiKey,
                   "Tracking byte-rate per client",
                   "client-id", clientId)
  }

  def shutdown() = {
    throttledRequestReaper.shutdown()
  }
}
