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

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{Metrics, Sensor, MeasurableStat, MetricConfig}

/**
  * Class which centralises the logic for creating/accessing sensors.
  * The quota can be updated by wrapping it in the passed MetricConfig
  *
  * The later arguments are passed as methods as they are only called when the sensor is instantiated.
  */
class SensorAccess {

  def getOrCreate(sensorName: String, expirationTime: Long, lock: ReentrantReadWriteLock, metrics: Metrics, metricName: () => MetricName, config: () => MetricConfig, measure: () => MeasurableStat): Sensor = {
    var sensor: Sensor = null

    /* Acquire the read lock to fetch the sensor. It is safe to call getSensor from multiple threads.
     * The read lock allows a thread to create a sensor in isolation. The thread creating the sensor
     * will acquire the write lock and prevent the sensors from being read while they are being created.
     * It should be sufficient to simply check if the sensor is null without acquiring a read lock but the
     * sensor being present doesn't mean that it is fully initialized i.e. all the Metrics may not have been added.
     * This read lock waits until the writer thread has released its lock i.e. fully initialized the sensor
     * at which point it is safe to read
     */
    lock.readLock().lock()
    try {
      sensor = metrics.getSensor(sensorName)
    }
    finally {
      lock.readLock().unlock()
    }

    /* If the sensor is null, try to create it else return the existing sensor
     * The sensor can be null, hence the null checks
     */
    if (sensor == null) {
      /* Acquire a write lock because the sensor may not have been created and we only want one thread to create it.
       * Note that multiple threads may acquire the write lock if they all see a null sensor initially
       * In this case, the writer checks the sensor after acquiring the lock again.
       * This is safe from Double Checked Locking because the references are read
       * after acquiring read locks and hence they cannot see a partially published reference
       */
      lock.writeLock().lock()
      try {
        // Set the var for both sensors in case another thread has won the race to acquire the write lock. This will
        // ensure that we initialise `ClientSensors` with non-null parameters.
        sensor = metrics.getSensor(sensorName)
        if (sensor == null) {
          sensor = metrics.sensor(sensorName, config(), expirationTime)
          sensor.add(metricName(), measure())
        }
      } finally {
        lock.writeLock().unlock()
      }
    }
    sensor
  }
}
