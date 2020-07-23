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

import java.util.concurrent.locks.ReadWriteLock

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{MeasurableStat, MetricConfig, Metrics, Sensor}

/**
  * Class which centralises the logic for creating/accessing sensors.
  * The quota can be updated by wrapping it in the passed MetricConfig
  *
  * The later arguments are passed as methods as they are only called when the sensor is instantiated.
  */
class SensorAccess(lock: ReadWriteLock, metrics: Metrics) {

  def getOrCreate(sensorName: String, expirationTime: Long,
                  metricName: => MetricName, config: => Option[MetricConfig], measure: => MeasurableStat): Sensor = {
    var sensor: Sensor = metrics.getSensor(sensorName) 

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
          var tempSensor: Sensor = metrics.sensor(sensorName, config.orNull, expirationTime)
          tempSensor.add(metricName, measure)
          sensor = tempSensor
        }
      } finally {
        lock.writeLock().unlock()
      }
    }
    sensor
  }
}
