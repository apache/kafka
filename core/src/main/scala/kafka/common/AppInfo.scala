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

package kafka.common

import java.net.URL
import java.util.jar.{Attributes, Manifest}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup

object AppInfo extends KafkaMetricsGroup {
  private var isRegistered = false
  private val lock = new Object()

  def registerInfo(): Unit = {
    lock.synchronized {
      if (isRegistered) {
        return
      }
    }

    try {
      val clazz = AppInfo.getClass
      val className = clazz.getSimpleName + ".class"
      val classPath = clazz.getResource(className).toString
      if (!classPath.startsWith("jar")) {
        // Class not from JAR
        return
      }
      val manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF"

      val mf = new Manifest
      mf.read(new URL(manifestPath).openStream())
      val version = mf.getMainAttributes.get(new Attributes.Name("Version")).toString

      newGauge("Version",
        new Gauge[String] {
          def value = {
            version
          }
        })

      lock.synchronized {
        isRegistered = true
      }
    } catch {
      case e: Exception =>
        warn("Can't read Kafka version from MANIFEST.MF. Possible cause: %s".format(e))
    }
  }
}
