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

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.utils.AppInfoParser

object AppInfo extends KafkaMetricsGroup {
  private var isRegistered = false
  private val lock = new Object()

  def registerInfo(): Unit = {
    lock.synchronized {
      if (isRegistered) {
        return
      }
    }

    newGauge("Version",
      new Gauge[String] {
        def value = {
          AppInfoParser.getVersion()
        }
      })

    newGauge("CommitID",
      new Gauge[String] {
        def value = {
          AppInfoParser.getCommitId()
        }
      })

    lock.synchronized {
      isRegistered = true
    }

  }
}
