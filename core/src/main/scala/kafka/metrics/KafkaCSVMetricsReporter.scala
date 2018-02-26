/**
 *
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics

import com.yammer.metrics.Metrics
import java.io.File
import java.nio.file.Files

import com.yammer.metrics.reporting.CsvReporter
import java.util.concurrent.TimeUnit

import kafka.utils.{Logging, VerifiableProperties}
import org.apache.kafka.common.utils.Utils

private trait KafkaCSVMetricsReporterMBean extends KafkaMetricsReporterMBean

private class KafkaCSVMetricsReporter extends KafkaMetricsReporter
                              with KafkaCSVMetricsReporterMBean
                              with Logging {

  private var csvDir: File = null
  private var underlying: CsvReporter = null
  private var running = false
  private var initialized = false


  override def getMBeanName = "kafka:type=kafka.metrics.KafkaCSVMetricsReporter"


  override def init(props: VerifiableProperties) {
    synchronized {
      if (!initialized) {
        val metricsConfig = new KafkaMetricsConfig(props)
        csvDir = new File(props.getString("kafka.csv.metrics.dir", "kafka_metrics"))
        Utils.delete(csvDir)
        Files.createDirectories(csvDir.toPath())
        underlying = new CsvReporter(Metrics.defaultRegistry(), csvDir)
        if (props.getBoolean("kafka.csv.metrics.reporter.enabled", default = false)) {
          initialized = true
          startReporter(metricsConfig.pollingIntervalSecs)
        }
      }
    }
  }


  override def startReporter(pollingPeriodSecs: Long) {
    synchronized {
      if (initialized && !running) {
        underlying.start(pollingPeriodSecs, TimeUnit.SECONDS)
        running = true
        info("Started Kafka CSV metrics reporter with polling period %d seconds".format(pollingPeriodSecs))
      }
    }
  }


  override def stopReporter() {
    synchronized {
      if (initialized && running) {
        underlying.shutdown()
        running = false
        info("Stopped Kafka CSV metrics reporter")
        underlying = new CsvReporter(Metrics.defaultRegistry(), csvDir)
      }
    }
  }

}

