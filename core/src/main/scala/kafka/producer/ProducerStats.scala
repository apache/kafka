/**
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
package kafka.producer

import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.TimeUnit
import kafka.utils.Pool

@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
class ProducerStats(clientId: String) extends KafkaMetricsGroup {
  val tags: Map[String, String] = Map("clientId" -> clientId)
  val serializationErrorRate = newMeter("SerializationErrorsPerSec", "errors", TimeUnit.SECONDS, tags)
  val resendRate = newMeter("ResendsPerSec", "resends", TimeUnit.SECONDS, tags)
  val failedSendRate = newMeter("FailedSendsPerSec", "failed sends", TimeUnit.SECONDS, tags)
}

/**
 * Stores metrics of serialization and message sending activity of each producer client in a (clientId -> ProducerStats) map.
 */
@deprecated("This object has been deprecated and will be removed in a future release.", "0.10.0.0")
object ProducerStatsRegistry {
  private val valueFactory = (k: String) => new ProducerStats(k)
  private val statsRegistry = new Pool[String, ProducerStats](Some(valueFactory))

  def getProducerStats(clientId: String) = {
    statsRegistry.getAndMaybePut(clientId)
  }

  def removeProducerStats(clientId: String) {
    statsRegistry.remove(clientId)
  }
}
