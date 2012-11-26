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

package kafka.consumer

import kafka.utils.{Pool, threadsafe, Logging}
import java.util.concurrent.TimeUnit
import kafka.metrics.KafkaMetricsGroup

@threadsafe
class ConsumerTopicStat(name: String) extends KafkaMetricsGroup {
  val messageRate = newMeter(name + "MessagesPerSec",  "messages", TimeUnit.SECONDS)
  val byteRate = newMeter(name + "BytesPerSec",  "bytes", TimeUnit.SECONDS)
}

object ConsumerTopicStat extends Logging {
  private val valueFactory = (k: String) => new ConsumerTopicStat(k)
  private val stats = new Pool[String, ConsumerTopicStat](Some(valueFactory))
  private val allTopicStat = new ConsumerTopicStat("AllTopics")

  def getConsumerAllTopicStat(): ConsumerTopicStat = allTopicStat

  def getConsumerTopicStat(topic: String): ConsumerTopicStat = {
    stats.getAndMaybePut(topic + "-")
  }
}
