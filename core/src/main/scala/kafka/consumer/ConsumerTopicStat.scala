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

import java.util.concurrent.atomic.AtomicLong
import kafka.utils.{Pool, Utils, threadsafe, Logging}

trait ConsumerTopicStatMBean {
  def getMessagesPerTopic: Long
}

@threadsafe
class ConsumerTopicStat extends ConsumerTopicStatMBean {
  private val numCumulatedMessagesPerTopic = new AtomicLong(0)

  def getMessagesPerTopic: Long = numCumulatedMessagesPerTopic.get

  def recordMessagesPerTopic(nMessages: Int) = numCumulatedMessagesPerTopic.getAndAdd(nMessages)
}

object ConsumerTopicStat extends Logging {
  private val stats = new Pool[String, ConsumerTopicStat]

  def getConsumerTopicStat(topic: String): ConsumerTopicStat = {
    var stat = stats.get(topic)
    if (stat == null) {
      stat = new ConsumerTopicStat
      if (stats.putIfNotExists(topic, stat) == null)
        Utils.registerMBean(stat, "kafka:type=kafka.ConsumerTopicStat." + topic)
      else
        stat = stats.get(topic)
    }
    return stat
  }
}
