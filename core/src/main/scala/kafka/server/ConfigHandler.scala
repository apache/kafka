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

package kafka.server

import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.log.{Log, LogConfig, LogManager}
import kafka.utils.Pool

import scala.collection.mutable

/**
 * The ConfigHandler is used to process config change notifications received by the DynamicConfigManager
 */
trait ConfigHandler {
  def processConfigChanges(entityName : String, value : Properties)
}

/**
 * The TopicConfigHandler will process topic config changes in ZK.
 * The callback provides the topic name and the full properties set read from ZK
 */
class TopicConfigHandler(private val logManager: LogManager) extends ConfigHandler{

  def processConfigChanges(topic : String, topicConfig : Properties) {
    val logs: mutable.Buffer[(TopicAndPartition, Log)] = logManager.logsByTopicPartition.toBuffer
    val logsByTopic: Map[String, mutable.Buffer[Log]] = logs.groupBy{ case (topicAndPartition, log) => topicAndPartition.topic }
            .mapValues{ case v: mutable.Buffer[(TopicAndPartition, Log)] => v.map(_._2) }

    if (logsByTopic.contains(topic)) {
      /* combine the default properties with the overrides in zk to create the new LogConfig */
      val props = new Properties()
      props.putAll(logManager.defaultConfig.originals)
      props.putAll(topicConfig)
      val logConfig = LogConfig(props)
      for (log <- logsByTopic(topic))
        log.config = logConfig
    }
  }
}

/**
 * The ClientIdConfigHandler will process clientId config changes in ZK.
 * The callback provides the clientId and the full properties set read from ZK.
 * This implementation does nothing currently. In the future, it will change quotas per client
 */
class ClientIdConfigHandler extends ConfigHandler {
  val configPool = new Pool[String, Properties]()

  def processConfigChanges(clientId : String, clientConfig : Properties): Unit = {
    configPool.put(clientId, clientConfig)
  }
}