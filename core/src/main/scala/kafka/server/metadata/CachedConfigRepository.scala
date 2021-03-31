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

package kafka.server.metadata

import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type

import scala.jdk.CollectionConverters._

/**
 * A ConfigRepository that stores configurations locally.
 */
class CachedConfigRepository extends ConfigRepository {
  private val configMap = new ConcurrentHashMap[ConfigResource, util.HashMap[String, String]]

  /**
   * Set the topic config for the given topic name and the given key to the given value.
   *
   * @param topicName the name of the topic for which the config will be set
   * @param key the key identifying the topic config to set
   * @param value the value to set for the topic config with null implying a removal
   */
  def setTopicConfig(topicName: String, key: String, value: String): Unit = {
    setConfig(new ConfigResource(Type.TOPIC, topicName), key, value)
  }

  /**
   * Set the broker config for the given broker ID and the given key to the given value.
   *
   * @param brokerId the ID of the broker for which the config will be set
   * @param key the key identifying the broker config to set
   * @param value the value to set for the broker config with null implying a removal
   */
  def setBrokerConfig(brokerId: Int, key: String, value: String): Unit = {
    setConfig(new ConfigResource(Type.BROKER, brokerId.toString()), key, value)
  }

  /**
   * Set the config for the given resource and the given key to the given value.
   *
   * @param configResource the resource for which the config will be set
   * @param key the key identifying the resource config to set
   * @param value the value to set for the resource config with null implying a removal
   */
  def setConfig(configResource: ConfigResource, key: String, value: String): Unit = {
    configMap.compute(configResource, new BiFunction[ConfigResource, util.HashMap[String, String], util.HashMap[String, String]] {
      override def apply(resource: ConfigResource,
                         curConfig: util.HashMap[String, String]): util.HashMap[String, String] = {
        if (value == null) {
          if (curConfig == null) {
            null
          } else {
            val newConfig = new util.HashMap[String, String](curConfig)
            newConfig.remove(key)
            if (newConfig.isEmpty) {
              null
            } else {
              newConfig
            }
          }
        } else {
          if (curConfig == null) {
            val newConfig = new util.HashMap[String, String](1)
            newConfig.put(key, value)
            newConfig
          } else {
            val newConfig = new util.HashMap[String, String](curConfig.size() + 1)
            newConfig.putAll(curConfig)
            newConfig.put(key, value)
            newConfig
          }
        }
      }
    })
  }

  override def config(configResource: ConfigResource): Properties = {
    val properties = new Properties()
    Option(configMap.get(configResource)).foreach { resourceConfigMap =>
      resourceConfigMap.entrySet.iterator.asScala.foreach { entry =>
        properties.put(entry.getKey, entry.getValue)
      }
    }
    properties
  }

  def remove(configResource: ConfigResource): Unit = {
    configMap.remove(configResource)
  }

}
