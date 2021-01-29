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
class LocalConfigRepository extends ConfigRepository {
  val configMap = new ConcurrentHashMap[ConfigResource, util.HashMap[String, String]]

  def setTopicConfig(topic: String, key: String, value: String): Unit = {
    setConfig(new ConfigResource(Type.TOPIC, topic), key, value)
  }

  def setBrokerConfig(id: Int, key: String, value: String): Unit = {
    setConfig(new ConfigResource(Type.BROKER, id.toString()), key, value)
  }

  def setConfig(configResource: ConfigResource, key: String, value: String): Unit = {
    configMap.compute(configResource, new BiFunction[ConfigResource, util.HashMap[String, String], util.HashMap[String, String]] {
      override def apply(resource: ConfigResource,
                         curConfigs: util.HashMap[String, String]): util.HashMap[String, String] = {
        if (value == null) {
          if (curConfigs == null) {
            null
          } else {
            val newConfigs = new util.HashMap[String, String](curConfigs)
            newConfigs.remove(key)
            if (newConfigs.isEmpty) {
              null
            } else {
              newConfigs
            }
          }
        } else {
          if (curConfigs == null) {
            val newConfigs = new util.HashMap[String, String](1)
            newConfigs.put(key, value)
            newConfigs
          } else {
            val newConfigs = new util.HashMap[String, String](curConfigs.size() + 1)
            newConfigs.putAll(curConfigs)
            newConfigs.put(key, value)
            newConfigs
          }
        }
      }
    })
  }

  def config(configResource: ConfigResource): Properties = {
    val properties = new Properties()
    Option(configMap.get(configResource)).foreach {
      _.entrySet().iterator().asScala.foreach { case e =>
        properties.put(e.getKey, e.getValue)
      }
    }
    properties
  }

  override def topicConfigs(topic: String): Properties = {
    config(new ConfigResource(Type.TOPIC, topic))
  }

  override def brokerConfigs(id: Int): Properties = {
    config(new ConfigResource(Type.BROKER, id.toString()))
  }
}
