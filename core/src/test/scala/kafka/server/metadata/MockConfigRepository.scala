/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.TOPIC

object MockConfigRepository {
  def forTopic(topic: String, key: String, value: String): MockConfigRepository = {
    val properties = new Properties()
    properties.put(key, value)
    forTopic(topic, properties)
  }

  def forTopic(topic: String, properties: Properties): MockConfigRepository = {
    val repository = new MockConfigRepository()
    repository.configs.put(new ConfigResource(TOPIC, topic), properties)
    repository
  }
}

class MockConfigRepository extends ConfigRepository {
  val configs = new util.HashMap[ConfigResource, Properties]()

  override def config(configResource: ConfigResource): Properties = configs.synchronized {
    configs.getOrDefault(configResource, new Properties())
  }

  def setConfig(configResource: ConfigResource, key: String, value: String): Unit = configs.synchronized {
    val properties = configs.getOrDefault(configResource, new Properties())
    val newProperties = new Properties()
    newProperties.putAll(properties)
    if (value == null) {
      newProperties.remove(key)
    } else {
      newProperties.put(key, value)
    }
    configs.put(configResource, newProperties)
  }

  def setTopicConfig(topicName: String, key: String, value: String): Unit = configs.synchronized {
    setConfig(new ConfigResource(TOPIC, topicName), key, value)
  }
}
