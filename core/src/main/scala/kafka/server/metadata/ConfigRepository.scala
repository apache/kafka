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

import java.util.Properties

import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type

trait ConfigRepository {
  /**
   * Return a copy of the topic configuration for the given topic.  Future changes will not be reflected.
   *
   * @param topicName the name of the topic for which the configuration will be returned
   * @return a copy of the topic configuration for the given topic
   */
  def topicConfig(topicName: String): Properties = {
    config(new ConfigResource(Type.TOPIC, topicName))
  }

  /**
   * Return a copy of the broker configuration for the given broker.  Future changes will not be reflected.
   *
   * @param brokerId the id of the broker for which configuration will be returned
   * @return a copy of the broker configuration for the given broker
   */
  def brokerConfig(brokerId: Int): Properties = {
    config(new ConfigResource(Type.BROKER, brokerId.toString))
  }

  /**
   * Return a copy of the configuration for the given resource.  Future changes will not be reflected.
   * @param configResource the resource for which the configuration will be returned
   * @return a copy of the configuration for the given resource
   */
  def config(configResource: ConfigResource): Properties
}
