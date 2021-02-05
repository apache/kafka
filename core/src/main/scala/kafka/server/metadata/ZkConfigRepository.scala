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

import kafka.server.ConfigType
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type


object ZkConfigRepository {
  def apply(zkClient: KafkaZkClient): ZkConfigRepository =
    new ZkConfigRepository(new AdminZkClient(zkClient))
}

class ZkConfigRepository(adminZkClient: AdminZkClient) extends ConfigRepository {
  override def config(configResource: ConfigResource): Properties = {
    val configResourceType = configResource.`type`()
    val configTypeForZk = if (configResourceType == Type.TOPIC) {
      ConfigType.Topic
    } else if (configResourceType == Type.BROKER) {
      ConfigType.Broker
    } else {
      throw new IllegalArgumentException(s"Unsupported config type: $configResourceType")
    }
    adminZkClient.fetchEntityConfig(configTypeForZk, configResource.name())
  }
}
