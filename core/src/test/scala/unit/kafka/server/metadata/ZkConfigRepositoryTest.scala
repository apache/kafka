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
package kafka.server

import java.util.Properties

import kafka.server.metadata.ZkConfigRepository
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

class ZkConfigRepositoryTest {

  @Test
  def testZkConfigRepository(): Unit = {
    val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
    val zkConfigRepository = ZkConfigRepository(zkClient)
    val brokerId = 1
    val topic = "topic"
    val brokerProps = new Properties()
    brokerProps.put("a", "b")
    val topicProps = new Properties()
    topicProps.put("c", "d")
    when(zkClient.getEntityConfigs(ConfigType.Broker, brokerId.toString)).thenReturn(brokerProps)
    when(zkClient.getEntityConfigs(ConfigType.Topic, topic)).thenReturn(topicProps)
    assertEquals(brokerProps, zkConfigRepository.brokerConfig(brokerId))
    assertEquals(topicProps, zkConfigRepository.topicConfig(topic))
  }

  @Test
  def testUnsupportedTypes(): Unit = {
    val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
    val zkConfigRepository = ZkConfigRepository(zkClient)
    Type.values().foreach(value => if (value != Type.BROKER && value != Type.TOPIC)
      assertThrows(classOf[IllegalArgumentException], () => zkConfigRepository.config(new ConfigResource(value, value.toString))))
  }
}
