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

import kafka.server.{BrokerDynamicThreadPool, ConfigHandler, KafkaBroker, KafkaConfig, KafkaRequestHandlerPool}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.ConfigRecord
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.server.config.{ConfigType, ServerConfigs}
import org.apache.kafka.server.fault.FaultHandler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, verify, verifyNoInteractions, verifyNoMoreInteractions, when}

import java.util.Properties

class DynamicConfigPublisherTest {

  @Test
  def testThreadCountBoundsNotEnforcedOnFirstPublish(): Unit = {
    val props = TestUtils.createBrokerConfig(0)
    props.put(ServerConfigs.NUM_IO_THREADS_CONFIG, "16")
    val config = KafkaConfig(props)
    val serverMock = mock(classOf[KafkaBroker])
    val handlerPoolMock = mock(classOf[KafkaRequestHandlerPool])
    when(serverMock.config).thenReturn(config)
    when(serverMock.dataPlaneRequestHandlerPool).thenReturn(handlerPoolMock)
    config.dynamicConfig.initialize(None)
    config.dynamicConfig.addBrokerReconfigurable(new BrokerDynamicThreadPool(serverMock))

    val brokerConfigHandler = new ConfigHandler {
      override def processConfigChanges(entityName: String, value: Properties): Unit =
        config.dynamicConfig.updateDefaultConfig(value)
    }
    val faultHandler = mock(classOf[FaultHandler])
    val publisher = new DynamicConfigPublisher(config, faultHandler, Map(ConfigType.BROKER -> brokerConfigHandler), "broker")

    // The first update carries the dynamic configs loaded on startup, which are not bounded by the static configs
    val image = publish(publisher, MetadataImage.EMPTY, "6")
    assertEquals(6, config.numIoThreads)
    verify(handlerPoolMock).resizeThreadPool(newSize = 6)

    // Later updates must be within the bounds of the current values
    publish(publisher, image, "2")
    assertEquals(6, config.numIoThreads)

    verifyNoMoreInteractions(handlerPoolMock)
    verifyNoInteractions(faultHandler)
  }

  private def publish(publisher: DynamicConfigPublisher, image: MetadataImage, numIoThreads: String): MetadataImage = {
    val delta = new MetadataDelta.Builder().setImage(image).build()
    delta.replay(new ConfigRecord()
      .setResourceType(ConfigResource.Type.BROKER.id)
      .setResourceName("")
      .setName(ServerConfigs.NUM_IO_THREADS_CONFIG)
      .setValue(numIoThreads))
    val newImage = delta.apply(MetadataProvenance.EMPTY)
    publisher.onMetadataUpdate(delta, newImage)
    newImage
  }
}
