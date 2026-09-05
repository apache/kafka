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

package kafka.server.controller

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.controller.util.ControllerListenerReconfigurable
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util
import scala.collection.mutable

class DynamicControllerConfigTest {

  private class StubReconfigurable extends ControllerListenerReconfigurable {
    val validatedConfigs: mutable.ArrayBuffer[util.Map[String, _]] = mutable.ArrayBuffer.empty
    val appliedConfigs: mutable.ArrayBuffer[util.Map[String, _]] = mutable.ArrayBuffer.empty

    override def reconfigurableConfigs(): util.Set[String] = util.Set.of("ssl.keystore.location")
    override def validateReconfiguration(configs: util.Map[String, _]): Unit = validatedConfigs += configs
    override def reconfigure(configs: util.Map[String, _]): Unit = appliedConfigs += configs
  }

  private def newConfig(): KafkaConfig = {
    val props = TestUtils.createBrokerConfig(0)
    new KafkaConfig(props)
  }

  @Test
  def testApplyDispatchesToRegisteredReconfigurables(): Unit = {
    val dynamicConfig = new DynamicControllerConfig(newConfig())
    val r1 = new StubReconfigurable
    val r2 = new StubReconfigurable
    dynamicConfig.addReconfigurable(r1)
    dynamicConfig.addReconfigurable(r2)

    val configs = new util.HashMap[String, String]()
    configs.put("ssl.keystore.location", "/tmp/keystore.jks")
    dynamicConfig.apply(configs)

    assertEquals(1, r1.validatedConfigs.size)
    assertEquals(1, r1.appliedConfigs.size)
    assertEquals(1, r2.validatedConfigs.size)
    assertEquals(1, r2.appliedConfigs.size)
    assertEquals("/tmp/keystore.jks", dynamicConfig.currentValue("ssl.keystore.location").get)
  }

  @Test
  def testApplySkipsWhenConfigUnchanged(): Unit = {
    val dynamicConfig = new DynamicControllerConfig(newConfig())
    val r = new StubReconfigurable
    dynamicConfig.addReconfigurable(r)

    val configs = new util.HashMap[String, String]()
    configs.put("ssl.keystore.location", "/tmp/keystore.jks")
    dynamicConfig.apply(configs)
    dynamicConfig.apply(configs)

    assertEquals(1, r.appliedConfigs.size, "Should not re-dispatch on identical config")
  }

  @Test
  def testRemoveReconfigurableStopsDispatch(): Unit = {
    val dynamicConfig = new DynamicControllerConfig(newConfig())
    val r = new StubReconfigurable
    dynamicConfig.addReconfigurable(r)
    dynamicConfig.removeReconfigurable(r)

    val configs = new util.HashMap[String, String]()
    configs.put("ssl.keystore.location", "/tmp/keystore.jks")
    dynamicConfig.apply(configs)

    assertEquals(0, r.appliedConfigs.size)
  }
}
