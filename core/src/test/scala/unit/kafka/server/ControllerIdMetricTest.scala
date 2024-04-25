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

import kafka.integration.KafkaServerTestHarness
import kafka.utils.TestUtils
import kafka.zk.ZkVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class ControllerIdMetricTest extends KafkaServerTestHarness {
  @Override
  def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(1, zkConnectOrNull, enableControlledShutdown = false).
      map(KafkaConfig.fromProps).toSeq
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk"))
  def testZkControllerId(): Unit = {
    val server = servers.head
    TestUtils.retry(30000) {
      assertEquals(server.config.brokerId, server.getCurrentControllerIdFromOldController())
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk"))
  def testZkControllerIdWhenZnodeIsDeleted(): Unit = {
    val server = servers.head
    TestUtils.retry(30000) {
      zkClient.deleteController(ZkVersion.MatchAnyVersion)
      assertEquals(-1, server.getCurrentControllerIdFromOldController())
    }
  }
}
