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
package kafka.raft

import com.yammer.metrics.core.MetricsRegistry
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.controller.metrics.ControllerMetadataMetrics
import org.apache.kafka.server.metrics.BrokerServerMetrics
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Optional

final class DefaultExternalKRaftMetricsTest {
  @Test
  def testDefaultExternalKRaftMetrics(): Unit = {
    val brokerServerMetrics = new BrokerServerMetrics(new Metrics())
    val controllerMetadataMetrics = new ControllerMetadataMetrics(Optional.of(new MetricsRegistry()))
    val metrics = new DefaultExternalKRaftMetrics(
      Option(brokerServerMetrics),
      Option(controllerMetadataMetrics)
    )

    assertFalse(brokerServerMetrics.ignoredStaticVoters())
    assertFalse(controllerMetadataMetrics.ignoredStaticVoters())

    metrics.setIgnoredStaticVoters(true)

    assertTrue(brokerServerMetrics.ignoredStaticVoters())
    assertTrue(controllerMetadataMetrics.ignoredStaticVoters())

    metrics.setIgnoredStaticVoters(false)

    assertFalse(brokerServerMetrics.ignoredStaticVoters())
    assertFalse(controllerMetadataMetrics.ignoredStaticVoters())
  }

  @Test
  def testEmptyDefaultExternalKRaftMetrics(): Unit = {
    val metrics = new DefaultExternalKRaftMetrics(None, None)
    metrics.setIgnoredStaticVoters(true)
  }
}
