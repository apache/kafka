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

import com.yammer.metrics.core.{Gauge, Histogram}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import scala.util.Using

final class ForwardingManagerMetricsTest {
  @Test
  def testMetricsNames(): Unit = {
    val metricsRegistry = KafkaYammerMetrics.defaultRegistry()

    val expectedMetrics = Set(
      "kafka.server:type=ForwardingManager,name=QueueTimeMs",
      "kafka.server:type=ForwardingManager,name=QueueLength",
      "kafka.server:type=ForwardingManager,name=RemoteTimeMs"
    )

    Using(ForwardingManagerMetrics()) { _ =>
      val histogramsMap = metricsRegistry.allMetrics().asScala.filter { case (name, _) => name.getGroup == "kafka.server" && name.getType == "ForwardingManager"}
      assertEquals(histogramsMap.size, expectedMetrics.size)
      histogramsMap.foreach {case (name, _) =>
        assertTrue(expectedMetrics.contains(name.getMBeanName))
      }
    }

    val histogramsMap = metricsRegistry.allMetrics().asScala.filter { case (name, _) => name.getGroup == "kafka.server" && name.getType == "ForwardingManager" }
    assertEquals(0, histogramsMap.size)
  }

  @Test
  def testQueueTimeMs(): Unit = {
    val metricsRegistry = KafkaYammerMetrics.defaultRegistry()

    Using(ForwardingManagerMetrics()) { forwardingManagerMetrics =>
      val queueTimeMs = metricsRegistry.allMetrics().get(forwardingManagerMetrics.queueTimeMsName).asInstanceOf[Histogram]
      forwardingManagerMetrics.queueTimeMsHist.update(10)
      assertEquals(10, queueTimeMs.max())
      forwardingManagerMetrics.queueTimeMsHist.update(20)
      assertEquals(15, queueTimeMs.mean().asInstanceOf[Long])
    }
  }

  @Test
  def testQueueLength(): Unit = {
    val metricsRegistry = KafkaYammerMetrics.defaultRegistry()

    Using(ForwardingManagerMetrics()) { forwardingManagerMetrics =>
      val queueLength = metricsRegistry.allMetrics().get(forwardingManagerMetrics.queueLengthName).asInstanceOf[Gauge[Int]]
      assertEquals(0, queueLength.value())
      forwardingManagerMetrics.queueLength.getAndIncrement()
      assertEquals(1, queueLength.value())
    }
  }

  @Test
  def testRemoteTimeMs(): Unit = {
    val metricsRegistry = KafkaYammerMetrics.defaultRegistry()

    Using(ForwardingManagerMetrics()) { forwardingManagerMetrics =>
      val remoteTimeMs = metricsRegistry.allMetrics().get(forwardingManagerMetrics.remoteTimeMsName).asInstanceOf[Histogram]
      forwardingManagerMetrics.remoteTimeMsHist.update(10)
      assertEquals(10, remoteTimeMs.max())
      forwardingManagerMetrics.remoteTimeMsHist.update(20)
      assertEquals(15, remoteTimeMs.mean().asInstanceOf[Long])
    }
  }

}
