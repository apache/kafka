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

package kafka.metrics

import org.apache.kafka.server.metrics.KafkaMetricsGroup

import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.api.Test

import java.util.Collections
import scala.jdk.CollectionConverters._

class KafkaMetricsGroupTest {

  @Test
  def testUntaggedMetricName(): Unit = {
    val metricName = KafkaMetricsGroup.explicitMetricName(
      "kafka.metrics",
      "TestMetrics",
      "TaggedMetric",
      Collections.emptyMap()
    )

    assertEquals("kafka.metrics", metricName.getGroup)
    assertEquals("TestMetrics", metricName.getType)
    assertEquals("TaggedMetric", metricName.getName)
    assertEquals("kafka.metrics:type=TestMetrics,name=TaggedMetric",
      metricName.getMBeanName)
    assertNull(metricName.getScope)
  }

  @Test
  def testTaggedMetricName(): Unit = {
    val tags = Map("foo" -> "bar", "bar" -> "baz", "baz" -> "raz.taz").asJava
    val metricName = KafkaMetricsGroup.explicitMetricName(
      "kafka.metrics",
      "TestMetrics",
      "TaggedMetric",
      tags
    )

    assertEquals("kafka.metrics", metricName.getGroup)
    assertEquals("TestMetrics", metricName.getType)
    assertEquals("TaggedMetric", metricName.getName)
    assertEquals("kafka.metrics:type=TestMetrics,name=TaggedMetric,foo=bar,bar=baz,baz=raz.taz",
      metricName.getMBeanName)
    assertEquals("bar.baz.baz.raz_taz.foo.bar", metricName.getScope)
  }

  @Test
  def testTaggedMetricNameWithEmptyValue(): Unit = {
    val tags = Map("foo" -> "bar", "bar" -> "", "baz" -> "raz.taz").asJava
    val metricName = KafkaMetricsGroup.explicitMetricName(
      "kafka.metrics",
      "TestMetrics",
      "TaggedMetric",
      tags
    )

    assertEquals("kafka.metrics", metricName.getGroup)
    assertEquals("TestMetrics", metricName.getType)
    assertEquals("TaggedMetric", metricName.getName)
    assertEquals("kafka.metrics:type=TestMetrics,name=TaggedMetric,foo=bar,baz=raz.taz",
      metricName.getMBeanName)
    assertEquals("baz.raz_taz.foo.bar", metricName.getScope)
  }


}
