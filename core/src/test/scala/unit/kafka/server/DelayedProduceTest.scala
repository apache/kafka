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

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

class DelayedProduceTest {

  @AfterEach
  def tearDown(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test
  def testRemovePartitionMetrics(): Unit = {
    val partition = new TopicPartition("test-topic", 0)

    // Record an expiration so the partition metric is created
    DelayedProduceMetrics.recordExpiration(partition)

    // Verify the partition metric exists in the registry
    val metricsBefore = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
    assertTrue(metricsBefore.exists(name =>
      name.getMBeanName.contains("topic=test-topic") &&
        name.getMBeanName.contains("partition=0") &&
        name.getMBeanName.contains("name=ExpiresPerSec")),
      "Partition metric should exist after recordExpiration")

    val aggregateCountBefore = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.count(name =>
      name.getMBeanName.contains("name=ExpiresPerSec") &&
        name.getMBeanName.contains("DelayedProduceMetrics") &&
        !name.getMBeanName.contains("topic="))

    // Remove the partition metric
    DelayedProduceMetrics.removePartitionMetrics(partition)

    // Verify the partition metric is removed from the registry
    val metricsAfter = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
    assertFalse(metricsAfter.exists(name =>
      name.getMBeanName.contains("topic=test-topic") &&
        name.getMBeanName.contains("partition=0") &&
        name.getMBeanName.contains("name=ExpiresPerSec")),
      "Partition metric should be removed after removePartitionMetrics")

    // Verify the aggregate metric is unaffected
    val aggregateCountAfter = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.count(name =>
      name.getMBeanName.contains("name=ExpiresPerSec") &&
        name.getMBeanName.contains("DelayedProduceMetrics") &&
        !name.getMBeanName.contains("topic="))
    assertEquals(aggregateCountBefore, aggregateCountAfter,
      "Aggregate metric should be unaffected by removePartitionMetrics")
  }

  @Test
  def testRemovePartitionMetricsForNonExistentPartition(): Unit = {
    val partition = new TopicPartition("nonexistent-topic", 0)

    // Should not throw when removing a partition that was never recorded
    DelayedProduceMetrics.removePartitionMetrics(partition)
  }
}
