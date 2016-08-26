/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package unit.kafka.server

import java.util.Collections

import kafka.common.TopicAndPartition
import kafka.server.{QuotaType, ReplicationQuotaManager, ReplicationQuotaManagerConfig}
import org.apache.kafka.common.metrics.stats.FixedWindowRate
import org.apache.kafka.common.metrics.{Quota, MetricConfig, Metrics}
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertFalse, assertTrue, assertEquals}
import org.junit.Test


class ReplicationQuotaManagerTest {
  private val time = new MockTime

  @Test
  def shouldThrottleOnlyDefinedReplicas() {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(), newMetrics, "consumer", time)
    quota.markReplicasAsThrottled("topic1", Seq(1, 2, 3))

    assertTrue(quota.isThrottled(tp1(1)))
    assertTrue(quota.isThrottled(tp1(2)))
    assertTrue(quota.isThrottled(tp1(3)))
    assertFalse(quota.isThrottled(tp1(4)))
  }

  @Test
  def shouldExceedQuotaThenReturnBackBelowBoundAsTimePasses(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(numQuotaSamples = 10, quotaWindowSizeSeconds = 1), newMetrics, QuotaType.LeaderReplication.toString, time) {
      override def newRateInstance() = new FixedWindowRate() //TODO - decide which rate is best prior to merge
    }

    //Given
    quota.updateQuota(new Quota(100, true))

    //When we record anything
    quota.record(100)

    //Then it should not break the quota
    assertFalse(quota.isQuotaExceeded())

    //Add a byte to push over quota
    quota.record(1)

    //Then it should break the quota
    assertTrue(quota.isQuotaExceeded())
    assertEquals(101, quota.rate, 0)

    //When we sleep for half the window
    time.sleep(500)

    //Then Our rate should be based on just half the window
    assertTrue(quota.isQuotaExceeded())
    assertEquals(101, quota.rate, 0)

    //When we sleep for the remainder of the window
    time.sleep(501)

    //Then the rate should be back down to below the quota
    assertFalse(quota.isQuotaExceeded())
    assertEquals(101d / 2, quota.rate, 0)

    //Sleep for 2 more window
    time.sleep(2 * 1000)
    assertFalse(quota.isQuotaExceeded())
    assertEquals(101d / 4, quota.rate, 0)
  }

  @Test
  def shouldExceedWhenProposedBytesExceedQuota(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(numQuotaSamples = 10, quotaWindowSizeSeconds = 1), newMetrics, QuotaType.LeaderReplication.toString, time) {
      override def newRateInstance() = new FixedWindowRate() //TODO - decide which rate is best prior to merge
    }

    //Given
    quota.updateQuota(new Quota(100, true))

    //Then
    assertFalse(quota.isQuotaExceededBy(0))
    assertFalse(quota.isQuotaExceededBy(50))
    assertTrue(quota.isQuotaExceededBy(101))

    //When add value
    quota.record(100)

    //Then
    assertFalse(quota.isQuotaExceededBy(0))
    assertTrue(quota.isQuotaExceededBy(1))

    //When advance over window boundary
    time.sleep(1000)

    //Then
    assertFalse(quota.isQuotaExceededBy(0))
    assertTrue(quota.isQuotaExceededBy(201)) //need twice the quota now

    //When we go past total number of samples + 1, so should reset first
    time.sleep(10 * 1000)
    assertFalse(quota.isQuotaExceededBy(100))
    assertTrue(quota.isQuotaExceededBy(101))
  }

  @Test
  def shouldSupportWildcardThrottledReplicas(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(), newMetrics, QuotaType.LeaderReplication.toString, time)

    //When
    quota.markReplicasAsThrottled("MyTopic")

    //Then
    assertTrue(quota.isThrottled(TopicAndPartition("MyTopic", 0)))
    assertFalse(quota.isThrottled(TopicAndPartition("MyOtherTopic", 0)))
  }

  def tp1(id: Int): TopicAndPartition = new TopicAndPartition("topic1", id)

  def newMetrics: Metrics = {
    new Metrics(new MetricConfig(), Collections.emptyList(), time)
  }
}
