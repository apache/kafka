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

package kafka.network

import com.yammer.metrics.core.Meter
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotSame, assertSame, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class RequestMetricsTest {

  private val requestName = "RequestMetricsTest"
  private val version: Short = 2

  @Test
  def testDeprecatedRequestRateMetersAreLimited(): Unit = {
    val metrics = new RequestMetrics(requestName)
    try {
      for (i <- 0 until RequestMetrics.MaxDeprecatedRequestRateMeters)
        deprecatedRequestRate(metrics, new ClientInformation(s"client$i", "1.0"))
      val overflow = deprecatedRequestRate(metrics, new ClientInformation("other1", "1.0"))
      assertSame(overflow, deprecatedRequestRate(metrics, new ClientInformation("other2", "1.0")))
      assertNotSame(overflow, deprecatedRequestRate(metrics, new ClientInformation("client0", "1.0")))
      val names = deprecatedRequestRateMBeanNames()
      assertEquals(RequestMetrics.MaxDeprecatedRequestRateMeters + 1, names.size)
      assertTrue(names.contains(mBeanName("client0", "1.0")))
      assertTrue(names.contains(mBeanName("_overflow_", "_overflow_")))
    } finally {
      metrics.removeMetrics()
    }
    assertEquals(Set.empty, deprecatedRequestRateMBeanNames())
  }

  @Test
  def testDeprecatedRequestRateTagsAreTruncated(): Unit = {
    val metrics = new RequestMetrics(requestName)
    try {
      val name = "a" * RequestMetrics.MaxClientInformationTagLength
      val softwareVersion = "1" * RequestMetrics.MaxClientInformationTagLength
      val meter = deprecatedRequestRate(metrics, new ClientInformation(name, "1.0"))
      assertSame(meter, deprecatedRequestRate(metrics, new ClientInformation(name + "b", "1.0")))
      deprecatedRequestRate(metrics, new ClientInformation("client", softwareVersion + "1"))
      assertEquals(Set(mBeanName(name, "1.0"), mBeanName("client", softwareVersion)), deprecatedRequestRateMBeanNames())
    } finally {
      metrics.removeMetrics()
    }
    assertEquals(Set.empty, deprecatedRequestRateMBeanNames())
  }

  private def deprecatedRequestRate(metrics: RequestMetrics, clientInformation: ClientInformation): Meter =
    metrics.deprecatedRequestRate(ApiKeys.PRODUCE, version, clientInformation).get

  private def mBeanName(softwareName: String, softwareVersion: String): String =
    s"kafka.network:type=RequestMetrics,name=${RequestMetrics.DeprecatedRequestsPerSec},request=$requestName," +
      s"version=$version,clientSoftwareName=$softwareName,clientSoftwareVersion=$softwareVersion"

  private def deprecatedRequestRateMBeanNames(): Set[String] =
    KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
      .filter(_.getName == RequestMetrics.DeprecatedRequestsPerSec)
      .map(_.getMBeanName)
      .filter(_.contains(s",request=$requestName,"))
      .toSet
}
