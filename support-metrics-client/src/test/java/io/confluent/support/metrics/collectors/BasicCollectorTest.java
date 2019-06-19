/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.support.metrics.collectors;

import kafka.server.KafkaServer;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.BeforeClass;
import org.junit.Test;

import io.confluent.support.metrics.SupportKafkaMetricsBasic;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.Uuid;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.time.TimeUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasicCollectorTest {
  private static KafkaServer mockServer;

  @BeforeClass
  public static void startCluster() {
    mockServer = mock(KafkaServer.class);
    when(mockServer.clusterId()).thenReturn("dummy");
  }

  @Test
  public void testCollectMetrics() {
    // Given
    TimeUtils time = new TimeUtils();
    Uuid uuid = new Uuid();
    long unixTimeAtTestStart = time.nowInUnixTime();
    Collector metricsCollector = new BasicCollector(mockServer, time, uuid);

    // When
    GenericContainer metricsRecord = metricsCollector.collectMetrics();

    // Then
    assertTrue(metricsRecord instanceof SupportKafkaMetricsBasic);
    assertEquals(SupportKafkaMetricsBasic.getClassSchema(), metricsRecord.getSchema());
    SupportKafkaMetricsBasic basicRecord = (SupportKafkaMetricsBasic) metricsRecord;
    assertTrue(unixTimeAtTestStart <= basicRecord.getTimestamp());
    assertTrue(basicRecord.getTimestamp() <= time.nowInUnixTime());
    assertEquals(AppInfoParser.getVersion(), basicRecord.getKafkaVersion());
    assertEquals(Version.getVersion(), basicRecord.getConfluentPlatformVersion());
    assertEquals(metricsCollector.getRuntimeState().stateId(), basicRecord.getCollectorState().intValue());
    assertEquals(uuid.toString(), basicRecord.getBrokerProcessUUID());
  }
}