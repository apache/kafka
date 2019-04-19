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

import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.utils.AppInfoParser;

import io.confluent.support.metrics.SupportKafkaMetricsBasic;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.Uuid;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.time.TimeUtils;
import kafka.server.KafkaServer;

public class BasicCollector extends Collector {

  private final TimeUtils time;
  private final Uuid uuid;
  private final KafkaServer server;

  public BasicCollector(
      KafkaServer server,
      TimeUtils time
  ) {
    this(server, time, new Uuid());
  }

  public BasicCollector(
      KafkaServer server,
      TimeUtils time,
      Uuid uuid
  ) {
    super();
    this.server = server;
    this.time = time;
    this.uuid = uuid;
  }

  /**
   * @return A new metrics record, or null in case of any errors.
   */
  @Override
  public GenericContainer collectMetrics() {
    SupportKafkaMetricsBasic metricsRecord = new SupportKafkaMetricsBasic();
    metricsRecord.setTimestamp(time.nowInUnixTime());
    metricsRecord.setKafkaVersion(AppInfoParser.getVersion());
    metricsRecord.setConfluentPlatformVersion(Version.getVersion());
    metricsRecord.setCollectorState(this.getRuntimeState().stateId());
    metricsRecord.setBrokerProcessUUID(uuid.toString());
    metricsRecord.setClusterId(server.clusterId());
    return metricsRecord;
  }

}
