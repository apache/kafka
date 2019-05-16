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

package io.confluent.support.metrics;

import java.util.Objects;

import io.confluent.support.metrics.collectors.CollectorFactory;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.CollectorType;
import io.confluent.support.metrics.common.kafka.KafkaUtilities;
import io.confluent.support.metrics.common.kafka.ZkClientProvider;
import io.confluent.support.metrics.common.time.TimeUtils;
import io.confluent.support.metrics.tools.KafkaServerZkClientProvider;
import kafka.server.KafkaServer;

/**
 * Periodically reports metrics collected from a Kafka broker.
 *
 * <p>Metrics are being reported to a Kafka topic within the same cluster and/or to Confluent via
 * the Internet.
 *
 * <p>This class is not thread-safe.
 */
public class MetricsReporter extends BaseMetricsReporter {

  private final KafkaServer server;
  private final Runtime serverRuntime;
  private final KafkaSupportConfig kafkaSupportConfig;
  private final KafkaServerZkClientProvider zkClientProvider;

  public MetricsReporter(String threadName,
                         boolean isDaemon,
                         KafkaServer server,
                         KafkaSupportConfig kafkaSupportConfig,
                         Runtime serverRuntime) {
    this(threadName, isDaemon, server, kafkaSupportConfig, serverRuntime, new KafkaUtilities());
  }

  /**
   * @param server The Kafka server.
   * @param kafkaSupportConfig The properties this server was created from, plus extra Proactive
   *     Support (PS) ones
   *     Note that Kafka does not understand PS properties,
   *     hence server->KafkaConfig() does not contain any of them, necessitating
   *     passing this extra argument to the API.
   * @param serverRuntime The Java runtime of the server that is being monitored.
   * @param kafkaUtilities An instance of {@link KafkaUtilities} that will be used to perform
   *     e.g. Kafka topic management if needed.
   */
  public MetricsReporter(String threadName,
                         boolean isDaemon,
                         KafkaServer server,
                         KafkaSupportConfig kafkaSupportConfig,
                         Runtime serverRuntime,
                         KafkaUtilities kafkaUtilities) {
    super(threadName, isDaemon, kafkaSupportConfig, kafkaUtilities, null, true);
    this.server = server;
    this.serverRuntime = serverRuntime;
    this.kafkaSupportConfig = kafkaSupportConfig;
    this.zkClientProvider = new KafkaServerZkClientProvider(server);
    Objects.requireNonNull(server, "Kafka Server can't be null");
    Objects.requireNonNull(serverRuntime, "serverRuntime can't be null");
  }

  @Override
  protected ZkClientProvider zkClientProvider() {
    return zkClientProvider;
  }

  @Override
  protected Collector metricsCollector() {
    TimeUtils time = new TimeUtils();
    CollectorType collectorType;
    if (BaseSupportConfig.isAnonymousUser(kafkaSupportConfig.getCustomerId())) {
      collectorType = CollectorType.BASIC;
    } else {
      collectorType = CollectorType.FULL;
    }
    CollectorFactory factory = new CollectorFactory(collectorType, time, server,
                                                    kafkaSupportConfig.getProperties(),
                                                    serverRuntime
    );
    Collector metricsCollector = factory.getCollector();
    return metricsCollector;
  }

  @Override
  protected boolean isReadyForMetricsCollection() {
    return kafkaUtilities.isReadyForMetricsCollection(server);
  }

  @Override
  protected boolean isShuttingDown() {
    return kafkaUtilities.isShuttingDown(server);
  }

}
