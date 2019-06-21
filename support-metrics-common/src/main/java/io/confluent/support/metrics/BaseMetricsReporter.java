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

import org.apache.avro.generic.GenericContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.serde.AvroSerializer;
import io.confluent.support.metrics.submitters.ConfluentSubmitter;
import io.confluent.support.metrics.submitters.ResponseHandler;
import io.confluent.support.metrics.submitters.Submitter;
import io.confluent.support.metrics.utils.Jitter;

/**
 * Periodically reports metrics collected from a Kafka broker.
 *
 * <p>Metrics are being reported to a Kafka topic within the same cluster and/or to Confluent via
 * the Internet.
 *
 * <p>This class is not thread-safe.
 */
public abstract class BaseMetricsReporter extends Thread implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(BaseMetricsReporter.class);

  /**
   * Default "retention.ms" setting (i.e. time-based retention) of the support metrics topic. Used
   * when creating the topic in case it doesn't exist yet.
   */
  protected static final long RETENTION_MS = 365 * 24 * 60 * 60 * 1000L;

  /**
   * Default replication factor of the support metrics topic. Used when creating the topic in case
   * it doesn't exist yet.
   */
  public static final int SUPPORT_TOPIC_REPLICATION = 3;

  /**
   * Default number of partitions of the support metrics topic. Used when creating the topic in case
   * it doesn't exist yet.
   */
  protected static final int SUPPORT_TOPIC_PARTITIONS = 1;

  /**
   * Length of the wait period we give the server to start up completely (in a different thread)
   * before we begin metrics collection.
   */
  private static final long SETTLING_TIME_MS = 10 * 1000L;
  private final boolean enableSettlingTime;

  private String customerId;
  private long reportIntervalMs;
  private String supportTopic;
  private Submitter kafkaSubmitter;
  private Submitter confluentSubmitter;
  private Collector metricsCollector;
  private final AvroSerializer encoder = new AvroSerializer();
  protected final BaseSupportConfig supportConfig;
  private final ResponseHandler responseHandler;
  private volatile boolean isClosing = false;

  public BaseMetricsReporter(String threadName,
                             boolean isDaemon,
                             BaseSupportConfig serverConfiguration) {
    this(threadName, isDaemon, serverConfiguration, null, true);
  }

  /**
   * @param supportConfig The properties this server was created from, plus extra Proactive Support
   *     (PS) one Note that Kafka does not understand PS properties, hence server->KafkaConfig()
   *     does not contain any of them, necessitating passing this extra argument to the API.
   * @param responseHandler Http Response Handler
   * @param enableSettlingTime Enable settling time before starting metrics
   */
  public BaseMetricsReporter(String threadName,
                             boolean isDaemon,
                             BaseSupportConfig supportConfig,
                             ResponseHandler responseHandler,
                             boolean enableSettlingTime) {
    super(threadName);
    setDaemon(isDaemon);
    Objects.requireNonNull(supportConfig, "supportConfig can't be null");
    this.supportConfig = supportConfig;
    this.responseHandler = responseHandler;
    this.enableSettlingTime = enableSettlingTime;
  }

  public void init() {
    customerId = supportConfig.getCustomerId();
    metricsCollector = metricsCollector();
    metricsCollector.setRuntimeState(Collector.RuntimeState.Running);

    reportIntervalMs = supportConfig.getReportIntervalMs();
    supportTopic = supportConfig.getKafkaTopic();

    if (!supportTopic.isEmpty()) {
      kafkaSubmitter = createKafkaSubmitter(supportTopic);
    } else {
      kafkaSubmitter = null;
    }

    String endpointHTTP = supportConfig.getEndpointHTTP();
    String endpointHTTPS = supportConfig.getEndpointHTTPS();
    String proxyURI = supportConfig.getProxy();

    if (!endpointHTTP.isEmpty() || !endpointHTTPS.isEmpty()) {
      confluentSubmitter = new ConfluentSubmitter(customerId, endpointHTTP, endpointHTTPS,
              proxyURI, responseHandler);
    } else {
      confluentSubmitter = null;
    }

    if (!reportingEnabled()) {
      log.info("Metrics collection disabled by component configuration");
    }
  }

  protected abstract Submitter createKafkaSubmitter(String supportTopic);

  protected abstract boolean kafkaSubmitterReady(String supportTopic);

  protected abstract Collector metricsCollector();

  protected boolean reportingEnabled() {
    return sendToKafkaEnabled() || sendToConfluentEnabled();
  }

  protected boolean sendToKafkaEnabled() {
    return kafkaSubmitter != null;
  }

  protected boolean sendToConfluentEnabled() {
    return confluentSubmitter != null;
  }

  @Override
  public void run() {
    try {
      if (reportingEnabled()) {
        waitForServer();
        while (!isClosing) {
          log.info("Attempting to collect and submit metrics");
          submitMetrics();
          Thread.sleep(Jitter.addOnePercentJitter(reportIntervalMs));
        }
      }
    } catch (InterruptedException e) {
      log.error("Caught InterruptedException during metrics collection", e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.error("Caught exception during metrics collection", e);
    } finally {
      if (isClosing) {
        log.info("Gracefully terminating metrics collection");
        metricsCollector.setRuntimeState(Collector.RuntimeState.ShuttingDown);
        submitMetrics();
      }
      log.info("Metrics collection stopped");
    }
  }

  @Override
  public void close() {
    log.info("Closing BaseMetricsReporter");
    isClosing = true;
    interrupt();
  }

  // Waits for the monitored service to fully start up.
  private void waitForServer() throws InterruptedException {
    log.info("Waiting until monitored service is ready for metrics collection");
    while (!isClosing && !isReadyForMetricsCollection() && !isShuttingDown()) {
      if (enableSettlingTime) {
        long waitTimeMs = Jitter.addOnePercentJitter(SETTLING_TIME_MS);
        log.info("Waiting {} ms for the monitored service to finish starting up", waitTimeMs);
        Thread.sleep(waitTimeMs);
      }
    }
    if (isShuttingDown()) {
      close();
    } else {
      log.info("Monitored service is now ready");
    }
  }

  protected abstract boolean isReadyForMetricsCollection();

  protected abstract boolean isShuttingDown();

  // package-private for tests
  void submitMetrics() {
    byte[] encodedMetricsRecord = null;
    GenericContainer metricsRecord = metricsCollector.collectMetrics();
    try {
      encodedMetricsRecord = encoder.serialize(metricsRecord);
    } catch (IOException e) {
      log.error("Could not serialize metrics record: {}", e.toString());
    }

    try {
      if (sendToKafkaEnabled() && encodedMetricsRecord != null) {
        // attempt to create the topic. If failures occur, try again in the next round, however
        // the current batch of metrics will be lost.
        if (kafkaSubmitterReady(supportTopic)) {
          kafkaSubmitter.submit(encodedMetricsRecord);
        }
      }
    } catch (RuntimeException e) {
      log.error("Could not submit metrics to Kafka topic {}: {}", supportTopic, e.getMessage());
    }

    try {
      if (sendToConfluentEnabled() && encodedMetricsRecord != null) {
        confluentSubmitter.submit(encodedMetricsRecord);
      }
    } catch (RuntimeException e) {
      log.error("Could not submit metrics to Confluent: {}", e.getMessage());
    }
  }

}
