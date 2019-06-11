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

import io.confluent.support.metrics.common.kafka.KafkaUtilities;
import kafka.utils.TestUtils;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Properties;

import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.kafka.EmbeddedKafkaCluster;
import io.confluent.support.metrics.common.time.TimeUtils;
import io.confluent.support.metrics.serde.AvroDeserializer;
import io.confluent.support.metrics.tools.KafkaMetricsToFile;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import scala.collection.JavaConverters;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Integration test. Verifies that collected metrics are successfully submitted to a Kafka topic.
 */
public class MetricsToKafkaTest {

  @Test
  public void savesAsManyMetricsToFileAsHaveBeenSubmittedBySingleNodeCluster() throws IOException {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    Properties brokerConfiguration = defaultBrokerConfiguration(broker, cluster.zookeeperConnectString());
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG,
        "test_metrics");
    String topic = brokerConfiguration.getProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    int timeoutMs = 10 * 1000;
    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(bootstrapServer(broker.zkClient()));

    // Sent metrics to the topic
    int numMetricSubmissions = 10;
    KafkaSupportConfig kafkaSupportConfig = new KafkaSupportConfig(brokerConfiguration);
    MetricsReporter reporter = new MetricsReporter("testThread", false, broker, kafkaSupportConfig, serverRuntime);
    reporter.init();
    for (int i = 0; i < numMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    // When/Then
    String outputFile = "testFile.zip";
    assertEquals(numMetricSubmissions, kafkaMetricsToFile.saveMetricsToFile(topic, outputFile, timeoutMs));

    Files.delete(Paths.get(outputFile));
    cluster.stopCluster();
  }

  private Properties defaultBrokerConfiguration(KafkaServer broker, String zookeeperConnect) throws IOException {
    Properties brokerConfiguration = new Properties();
    brokerConfiguration.load(MetricsToKafkaTest.class.getResourceAsStream("/default-server.properties"));
    brokerConfiguration.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), Integer.toString(broker.config().brokerId()));
    brokerConfiguration.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeperConnect);

    return brokerConfiguration;
  }

  @Test
  public void retrievesBasicMetricsSubmittedByMultiNodeCluster() throws IOException {
    // Given
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 3;
    cluster.startCluster(numBrokers);
    KafkaServer firstBroker = cluster.getBroker(0);

    Properties brokerConfiguration = defaultBrokerConfiguration(firstBroker, cluster.zookeeperConnectString());
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG,
        "test_metrics");
    KafkaSupportConfig kafkaSupportConfig = new KafkaSupportConfig(brokerConfiguration);
    MetricsReporter reporter = new MetricsReporter("testThread", false, firstBroker, kafkaSupportConfig, Runtime.getRuntime());
    String topic = brokerConfiguration.getProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG);
    reporter.init();
    // When
    int expNumMetricSubmissions = 10;
    for (int i = 0; i < expNumMetricSubmissions; i++) {
      reporter.submitMetrics();
    }

    // Then
    verifyMetricsSubmittedToTopic(bootstrapServer(firstBroker.zkClient()), topic, expNumMetricSubmissions);

    // Cleanup
    cluster.stopCluster();
  }

  /**
   * Helper function that consumes messages from a topic with a timeout.
   */
  private static void verifyMetricsSubmittedToTopic(
      String bootstrapServer,
      String topic,
      int expNumMetricSubmissions) throws IOException {
    int timeoutMs = 10 * 1000;
    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(bootstrapServer);
    KafkaConsumer<byte[], byte[]> consumer = kafkaMetricsToFile.createConsumer();
    consumer.subscribe(singleton(topic));

    Collection<ConsumerRecord<byte[], byte[]>> records = JavaConverters.asJavaCollectionConverter(
            TestUtils.consumeRecords(consumer, expNumMetricSubmissions, timeoutMs)).asJavaCollection();
    AvroDeserializer decoder = new AvroDeserializer();
    for (final ConsumerRecord<byte[], byte[]> record : records) {
      SupportKafkaMetricsBasic[] container = decoder.deserialize(SupportKafkaMetricsBasic.class,
              record.value());
      assertEquals(1, container.length);
      verifyBasicMetrics(container[0]);
    }
  }

  private static void verifyBasicMetrics(SupportKafkaMetricsBasic basicRecord) {
    TimeUtils time = new TimeUtils();
    assertTrue(basicRecord.getTimestamp() <= time.nowInUnixTime());
    assertEquals(AppInfoParser.getVersion(), basicRecord.getKafkaVersion());
    assertEquals(Version.getVersion(), basicRecord.getConfluentPlatformVersion());
    assertFalse(basicRecord.getBrokerProcessUUID().isEmpty());
  }

  private String bootstrapServer(KafkaZkClient zkClient) {
    return new KafkaUtilities().getBootstrapServers(zkClient, 1).get(0);
  }
}
