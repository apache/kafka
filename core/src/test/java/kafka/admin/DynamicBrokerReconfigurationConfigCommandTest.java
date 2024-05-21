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
package kafka.admin;

import kafka.server.AbstractDynamicBrokerReconfigurationTest;
import kafka.server.KafkaBroker;
import kafka.server.TestMetricsReporter;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Tuple2;
import scala.collection.Map;
import scala.collection.Map$;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.network.CertStores.KEYSTORE_PROPS;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicBrokerReconfigurationConfigCommandTest extends AbstractDynamicBrokerReconfigurationTest {
    private void verifyConfig(String configName, ConfigEntry configEntry, boolean isSensitive, boolean isReadOnly,
                             Properties expectedProps) {
        if (isSensitive) {
            assertTrue(configEntry.isSensitive(), "Value is sensitive: " + configName);
            assertNull(configEntry.value(), "Sensitive value returned for " + configName);
        } else {
            assertFalse(configEntry.isSensitive(), "Config is not sensitive: " + configName);
            assertEquals(expectedProps.getProperty(configName), configEntry.value());
        }
        assertEquals(isReadOnly, configEntry.isReadOnly(), "isReadOnly incorrect for " + configName + ": " + configEntry);
    }

    private void verifySynonym(String configName, ConfigEntry.ConfigSynonym synonym, boolean isSensitive,
                               String expectedPrefix, ConfigEntry.ConfigSource expectedSource, Properties expectedProps) {
        if (isSensitive)
            assertNull(synonym.value(), "Sensitive value returned for " + configName);
        else
            assertEquals(expectedProps.getProperty(configName), synonym.value());
        assertTrue(synonym.name().startsWith(expectedPrefix), "Expected listener config, got " + synonym);
        assertEquals(expectedSource, synonym.source());
    }

    private void verifySynonyms(String configName, List<ConfigEntry.ConfigSynonym> synonyms, boolean isSensitive,
                                String prefix, Optional<String> defaultValue) {
        int overrideCount = prefix.isEmpty() ? 0 : 2;
        assertEquals(1 + overrideCount + defaultValue.map(s -> 1).orElse(0), synonyms.size(), "Wrong synonyms for " + configName + ": " + synonyms);
        if (overrideCount > 0) {
            String listenerPrefix = "listener.name.external.ssl.";
            verifySynonym(configName, synonyms.get(0), isSensitive, listenerPrefix, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, sslProperties1());
            verifySynonym(configName, synonyms.get(1), isSensitive, listenerPrefix, ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, sslProperties1());
        }
        verifySynonym(configName, synonyms.get(overrideCount), isSensitive, "ssl.", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, invalidSslProperties());
        defaultValue.ifPresent(value -> {
            Properties defaultProps = new Properties();
            defaultProps.setProperty(configName, value);
            verifySynonym(configName, synonyms.get(overrideCount + 1), isSensitive, "ssl.", ConfigEntry.ConfigSource.DEFAULT_CONFIG, defaultProps);
        });
    }

    private void verifySslConfig(String prefix, Properties expectedProps, Config configDesc) {
        // Validate file-based SSL keystore configs
        Set<String> keyStoreProps = new HashSet<>(KEYSTORE_PROPS);
        keyStoreProps.remove(SSL_KEYSTORE_KEY_CONFIG);
        keyStoreProps.remove(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG);
        keyStoreProps.forEach(configName -> {
            ConfigEntry desc = configEntry(configDesc, prefix + configName);
            boolean isSensitive = configName.contains("password");
            verifyConfig(configName, desc, isSensitive, !prefix.isEmpty(), expectedProps);
            Optional<String> defaultValue = configName.equals(SSL_KEYSTORE_TYPE_CONFIG) ? Optional.of("JKS") : Optional.empty();
            verifySynonyms(configName, desc.synonyms(), isSensitive, prefix, defaultValue);
        });
    }

    List<Entry<String, ConfigEntry.ConfigSource>> synonymsList(ConfigEntry configEntry) {
        return configEntry.synonyms().stream().map(s -> new SimpleImmutableEntry<>(s.name(), s.source())).collect(Collectors.toList());
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testConfigDescribeUsingAdminClient(String quorum) throws Exception {
        Admin adminClient = adminClients().head();
        alterSslKeystoreUsingConfigCommand(sslProperties1(), SecureExternal());

        Config configDesc = TestUtils.tryUntilNoAssertionError(DEFAULT_MAX_WAIT_MS, 100, () -> {
            Config describeConfigsResult = describeConfig(adminClient, servers());
            verifySslConfig("listener.name.external.", sslProperties1(), describeConfigsResult);
            verifySslConfig("", invalidSslProperties(), describeConfigsResult);
            return describeConfigsResult;
        });

        // Verify a few log configs with and without synonyms
        Properties expectedProps = new Properties();
        expectedProps.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "1680000000");
        expectedProps.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, "168");
        expectedProps.setProperty(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, "168");
        expectedProps.setProperty(CleanerConfig.LOG_CLEANER_THREADS_PROP, "1");
        ConfigEntry logRetentionMs = configEntry(configDesc, ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG);
        verifyConfig(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, logRetentionMs,
            false, false, expectedProps);
        ConfigEntry logRetentionHours = configEntry(configDesc, ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG);
        verifyConfig(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, logRetentionHours,
            false, true, expectedProps);
        ConfigEntry logRollHours = configEntry(configDesc, ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG);
        verifyConfig(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, logRollHours,
            false, true, expectedProps);
        ConfigEntry logCleanerThreads = configEntry(configDesc, CleanerConfig.LOG_CLEANER_THREADS_PROP);
        verifyConfig(CleanerConfig.LOG_CLEANER_THREADS_PROP, logCleanerThreads,
            false, false, expectedProps);

        assertEquals(Arrays.asList(
                new SimpleImmutableEntry<>(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
                new SimpleImmutableEntry<>(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
                new SimpleImmutableEntry<>(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigEntry.ConfigSource.DEFAULT_CONFIG)),
            synonymsList(logRetentionMs));
        assertEquals(Arrays.asList(
                new SimpleImmutableEntry<>(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
                new SimpleImmutableEntry<>(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigEntry.ConfigSource.DEFAULT_CONFIG)),
            synonymsList(logRetentionHours));
        assertEquals(Arrays.asList(new SimpleImmutableEntry<>(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, ConfigEntry.ConfigSource.DEFAULT_CONFIG)), synonymsList(logRollHours));
        assertEquals(Arrays.asList(new SimpleImmutableEntry<>(CleanerConfig.LOG_CLEANER_THREADS_PROP, ConfigEntry.ConfigSource.DEFAULT_CONFIG)), synonymsList(logCleanerThreads));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testUpdatesUsingConfigProvider(String quorum) throws Exception {
        String pollingIntervalVal = "${file:polling.interval:interval}";
        String pollingIntervalUpdateVal = "${file:polling.interval:updinterval}";
        String sslTruststoreTypeVal = "${file:ssl.truststore.type:storetype}";
        String sslKeystorePasswordVal = "${file:ssl.keystore.password:password}";

        String configPrefix = listenerPrefix(SecureExternal());
        Collection<ConfigEntry> brokerConfigs = describeConfig(adminClients().head(), servers()).entries();
        // the following are values before updated
        assertFalse(brokerConfigs.stream().anyMatch(__ -> Objects.equals(__.name(), TestMetricsReporter.PollingIntervalProp())), "Initial value of polling interval");
        assertFalse(brokerConfigs.stream().anyMatch(__ -> Objects.equals(__.name(), configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)), "Initial value of ssl truststore type");

        // setup ssl properties
        Properties secProps = securityProps(sslProperties1(), KEYSTORE_PROPS, configPrefix);

        // configure config providers and properties need be updated
        Properties updatedProps = new Properties();
        updatedProps.setProperty("config.providers", "file");
        updatedProps.setProperty("config.providers.file.class", "kafka.server.MockFileConfigProvider");
        updatedProps.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, TestMetricsReporter.class.getName());

        // 1. update Integer property using config provider
        updatedProps.put(TestMetricsReporter.PollingIntervalProp(), pollingIntervalVal);

        // 2. update String property using config provider
        updatedProps.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslTruststoreTypeVal);

        // merge two properties
        updatedProps.putAll(secProps);

        // 3. update password property using config provider
        updatedProps.put(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePasswordVal);

        alterConfigsUsingConfigCommand(updatedProps);
        waitForConfig(TestMetricsReporter.PollingIntervalProp(), "1000", 10000);
        waitForConfig(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS", 10000);
        waitForConfig(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "ServerPassword", 10000);

        // wait for MetricsReporter
        scala.collection.immutable.List<TestMetricsReporter> reporters = TestMetricsReporter.waitForReporters(servers().size());
        reporters.foreach(reporter -> {
            reporter.verifyState(0, 0, 1000);
            assertFalse(reporter.kafkaMetrics().isEmpty(), "No metrics found");
            return null;
        });

        if (!isKRaftTest()) {
            // fetch from ZK, values should be unresolved
            Properties props = fetchBrokerConfigsFromZooKeeper(servers().head());
            assertEquals(pollingIntervalVal, props.getProperty(TestMetricsReporter.PollingIntervalProp()), "polling interval is not updated in ZK");
            assertEquals(sslTruststoreTypeVal, props.getProperty(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), "store type is not updated in ZK");
            assertEquals(sslKeystorePasswordVal, props.getProperty(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), "keystore password is not updated in ZK");
        }

        // verify the update
        // 1. verify update not occurring if the value of property is same.
        alterConfigsUsingConfigCommand(updatedProps);
        waitForConfig(TestMetricsReporter.PollingIntervalProp(), "1000", 10000);
        reporters.foreach(reporter -> {
            reporter.verifyState(0, 0, 1000);
            return null;
        });

        // 2. verify update occurring if the value of property changed.
        updatedProps.put(TestMetricsReporter.PollingIntervalProp(), pollingIntervalUpdateVal);
        alterConfigsUsingConfigCommand(updatedProps);
        waitForConfig(TestMetricsReporter.PollingIntervalProp(), "2000", 10000);
        reporters.foreach(reporter -> {
            reporter.verifyState(1, 0, 2000);
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testKeyStoreAlter(String quorum) throws Exception {
        String topic2 = "testtopic2";
        TestUtils.createTopicWithAdmin(adminClients().head(), topic2, servers(), controllerServers(), numPartitions(), numServers(), (Map<Object, Seq<Object>>) Map$.MODULE$.empty(), new Properties());

        // Start a producer and consumer that work with the current broker keystore.
        // This should continue working while changes are made
        Tuple2<ProducerThread, ConsumerThread> producerConsumerThread = startProduceConsume(0, "test-producer");
        TestUtils.waitUntilTrue(() -> producerConsumerThread._2().received() >= 10, () -> "Messages not received", DEFAULT_MAX_WAIT_MS, 100L);

        // Producer with new truststore should fail to connect before keystore update
        KafkaProducer<String, String> producer1 = new ProducerBuilder().maxRetries(0).trustStoreProps(sslProperties2()).build();
        verifyAuthenticationFailure(producer1);

        // Update broker keystore for external listener
        alterSslKeystoreUsingConfigCommand(sslProperties2(), SecureExternal());

        // New producer with old truststore should fail to connect
        KafkaProducer<?, ?> producer2 = new ProducerBuilder().maxRetries(0).trustStoreProps(sslProperties1()).build();
        verifyAuthenticationFailure(producer2);

        // Produce/consume should work with new truststore with new producer/consumer
        KafkaProducer<String, String> producer = new ProducerBuilder().maxRetries(0).trustStoreProps(sslProperties2()).build();
        // Start the new consumer in a separate group than the continuous consumer started at the beginning of the test so
        // that it is not disrupted by rebalance.
        Consumer<String, String> consumer = new ConsumerBuilder("group2").topic(topic2).trustStoreProps(sslProperties2()).build();
        verifyProduceConsume(producer, consumer, 10, topic2);

        // Broker keystore update for internal listener with incompatible keystore should fail without update
        alterSslKeystore(sslProperties2(), SecureInternal(), true);
        verifyProduceConsume(producer, consumer, 10, topic2);

        // Broker keystore update for internal listener with compatible keystore should succeed
        Properties sslPropertiesCopy = (Properties) sslProperties1().clone();
        File oldFile = new File(sslProperties1().getProperty(SSL_KEYSTORE_LOCATION_CONFIG));
        File newFile = TestUtils.tempFile("keystore", ".jks");
        Files.copy(oldFile.toPath(), newFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        sslPropertiesCopy.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, newFile.getPath());
        alterSslKeystore(sslPropertiesCopy, SecureInternal(), false);
        verifyProduceConsume(producer, consumer, 10, topic2);

        // Verify that keystores can be updated using same file name.
        Properties reusableProps = (Properties) sslProperties2().clone();
        File reusableFile = TestUtils.tempFile("keystore", ".jks");
        reusableProps.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, reusableFile.getPath());
        Files.copy(new File(sslProperties1().getProperty(SSL_KEYSTORE_LOCATION_CONFIG)).toPath(),
            reusableFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        alterSslKeystore(reusableProps, SecureExternal(), false);
        KafkaProducer<String, String> producer3 = new ProducerBuilder().maxRetries(0).trustStoreProps(sslProperties2()).build();
        verifyAuthenticationFailure(producer3);
        // Now alter using same file name. We can't check if the update has completed by comparing config on
        // the broker, so we wait for producer operation to succeed to verify that the update has been performed.
        Files.copy(new File(sslProperties2().getProperty(SSL_KEYSTORE_LOCATION_CONFIG)).toPath(),
            reusableFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        reusableFile.setLastModified(System.currentTimeMillis() + 1000);
        alterSslKeystore(reusableProps, SecureExternal(), false);
        TestUtils.waitUntilTrue(() -> {
            try {
                return producer3.partitionsFor(topic()).size() == numPartitions();
            } catch (Exception e) {
                return false;
            }
        }, () -> "Keystore not updated", DEFAULT_MAX_WAIT_MS, 100);

        // Verify that all messages sent with retries=0 while keystores were being altered were consumed
        stopAndVerifyProduceConsume(producerConsumerThread._1(), producerConsumerThread._2(), false);
    }

    public void verifyConfiguration(boolean enabled) {
        servers().foreach(server -> {
            TestUtils.waitUntilTrue(
                () -> server.logManager().producerStateManagerConfig().transactionVerificationEnabled() == enabled,
                () -> "Configuration was not updated.", DEFAULT_MAX_WAIT_MS, 100);
            return null;
        });
        verifyThreads("AddPartitionsToTxnSenderThread-", 1, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testTransactionVerificationEnable(String quorum) throws Exception {
        // Verification enabled by default
        verifyConfiguration(true);

        // Dynamically turn verification off.
        String configPrefix = listenerPrefix(SecureExternal());
        Properties updatedProps = securityProps(sslProperties1(), KEYSTORE_PROPS, configPrefix);
        updatedProps.put(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "false");
        alterConfigsUsingConfigCommand(updatedProps);
        verifyConfiguration(false);

        // Ensure it remains off after shutdown.
        KafkaBroker shutdownServer = servers().head();
        shutdownServer.shutdown();
        shutdownServer.awaitShutdown();
        shutdownServer.startup();
        verifyConfiguration(false);

        // Turn verification back on.
        updatedProps.put(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "true");
        alterConfigsUsingConfigCommand(updatedProps);
        verifyConfiguration(true);
    }

    private ConfigEntry configEntry(Config configDesc, String configName) {
        return configDesc.entries().stream()
            .filter(cfg -> cfg.name().equals(configName))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Config not found " + configName));
    }

    private void alterSslKeystore(Properties props, String listener, boolean expectFailure) {
        String configPrefix = listenerPrefix(listener);
        Properties newProps = securityProps(props, KEYSTORE_PROPS, configPrefix);
        reconfigureServers(newProps, true,
            new Tuple2<>(configPrefix + SSL_KEYSTORE_LOCATION_CONFIG, props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)), expectFailure);
    }

    public void alterSslKeystoreUsingConfigCommand(Properties props, String listener) throws IOException {
        String configPrefix = listenerPrefix(listener);
        Properties newProps = securityProps(props, KEYSTORE_PROPS, configPrefix);
        alterConfigsUsingConfigCommand(newProps);
        waitForConfig(configPrefix + SSL_KEYSTORE_LOCATION_CONFIG, props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG), 10000);
    }

    private void alterConfigsUsingConfigCommand(Properties props) throws IOException {
        File propsFile = tempPropertiesFile(clientProps(SecurityProtocol.SSL, scala.None$.empty()));

        String propsStr = props.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(","));

        servers().foreach(server -> {
            String[] args = new String[]{"--bootstrap-server", TestUtils.bootstrapServers(servers(), new ListenerName(SecureInternal())),
                "--command-config", propsFile.getAbsolutePath(),
                "--alter", "--add-config", propsStr,
                "--entity-type", "brokers",
                "--entity-name", "" + server.config().brokerId()};
            ConfigCommand.main(args);
            return null;
        });
    }

    private File tempPropertiesFile(Properties properties) throws IOException {
        return org.apache.kafka.test.TestUtils.tempFile(properties.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(System.lineSeparator())));
    }
}
