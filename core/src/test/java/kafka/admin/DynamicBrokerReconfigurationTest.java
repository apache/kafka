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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSynonym;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.None$;
import scala.collection.JavaConverters;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.network.CertStores.KEYSTORE_PROPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicBrokerReconfigurationTest extends AbstractDynamicBrokerReconfigurationTest {
    public void verifyConfig(String configName, ConfigEntry configEntry, boolean isSensitive, boolean isReadOnly,
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

    public void verifySynonym(String configName, ConfigSynonym synonym, boolean isSensitive,
                              String expectedPrefix, ConfigSource expectedSource, Properties expectedProps) {
        if (isSensitive)
            assertNull(synonym.value(), "Sensitive value returned for " + configName);
        else
            assertEquals(expectedProps.getProperty(configName), synonym.value());
        assertTrue(synonym.name().startsWith(expectedPrefix), "Expected listener config, got " + synonym);
        assertEquals(expectedSource, synonym.source());
    }

    public void verifySynonyms(String configName, List<ConfigSynonym> synonyms, boolean isSensitive,
                               String prefix, Optional<String> defaultValue) {
        int overrideCount = prefix.isEmpty() ? 0 : 2;
        assertEquals(1 + overrideCount + defaultValue.map(__ -> 1).orElse(0), synonyms.size(), "Wrong synonyms for " + configName + ": " + synonyms);
        if (overrideCount > 0) {
            String listenerPrefix = "listener.name.external.ssl.";
            verifySynonym(configName, synonyms.get(0), isSensitive, listenerPrefix, ConfigSource.DYNAMIC_BROKER_CONFIG, sslProperties1());
            verifySynonym(configName, synonyms.get(1), isSensitive, listenerPrefix, ConfigSource.STATIC_BROKER_CONFIG, sslProperties1());
        }
        verifySynonym(configName, synonyms.get(overrideCount), isSensitive, "ssl.", ConfigSource.STATIC_BROKER_CONFIG, invalidSslProperties());
        defaultValue.ifPresent(value -> {
            Properties defaultProps = new Properties();
            defaultProps.setProperty(configName, value);
            verifySynonym(configName, synonyms.get(overrideCount + 1), isSensitive, "ssl.", ConfigSource.DEFAULT_CONFIG, defaultProps);
        });
    }

    public void verifySslConfig(String prefix, Properties expectedProps, Config configDesc) {
        // Validate file-based SSL keystore configs
        Set<String> keyStoreProps = new HashSet<>(KEYSTORE_PROPS);
        keyStoreProps.remove(SSL_KEYSTORE_KEY_CONFIG);
        keyStoreProps.remove(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG);
        keyStoreProps.forEach(configName -> {
            ConfigEntry desc = configEntry(configDesc, prefix + configName);
            boolean isSensitive = configName.contains("password");
            verifyConfig(configName, desc, isSensitive, !prefix.isEmpty(), expectedProps);
            Optional<String> defaultValue =
                configName.equals(SSL_KEYSTORE_TYPE_CONFIG) ? Optional.of("JKS") : Optional.empty();
            verifySynonyms(configName, desc.synonyms(), isSensitive, prefix, defaultValue);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testConfigDescribeUsingAdminClient(String quorum) {
        Admin adminClient = adminClients().head();
        alterSslKeystoreUsingConfigCommand(sslProperties1(), SecureExternal());

        Config configDesc = kafka.utils.TestUtils.tryUntilNoAssertionError(TestUtils.DEFAULT_MAX_WAIT_MS, 100, () -> {
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

        Function<ConfigEntry, List<Tuple2<String, ConfigSource>>> synonymsList =
            configEntry -> configEntry.synonyms().stream()
                .map(s -> new Tuple2<>(s.name(), s.source()))
                .collect(Collectors.toList());
        assertEquals(Arrays.asList(
                new Tuple2<>(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, ConfigSource.STATIC_BROKER_CONFIG),
                new Tuple2<>(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigSource.STATIC_BROKER_CONFIG),
                new Tuple2<>(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigSource.DEFAULT_CONFIG)),
            synonymsList.apply(logRetentionMs));
        assertEquals(Arrays.asList(
                new Tuple2<>(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigSource.STATIC_BROKER_CONFIG),
                new Tuple2<>(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigSource.DEFAULT_CONFIG)),
            synonymsList.apply(logRetentionHours));
        assertEquals(Collections.singletonList(new Tuple2<>(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, ConfigSource.DEFAULT_CONFIG)), synonymsList.apply(logRollHours));
        assertEquals(Collections.singletonList(new Tuple2<>(CleanerConfig.LOG_CLEANER_THREADS_PROP, ConfigSource.DEFAULT_CONFIG)), synonymsList.apply(logCleanerThreads));
    }

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testUpdatesUsingConfigProvider(String quorum) {
        String pollingIntervalVal = "${file:polling.interval:interval}";
        String pollingIntervalUpdateVal = "${file:polling.interval:updinterval}";
        String sslTruststoreTypeVal = "${file:ssl.truststore.type:storetype}";
        String sslKeystorePasswordVal = "${file:ssl.keystore.password:password}";

        String configPrefix = listenerPrefix(SecureExternal());
        Collection<ConfigEntry> brokerConfigs = describeConfig(adminClients().head(), servers()).entries();
        // the following are values before updated
        assertFalse(brokerConfigs.stream().anyMatch(p -> Objects.equals(p.name(), TestMetricsReporter.PollingIntervalProp())), "Initial value of polling interval");
        assertFalse(brokerConfigs.stream().anyMatch(p -> Objects.equals(p.name(), configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)), "Initial value of ssl truststore type");
        Optional<ConfigEntry> sslKeystorePasswordProp = brokerConfigs.stream().filter(p -> Objects.equals(p.name(), configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).findFirst();
        assertTrue(sslKeystorePasswordProp.isPresent());
        assertNull(sslKeystorePasswordProp.get().value(), "Initial value of ssl keystore password");

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
            assertEquals(props.getProperty(TestMetricsReporter.PollingIntervalProp()), pollingIntervalVal, "polling interval is not updated in ZK");
            assertEquals(props.getProperty(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), sslTruststoreTypeVal, "store type is not updated in ZK");
            assertEquals(props.getProperty(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), sslKeystorePasswordVal, "keystore password is not updated in ZK");
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

    @ParameterizedTest
    @ValueSource(strings = {"zk", "kraft"})
    public void testTransactionVerificationEnable(String quorum) {
        Consumer<Boolean> verifyConfiguration = enabled -> {
            servers().foreach(server -> {
                try {
                    TestUtils.waitForCondition(
                        () -> server.logManager().producerStateManagerConfig().transactionVerificationEnabled() == enabled,
                        () -> "Configuration was not updated.");
                    return null;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            verifyThreads("AddPartitionsToTxnSenderThread-", 1, 0);
        };
        // Verification enabled by default
        verifyConfiguration.accept(true);

        // Dynamically turn verification off.
        String configPrefix = listenerPrefix(SecureExternal());
        Properties updatedProps = securityProps(sslProperties1(), KEYSTORE_PROPS, configPrefix);
        updatedProps.put(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "false");
        alterConfigsUsingConfigCommand(updatedProps);
        verifyConfiguration.accept(false);

        // Ensure it remains off after shutdown.
        KafkaBroker shutdownServer = servers().head();
        shutdownServer.shutdown();
        shutdownServer.awaitShutdown();
        shutdownServer.startup();
        verifyConfiguration.accept(false);

        // Turn verification back on.
        updatedProps.put(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "true");
        alterConfigsUsingConfigCommand(updatedProps);
        verifyConfiguration.accept(true);
    }

    private void alterSslKeystoreUsingConfigCommand(Properties props, String listener) {
        String configPrefix = listenerPrefix(listener);
        Properties newProps = securityProps(props, KEYSTORE_PROPS, configPrefix);
        alterConfigsUsingConfigCommand(newProps);
        waitForConfig(configPrefix + SSL_KEYSTORE_LOCATION_CONFIG, props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG), 10000);
    }

    @SuppressWarnings({"deprecation", "unchecked"}) // Added for Scala 2.12 compatibility for usages of JavaConverters
    private void alterConfigsUsingConfigCommand(Properties props) {
        File propsFile = kafka.utils.TestUtils.tempPropertiesFile(JavaConverters.mapAsScalaMap((Map) clientProps(SecurityProtocol.SSL, None$.empty())));

        servers().foreach(server -> {
            String[] args = new String[]{"--bootstrap-server", kafka.utils.TestUtils.bootstrapServers(servers(), new ListenerName(SecureInternal())),
                "--command-config", propsFile.getAbsolutePath(),
                "--alter", "--add-config", props.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(",")),
                "--entity-type", "brokers",
                "--entity-name", Integer.toString(server.config().brokerId())};
            ConfigCommand.main(args);
            return null;
        });
    }
}
