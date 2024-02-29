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
package org.apache.kafka.tools.config;

import kafka.server.AbstractDynamicBrokerReconfigurationTest;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.server.TestMetricsReporter;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.immutable.List;

import java.io.File;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.network.CertStores.KEYSTORE_PROPS;
import static org.apache.kafka.tools.config.ConfigCommandIntegrationTest.TEST_WITH_PARAMETERIZED_QUORUM_NAME;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicBrokerReconfigurationTest extends AbstractDynamicBrokerReconfigurationTest {
    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testUpdatesUsingConfigProvider(String quorum) {
/*
        String PollingIntervalVal = f"$${file:polling.interval:interval}";
        String PollingIntervalUpdateVal = f"$${file:polling.interval:updinterval}";
        String SslTruststoreTypeVal = f"$${file:ssl.truststore.type:storetype}";
        String SslKeystorePasswordVal = f"$${file:ssl.keystore.password:password}";
*/
        String PollingIntervalVal = "$${file:polling.interval:interval}";
        String PollingIntervalUpdateVal = "$${file:polling.interval:updinterval}";
        String SslTruststoreTypeVal = "$${file:ssl.truststore.type:storetype}";
        String SslKeystorePasswordVal = "$${file:ssl.keystore.password:password}";

        String configPrefix = listenerPrefix(SecureExternal());
        Collection<ConfigEntry> brokerConfigs = describeConfig(adminClients().head(), servers()).entries();
        // the following are values before updated
        assertFalse(brokerConfigs.stream().anyMatch(p -> Objects.equals(p.name(), TestMetricsReporter.PollingIntervalProp())), "Initial value of polling interval");
        assertFalse(brokerConfigs.stream().anyMatch(p -> Objects.equals(p.name(), configPrefix + KafkaConfig.SslTruststoreTypeProp())), "Initial value of ssl truststore type");
        Optional<ConfigEntry> sslKeystorePasswordProp = brokerConfigs.stream().filter(p -> Objects.equals(p.name(), configPrefix + KafkaConfig.SslKeystorePasswordProp())).findFirst();
        assertTrue(sslKeystorePasswordProp.isPresent());
        assertNull(sslKeystorePasswordProp.get().value(), "Initial value of ssl keystore password");

        // setup ssl properties
        Properties secProps = securityProps(sslProperties1(), KEYSTORE_PROPS, configPrefix);

        // configure config providers and properties need be updated
        Properties updatedProps = new Properties();
        updatedProps.setProperty("config.providers", "file");
        updatedProps.setProperty("config.providers.file.class", "kafka.server.MockFileConfigProvider");
        updatedProps.put(KafkaConfig.MetricReporterClassesProp(), TestMetricsReporter.class.getName());

        // 1. update Integer property using config provider
        updatedProps.put(TestMetricsReporter.PollingIntervalProp(), PollingIntervalVal);

        // 2. update String property using config provider
        updatedProps.put(configPrefix+KafkaConfig.SslTruststoreTypeProp(), SslTruststoreTypeVal);

        // merge two properties
        updatedProps.putAll(secProps);

        // 3. update password property using config provider
        updatedProps.put(configPrefix+KafkaConfig.SslKeystorePasswordProp(), SslKeystorePasswordVal);

        alterConfigsUsingConfigCommand(updatedProps);
        waitForConfig(TestMetricsReporter.PollingIntervalProp(), "1000", 10000);
        waitForConfig(configPrefix+KafkaConfig.SslTruststoreTypeProp(), "JKS", 10000);
        waitForConfig(configPrefix+KafkaConfig.SslKeystorePasswordProp(), "ServerPassword", 10000);

        // wait for MetricsReporter
        List<TestMetricsReporter> reporters = TestMetricsReporter.waitForReporters(servers().size());
        reporters.foreach(reporter -> {
            reporter.verifyState(0, 0, 1000);
            assertFalse(reporter.kafkaMetrics().isEmpty(), "No metrics found");
            return null;
        });

        if (!isKRaftTest()) {
            // fetch from ZK, values should be unresolved
            Properties props = fetchBrokerConfigsFromZooKeeper(servers().head());
            assertTrue(props.getProperty(TestMetricsReporter.PollingIntervalProp()) == PollingIntervalVal, "polling interval is not updated in ZK");
            assertTrue(props.getProperty(configPrefix + KafkaConfig.SslTruststoreTypeProp()) == SslTruststoreTypeVal, "store type is not updated in ZK");
            assertTrue(props.getProperty(configPrefix + KafkaConfig.SslKeystorePasswordProp()) == SslKeystorePasswordVal, "keystore password is not updated in ZK");
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
        updatedProps.put(TestMetricsReporter.PollingIntervalProp(), PollingIntervalUpdateVal);
        alterConfigsUsingConfigCommand(updatedProps);
        waitForConfig(TestMetricsReporter.PollingIntervalProp(), "2000", 10000);
        reporters.foreach(reporter -> {
            reporter.verifyState(1, 0, 2000);
            return null;
        });
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testTransactionVerificationEnable(String quorum) {
        Consumer<Boolean> verifyConfiguration = (enabled) -> {
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
        updatedProps.put(KafkaConfig.TransactionPartitionVerificationEnableProp(), "false");
        alterConfigsUsingConfigCommand(updatedProps);
        verifyConfiguration.accept(false);

        // Ensure it remains off after shutdown.
        KafkaBroker shutdownServer = servers().head();
        shutdownServer.shutdown();
        shutdownServer.awaitShutdown();
        shutdownServer.startup();
        verifyConfiguration.accept(false);

        // Turn verification back on.
        updatedProps.put(KafkaConfig.TransactionPartitionVerificationEnableProp(), "true");
        alterConfigsUsingConfigCommand(updatedProps);
        verifyConfiguration.accept(true);
    }

    private void alterSslKeystoreUsingConfigCommand(Properties props, String listener) {
        String configPrefix = listenerPrefix(listener);
        Properties newProps = securityProps(props, KEYSTORE_PROPS, configPrefix);
        alterConfigsUsingConfigCommand(newProps);
        waitForConfig(configPrefix + SSL_KEYSTORE_LOCATION_CONFIG, props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG), 10000);
    }

    private void alterConfigsUsingConfigCommand(Properties props) {
        File propsFile = kafka.utils.TestUtils.tempPropertiesFile(clientProps(SecurityProtocol.SSL, scala.None$.empty()));

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
