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
package org.apache.kafka.clients.admin;

import kafka.server.KafkaConfig;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.JaasUtils;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.config.AbstractConfig.CONFIG_PROVIDERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AlterConfigsIntegrationTest {

    private final ClusterInstance clusterInstance;
    private Path file;

    AlterConfigsIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws IOException {
        file = Files.createTempFile("provider", ".properties");
        Files.writeString(file, "key=token");
    }

    @AfterEach
    public void teardown() throws IOException {
        Files.deleteIfExists(file);
    }

    @ClusterTest
    public void testAlterConfigsWithConfigProviders() {
        checkAlterConfigs(Map.of(), InvalidRequestException.class, "token", true);
    }

    @ClusterTest
    public void testAlterConfigsWithConfigProvidersAndValidatorRejection() throws IOException {
        Files.writeString(file, "key=" + Long.MAX_VALUE);
        checkAlterConfigs(Map.of(), InvalidRequestException.class, String.valueOf(Long.MAX_VALUE), true);
    }

    @ClusterTest
    public void testAlterConfigsRespectsConfigProviderAllowlist() {
        String previous = System.getProperty(AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY);
        try {
            System.setProperty(AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY, "none");
            checkAlterConfigs(Map.of(), InvalidRequestException.class, "token", false);
        } finally {
            if (previous == null) {
                System.clearProperty(AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY);
            } else {
                System.setProperty(AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY, previous);
            }
        }
    }

    @ClusterTest(
            brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
            controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testAlterConfigsWithConfigProvidersWithoutAcls() {
        Map<String, Object> adminConfig = Map.of(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
            SaslConfigs.SASL_MECHANISM, "PLAIN",
            SaslConfigs.SASL_JAAS_CONFIG,
                PlainLoginModule.class.getName() + " required username=\"" + JaasUtils.KAFKA_PLAIN_USER1 +
                "\" password=\"" + JaasUtils.KAFKA_PLAIN_USER1_PASSWORD + "\";");
        checkAlterConfigs(adminConfig, ClusterAuthorizationException.class, "token", true);
    }

    // Non-STRING configs resolved through a config provider must be applied by the broker, not only accepted
    // by the request validation (KAFKA-20973).

    @ClusterTest
    public void testAlterConfigsWithConfigProvidersForPerBrokerIntConfig() throws Exception {
        checkAlterConfigsApplied(
            brokerResource(),
            ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
            ServerConfigs.NUM_IO_THREADS_CONFIG,
            "10"
        );
        assertEquals(10, brokerConfig().getInt(ServerConfigs.NUM_IO_THREADS_CONFIG));
    }

    @ClusterTest
    public void testAlterConfigsWithConfigProvidersForClusterWideIntConfig() throws Exception {
        checkAlterConfigsApplied(
            new ConfigResource(ConfigResource.Type.BROKER, ""),
            ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
            ServerConfigs.NUM_IO_THREADS_CONFIG,
            "10"
        );
        assertEquals(10, brokerConfig().getInt(ServerConfigs.NUM_IO_THREADS_CONFIG));
    }

    @ClusterTest
    public void testAlterConfigsWithConfigProvidersForLongConfig() throws Exception {
        checkAlterConfigsApplied(
            brokerResource(),
            ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
            ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG,
            "86400000"
        );
        assertEquals(86400000L, brokerConfig().getLong(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG));
    }

    @ClusterTest
    public void testAlterConfigsWithConfigProvidersForDoubleConfig() throws Exception {
        checkAlterConfigsApplied(
            brokerResource(),
            ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
            CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP,
            "0.75"
        );
        assertEquals(0.75, brokerConfig().getDouble(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP));
    }

    @ClusterTest
    public void testAlterConfigsWithConfigProvidersForBooleanConfig() throws Exception {
        checkAlterConfigsApplied(
            brokerResource(),
            ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
            ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG,
            "true"
        );
        assertTrue(brokerConfig().getBoolean(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG));
    }

    @ClusterTest
    public void testAlterConfigsWithConfigProvidersForListConfig() throws Exception {
        checkAlterConfigsApplied(
            brokerResource(),
            ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
            ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG,
            "compact,delete"
        );
        assertEquals(List.of("compact", "delete"), brokerConfig().getList(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG));
    }

    private void checkAlterConfigs(Map<String, Object> adminConfig, Class<? extends Throwable> expectedCause, String value, boolean placeholder) {
        try (Admin admin = clusterInstance.admin(adminConfig)) {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            Map<ConfigResource, Collection<AlterConfigOp>> alterations =
                Map.of(brokerResource, configProviderOps(ServerConfigs.NUM_IO_THREADS_CONFIG));
            ExecutionException ee = assertThrows(ExecutionException.class,
                    () -> admin.incrementalAlterConfigs(alterations).all().get());
            assertInstanceOf(expectedCause, ee.getCause());
            String message = ee.getCause().getMessage();
            assertFalse(message.contains(value));
            if (expectedCause == InvalidRequestException.class) {
                if (placeholder) {
                    assertTrue(message.contains("${file:"));
                } else {
                    assertTrue(message.contains(FileConfigProvider.class.getName() + " is not allowed"));
                }
            }
        }
    }

    private void checkAlterConfigsApplied(
        ConfigResource target,
        ConfigEntry.ConfigSource expectedSource,
        String configName,
        String value
    ) throws Exception {
        Files.writeString(file, "key=" + value);
        ConfigResource brokerResource = brokerResource();
        try (Admin admin = clusterInstance.admin()) {
            admin.incrementalAlterConfigs(Map.of(target, configProviderOps(configName))).all().get();
            // The broker applies the update asynchronously when it replays the metadata log. DescribeConfigs is
            // served from the broker's own config, so the source only changes once the update took effect.
            TestUtils.waitForCondition(
                () -> describeConfig(admin, brokerResource, configName).source() == expectedSource,
                configName + " was not applied with source " + expectedSource
            );
            assertEquals(value, describeConfig(admin, brokerResource, configName).value());
        }
    }

    private static ConfigEntry describeConfig(Admin admin, ConfigResource resource, String name) throws Exception {
        return admin.describeConfigs(List.of(resource)).all().get().get(resource).get(name);
    }

    private int brokerId() {
        return clusterInstance.brokerIds().iterator().next();
    }

    private ConfigResource brokerResource() {
        return new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId()));
    }

    private KafkaConfig brokerConfig() {
        return clusterInstance.brokers().get(brokerId()).config();
    }

    private Collection<AlterConfigOp> configProviderOps(String configName) {
        return List.of(
            new AlterConfigOp(new ConfigEntry(CONFIG_PROVIDERS_CONFIG, "file"),
                    AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(
                CONFIG_PROVIDERS_CONFIG + ".file.class", FileConfigProvider.class.getName()),
                    AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(configName, "${file:" + file.toAbsolutePath() + ":key}"),
                    AlterConfigOp.OpType.SET)
        );
    }
}
