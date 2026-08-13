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

import org.apache.kafka.clients.CommonClientConfigs;
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
        checkAlterConfigs(Map.of(), InvalidRequestException.class, "token");
    }

    @ClusterTest
    public void testAlterConfigsWithConfigProvidersAndValidatorRejection() throws IOException {
        Files.writeString(file, "key=" + Long.MAX_VALUE);
        checkAlterConfigs(Map.of(), InvalidRequestException.class, String.valueOf(Long.MAX_VALUE));
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
        checkAlterConfigs(adminConfig, ClusterAuthorizationException.class, "token");
    }

    private void checkAlterConfigs(Map<String, Object> adminConfig, Class<? extends Throwable> expectedCause, String value) {
        try (Admin admin = clusterInstance.admin(adminConfig)) {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            Map<ConfigResource, Collection<AlterConfigOp>> alterations = Map.of(brokerResource, configProviderOps());
            ExecutionException ee = assertThrows(ExecutionException.class,
                    () -> admin.incrementalAlterConfigs(alterations).all().get());
            assertInstanceOf(expectedCause, ee.getCause());
            String message = ee.getCause().getMessage();
            assertFalse(message.contains(value));
            if (expectedCause == InvalidRequestException.class) {
                assertTrue(message.contains("${file:"));
            }
        }
    }

    private Collection<AlterConfigOp> configProviderOps() {
        return List.of(
            new AlterConfigOp(new ConfigEntry(CONFIG_PROVIDERS_CONFIG, "file"),
                    AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(
                CONFIG_PROVIDERS_CONFIG + ".file.class", FileConfigProvider.class.getName()),
                    AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(ServerConfigs.NUM_IO_THREADS_CONFIG, "${file:" + file.toAbsolutePath() + ":key}"),
                    AlterConfigOp.OpType.SET)
        );
    }
}
