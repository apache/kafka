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
package org.apache.kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KRaftClusterTest {

    @Test
    public void testAuthorizerFailureFoundInControllerStartup() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumControllerNodes(3).build()).
            setConfigProp("authorizer.class.name", BadAuthorizer.class.getName()).build()) {
            cluster.format();
            ExecutionException exception = assertThrows(ExecutionException.class,
                cluster::startup);
            assertEquals("java.lang.IllegalStateException: test authorizer exception",
                exception.getMessage());
            cluster.fatalFaultHandler().setIgnore(true);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReconfigureControllerClientQuotas(boolean combinedController) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setCombined(combinedController)
                .setNumControllerNodes(1)
                .build())
            .setConfigProp("client.quota.callback.class", DummyClientQuotaCallback.class.getName())
            .setConfigProp(DummyClientQuotaCallback.DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY, "0")
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            assertConfigValue(cluster, 0);

            try (Admin admin = Admin.create(cluster.clientProperties())) {
                admin.incrementalAlterConfigs(
                    Map.of(new ConfigResource(Type.BROKER, ""),
                        List.of(new AlterConfigOp(
                            new ConfigEntry(DummyClientQuotaCallback.DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY, "1"), OpType.SET))))
                        .all().get();
            }
            assertConfigValue(cluster, 1);
        }
    }

    private void assertConfigValue(KafkaClusterTestKit cluster, int expected) throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(60000, () -> {
            Object controllerCallback = cluster.controllers().values().iterator().next()
                .quotaManagers().clientQuotaCallbackPlugin().get().get();
            assertEquals(expected, ((DummyClientQuotaCallback) controllerCallback).value);

            Object brokerCallback = cluster.brokers().values().iterator().next()
                .quotaManagers().clientQuotaCallbackPlugin().get().get();
            assertEquals(expected, ((DummyClientQuotaCallback) brokerCallback).value);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReconfigureControllerAuthorizer(boolean combinedMode) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setCombined(combinedMode)
                .setNumControllerNodes(1)
                .build())
            .setConfigProp("authorizer.class.name", FakeConfigurableAuthorizer.class.getName())
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            assertFoobarValue(cluster, 0);

            try (Admin admin = Admin.create(cluster.clientProperties())) {
                admin.incrementalAlterConfigs(
                    Map.of(new ConfigResource(Type.BROKER, ""),
                        List.of(new AlterConfigOp(
                            new ConfigEntry(FakeConfigurableAuthorizer.FOOBAR_CONFIG_KEY, "123"), OpType.SET))))
                    .all().get();
            }

            assertFoobarValue(cluster, 123);
        }
    }

    private void assertFoobarValue(KafkaClusterTestKit cluster, int expected) throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(60000, () -> {
            // 获取控制器的值
            Object controllerAuthorizer = cluster.controllers().values().iterator().next()
                .authorizerPlugin().get().get();
            assertEquals(expected, ((FakeConfigurableAuthorizer) controllerAuthorizer).foobar.get());

            // 获取Broker的值
            Object brokerAuthorizer = cluster.brokers().values().iterator().next()
                .authorizerPlugin().get().get();
            assertEquals(expected, ((FakeConfigurableAuthorizer) brokerAuthorizer).foobar.get());
        });
    }

    public static class BadAuthorizer implements Authorizer {
        // Default constructor needed for reflection object creation
        public BadAuthorizer() {
        }

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            throw new IllegalStateException("test authorizer exception");
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
            return null;
        }

        @Override
        public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings) {
            return null;
        }

        @Override
        public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext,
            List<AclBindingFilter> aclBindingFilters) {
            return null;
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return null;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    public static class DummyClientQuotaCallback implements ClientQuotaCallback, Reconfigurable {
        // Default constructor needed for reflection object creation
        public DummyClientQuotaCallback() {
        }

        public static final String DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY = "dummy.client.quota.callback.value";

        private int value = 0;

        @Override
        public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
            return Collections.emptyMap();
        }

        @Override
        public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
            return 1.0;
        }

        @Override
        public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {
        }

        @Override
        public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
        }

        @Override
        public boolean quotaResetRequired(ClientQuotaType quotaType) {
            return true;
        }

        @Override
        public boolean updateClusterMetadata(Cluster cluster) {
            return false;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            Object newValue = configs.get(DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY);
            if (newValue != null) {
                value = Integer.parseInt(newValue.toString());
            }
        }

        @Override
        public Set<String> reconfigurableConfigs() {
            return Collections.singleton(DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY);
        }

        @Override
        public void validateReconfiguration(Map<String, ?> configs) {
        }

        @Override
        public void reconfigure(Map<String, ?> configs) {
            configure(configs);
        }
    }

    public static class FakeConfigurableAuthorizer implements Authorizer, Reconfigurable {
        // Default constructor needed for reflection object creation
        public FakeConfigurableAuthorizer() {
        }

        public static final String FOOBAR_CONFIG_KEY = "fake.configurable.authorizer.foobar.config";

        private final AtomicInteger foobar = new AtomicInteger(0);

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            return serverInfo.endpoints().stream()
                .collect(Collectors.toMap(
                    endpoint -> endpoint,
                    endpoint -> {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        future.complete(null);
                        return future;
                    }
                ));
        }

        @Override
        public Set<String> reconfigurableConfigs() {
            return Collections.singleton(FOOBAR_CONFIG_KEY);
        }

        @Override
        public void validateReconfiguration(Map<String, ?> configs) {
            fakeConfigurableAuthorizerConfigToInt(configs);
        }

        @Override
        public void reconfigure(Map<String, ?> configs) {
            foobar.set(fakeConfigurableAuthorizerConfigToInt(configs));
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext,
            List<Action> actions
        ) {
            return actions.stream()
                .map(action -> AuthorizationResult.ALLOWED)
                .collect(Collectors.toList());
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return Collections.emptyList();
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            foobar.set(fakeConfigurableAuthorizerConfigToInt(configs));
        }

        @Override
        public List<? extends CompletionStage<AclCreateResult>> createAcls(
            AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings
        ) {
            return Collections.emptyList();
        }

        @Override
        public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
            AuthorizableRequestContext requestContext,
            List<AclBindingFilter> aclBindingFilters
        ) {
            return Collections.emptyList();
        }

        private int fakeConfigurableAuthorizerConfigToInt(Map<String, ?> configs) {
            Object value = configs.get(FOOBAR_CONFIG_KEY);
            if (value == null) {
                return 0;
            } else {
                return Integer.parseInt(value.toString());
            }
        }
    }
}
