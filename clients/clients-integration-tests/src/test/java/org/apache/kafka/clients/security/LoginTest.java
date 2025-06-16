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
package org.apache.kafka.clients.security;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.Login;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.AbstractLogin;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CLASS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoginTest {

    private static final String USERNAME = "userA";
    private static final String PASSWORD = "pwd";
    private static final String CLIENT_ID = "test-login-client";
    private static final String LISTENER_PREFIX = "listener.name.controller.";
    private static final String EXTERNAL_PREFIX = "listener.name.external.";
    private static final String MECHANISMS = "PLAIN";
    private static final String MECHANISMS_PREFIX = "plain.";
    private static final String SASL_JAAS = "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "user_" + USERNAME + "=\"" + PASSWORD + "\";";

    @ClusterTest(
        types = {Type.CO_KRAFT},
        controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        serverProperties = {
            @ClusterConfigProperty(key = LISTENER_PREFIX + MECHANISMS_PREFIX + SASL_LOGIN_CLASS, value = "org.apache.kafka.clients.security.LoginTest$CustomerLogin"),
            @ClusterConfigProperty(key = EXTERNAL_PREFIX + MECHANISMS_PREFIX + SASL_LOGIN_CLASS, value = "org.apache.kafka.clients.security.LoginTest$CustomerLogin"),
            @ClusterConfigProperty(key = SASL_ENABLED_MECHANISMS_CONFIG, value = MECHANISMS),
            @ClusterConfigProperty(key = SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, value = MECHANISMS),
            @ClusterConfigProperty(key = LISTENER_PREFIX + MECHANISMS_PREFIX + SASL_JAAS_CONFIG, value = SASL_JAAS),
            @ClusterConfigProperty(key = AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.clients.security.LoginTest$CustomerAuthorizer"),
        }
    )
    public void testCustomLoginWithKafkaCluster(ClusterInstance cluster) {
        try (Admin admin = cluster.admin(saslConfig())) {
            int found = 0;
            for (Metric metric : admin.metrics().values()) {
                found += assertMetricName(
                    metric.metricName(), 
                    expectedTags(Map.of("client-id", CLIENT_ID))
                );
            }
            assertEquals(1, found, "Expected to find 1 metric");
            
            Map<MetricName, KafkaMetric> allMetrics = Stream.of(
                cluster.controllers().get(0).metrics().metrics(),
                cluster.brokers().get(0).metrics().metrics()
            ).collect(HashMap::new, Map::putAll, Map::putAll);
            assertMetrics(
                allMetrics, 
                expectedTags(Map.of(
                    "mechanism", MECHANISMS, 
                    "listener", "EXTERNAL", 
                    "networkProcessor", "0"
                ))
            );
            assertMetrics(
                allMetrics, 
                expectedTags(Map.of(
                    "node-id", "0", 
                    "component", "raft-client"
                ))
            );
        }
    }

    private int assertMetricName(MetricName metricName, Map<String, String> expectedTags) {
        Map<String, String> tags = metricName.tags();
        if (expectedTags.equals(tags)) {
            assertEquals(CustomerLogin.METRIC_NAME, metricName.name());
            assertEquals(CustomerLogin.METRIC_DESCRIPTION, metricName.description());
            return 1;
        }
        return 0;
    }

    private void assertMetrics(Map<MetricName, KafkaMetric> metrics, Map<String, String> expectedTags) {
        int found = 0;
        for (MetricName metricName : metrics.keySet()) {
            found += assertMetricName(metricName, expectedTags);
        }
        assertEquals(1, found, "Expected to find 1 metric with the expected tags");
    }

    private static Map<String, String> expectedTags(Map<String, String> extraTags) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("config", SASL_LOGIN_CLASS);
        tags.put("class", LoginTest.CustomerLogin.class.getSimpleName());
        tags.putAll(extraTags);
        return tags;
    }

    protected static Map<String, Object> saslConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        config.put(CLIENT_ID_CONFIG, CLIENT_ID);
        config.put(SASL_MECHANISM, MECHANISMS);
        config.put(SASL_LOGIN_CLASS, LoginTest.CustomerLogin.class.getName());
        config.put(SASL_JAAS_CONFIG, SASL_JAAS);
        return config;
    }

    public static class CustomerLogin implements Login, Monitorable {

        private static final String METRIC_NAME = "monitorable-custom-login-name";
        private static final String METRIC_DESCRIPTION = "monitorable-custom-login-description";

        private String contextName;
        private Configuration configuration;
        private Subject subject;

        @Override
        public void withPluginMetrics(PluginMetrics metrics) {
            MetricName metricName = metrics.metricName(METRIC_NAME, METRIC_DESCRIPTION, new LinkedHashMap<>());
            metrics.addMetric(metricName, (Gauge<Integer>) (config, now) -> 1);
        }

        @Override
        public void configure(
            Map<String, ?> configs,
            String contextName,
            Configuration configuration,
            AuthenticateCallbackHandler loginCallbackHandler
        ) {
            this.contextName = contextName;
            this.configuration = configuration;
        }

        @Override
        public LoginContext login() throws LoginException {
            LoginContext context = new LoginContext(contextName, null, new AbstractLogin.DefaultLoginCallbackHandler(), configuration);
            context.login();
            subject = context.getSubject();
            subject.getPublicCredentials().clear();
            subject.getPrivateCredentials().clear();
            subject.getPublicCredentials().add(USERNAME);
            subject.getPrivateCredentials().add(PASSWORD);
            return context;
        }

        @Override
        public Subject subject() {
            return subject;
        }

        @Override
        public String serviceName() {
            return "customer login";
        }

        @Override
        public void close() {

        }
    }
    
    public static class CustomerAuthorizer implements Authorizer {
        
        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            return Map.of();
        }

        @Override
        public List<AuthorizationResult> authorize(
            AuthorizableRequestContext requestContext, 
            List<Action> actions
        ) {
            return IntStream.range(0, actions.size())
                .mapToObj(i -> AuthorizationResult.ALLOWED)
                .toList();
        }

        @Override
        public List<? extends CompletionStage<AclCreateResult>> createAcls(
            AuthorizableRequestContext requestContext, 
            List<AclBinding> aclBindings
        ) {
            return IntStream.range(0, aclBindings.size())
                .mapToObj(i -> CompletableFuture.completedFuture(AclCreateResult.SUCCESS))
                .toList();
        }

        @Override
        public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
            AuthorizableRequestContext requestContext, 
            List<AclBindingFilter> aclBindingFilters
        ) {
            return IntStream.range(0, aclBindingFilters.size())
                .mapToObj(i -> CompletableFuture.completedFuture(new AclDeleteResult(List.of())))
                .toList();
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return List.of();
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }
}
