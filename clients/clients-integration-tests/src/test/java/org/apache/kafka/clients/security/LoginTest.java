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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
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
import org.apache.kafka.test.TestUtils;

import org.opentest4j.AssertionFailedError;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
import static org.apache.kafka.metadata.authorizer.StandardAuthorizer.SUPER_USERS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoginTest {

    private static final String INTER_BROKER_USERNAME = "userA";
    private static final String INTER_BROKER_PASSWORD = "pwdA";
    private static final String EXTERNAL_USERNAME = "userB";
    private static final String EXTERNAL_PASSWORD = "pwdB";
    private static final String CLIENT_ID = "test-login-client";
    private static final String LISTENER_PREFIX = "listener.name.controller.";
    private static final String EXTERNAL_PREFIX = "listener.name.external.";
    private static final String MECHANISMS = "PLAIN";
    private static final String MECHANISMS_PREFIX = "plain.";
    private static final String INTER_BROKER_SASL_JAAS = "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "user_" + INTER_BROKER_USERNAME + "=\"" + INTER_BROKER_PASSWORD + "\";";
    private static final String EXTERNAL_SASL_JAAS = "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "user_" + EXTERNAL_USERNAME + "=\"" + EXTERNAL_PASSWORD + "\";";

    @ClusterTest(
        types = {Type.CO_KRAFT, Type.KRAFT},
        controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        serverProperties = {
            @ClusterConfigProperty(key = LISTENER_PREFIX + MECHANISMS_PREFIX + SASL_LOGIN_CLASS, value = "org.apache.kafka.clients.security.LoginTest$InterBrokerCustomLogin"),
            @ClusterConfigProperty(key = EXTERNAL_PREFIX + MECHANISMS_PREFIX + SASL_LOGIN_CLASS, value = "org.apache.kafka.clients.security.LoginTest$ExternalCustomLogin"),
            @ClusterConfigProperty(key = SASL_ENABLED_MECHANISMS_CONFIG, value = MECHANISMS),
            @ClusterConfigProperty(key = SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, value = MECHANISMS),
            @ClusterConfigProperty(key = LISTENER_PREFIX + MECHANISMS_PREFIX + SASL_JAAS_CONFIG, value = INTER_BROKER_SASL_JAAS),
            @ClusterConfigProperty(key = EXTERNAL_PREFIX + MECHANISMS_PREFIX + SASL_JAAS_CONFIG, value = EXTERNAL_SASL_JAAS),
            @ClusterConfigProperty(key = SUPER_USERS_CONFIG, value = "User:" + INTER_BROKER_USERNAME + ";User:" + EXTERNAL_USERNAME),
        }
    )
    public void testCustomLoginWithKafkaCluster(ClusterInstance cluster) throws InterruptedException {
        try (Admin admin = cluster.admin(Map.of(
                SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
                CLIENT_ID_CONFIG, CLIENT_ID,
                SASL_MECHANISM, MECHANISMS,
                SASL_LOGIN_CLASS, ExternalCustomLogin.class.getName(),
                SASL_JAAS_CONFIG, INTER_BROKER_SASL_JAAS))
        ) {
            assertMetrics(
                admin.metrics(),
                ExternalCustomLogin.METRIC_NAME,
                ExternalCustomLogin.METRIC_DESCRIPTION,
                expectedTags(Map.of(
                    "class", ExternalCustomLogin.class.getSimpleName(),
                    "client-id", CLIENT_ID
                ))
            );
            
            int controllerId = cluster.type() == Type.CO_KRAFT ? 0 : 3000;
            TestUtils.waitForCondition(() -> {
                try {
                    Map<MetricName, Metric> allMetrics = Stream.of(
                        cluster.controllers().get(controllerId).metrics().metrics(),
                        cluster.brokers().get(0).metrics().metrics()
                    ).collect(HashMap::new, Map::putAll, Map::putAll);
                    assertMetrics(
                        allMetrics,
                        ExternalCustomLogin.METRIC_NAME,
                        ExternalCustomLogin.METRIC_DESCRIPTION,
                        expectedTags(Map.of(
                            "mechanism", MECHANISMS,
                            "listener", "EXTERNAL",
                            "class", ExternalCustomLogin.class.getSimpleName()
                        ))
                    );
                    assertMetrics(
                        allMetrics,
                        InterBrokerCustomLogin.METRIC_NAME,
                        InterBrokerCustomLogin.METRIC_DESCRIPTION,
                        expectedTags(Map.of(
                            "class", InterBrokerCustomLogin.class.getSimpleName()
                        ))
                    );
                    return true;
                } catch (AssertionFailedError e) {
                    return false;
                }
            }, "Waiting for broker and controller metrics to be available");
        }
    }

    private int assertMetricName(
        MetricName metricName,
        String expectedName,
        String expectedDescription,
        Map<String, String> expectedTags
    ) {
        Map<String, String> tags = metricName.tags();
        if (metricName.group().equals("plugins") && expectedTags.equals(tags)) {
            assertEquals(expectedName, metricName.name());
            assertEquals(expectedDescription, metricName.description());
            return 1;
        }
        return 0;
    }

    private void assertMetrics(
        Map<MetricName, ? extends Metric> metrics,
        String expectedName,
        String expectedDescription,
        Map<String, String> expectedTags
    ) {
        int found = 0;
        for (MetricName metricName : metrics.keySet()) {
            found += assertMetricName(metricName, expectedName, expectedDescription, expectedTags);
        }
        assertEquals(1, found, "Expected to find 1 metric with the expected tags");
    }

    private static Map<String, String> expectedTags(Map<String, String> extraTags) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("config", SASL_LOGIN_CLASS);
        tags.putAll(extraTags);
        return tags;
    }

    public static class InterBrokerCustomLogin implements Login, Monitorable {

        private static final String METRIC_NAME = "monitorable-inter-broker-custom-login-name";
        private static final String METRIC_DESCRIPTION = "monitorable-inter-broker-custom-login-description";

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
            subject.getPublicCredentials().add(INTER_BROKER_USERNAME);
            subject.getPrivateCredentials().add(INTER_BROKER_PASSWORD);
            return context;
        }

        @Override
        public Subject subject() {
            return subject;
        }

        @Override
        public String serviceName() {
            return "inter broker custom login";
        }

        @Override
        public void close() {

        }
    }

    public static class ExternalCustomLogin implements Login, Monitorable {

        private static final String METRIC_NAME = "monitorable-external-custom-login-name";
        private static final String METRIC_DESCRIPTION = "monitorable-external-custom-login-description";

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
            subject.getPublicCredentials().add(EXTERNAL_USERNAME);
            subject.getPrivateCredentials().add(EXTERNAL_PASSWORD);
            return context;
        }

        @Override
        public Subject subject() {
            return subject;
        }

        @Override
        public String serviceName() {
            return "external custom login";
        }

        @Override
        public void close() {

        }
    }
}
