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

    private static final String CONTROLLER_USERNAME = "userA";
    private static final String CONTROLLER_PASSWORD = "pwdA";
    private static final String EXTERNAL_USERNAME = "userB";
    private static final String EXTERNAL_PASSWORD = "pwdB";
    private static final String CLIENT_ID = "test-login-client";
    private static final String LISTENER_PREFIX = "listener.name.controller.";
    private static final String EXTERNAL_PREFIX = "listener.name.external.";
    private static final String MECHANISMS = "PLAIN";
    private static final String MECHANISMS_PREFIX = "plain.";
    private static final String CONTROLLER_SASL_JAAS = "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "user_" + CONTROLLER_USERNAME + "=\"" + CONTROLLER_PASSWORD + "\";";
    private static final String EXTERNAL_SASL_JAAS = "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "user_" + EXTERNAL_USERNAME + "=\"" + EXTERNAL_PASSWORD + "\";";

    @ClusterTest(
        types = {Type.CO_KRAFT},
        controllerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT,
        serverProperties = {
            @ClusterConfigProperty(key = LISTENER_PREFIX + MECHANISMS_PREFIX + SASL_LOGIN_CLASS, value = "org.apache.kafka.clients.security.LoginTest$ControllerCustomerLogin"),
            @ClusterConfigProperty(key = EXTERNAL_PREFIX + MECHANISMS_PREFIX + SASL_LOGIN_CLASS, value = "org.apache.kafka.clients.security.LoginTest$ExternalCustomerLogin"),
            @ClusterConfigProperty(key = SASL_ENABLED_MECHANISMS_CONFIG, value = MECHANISMS),
            @ClusterConfigProperty(key = SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, value = MECHANISMS),
            @ClusterConfigProperty(key = LISTENER_PREFIX + MECHANISMS_PREFIX + SASL_JAAS_CONFIG, value = CONTROLLER_SASL_JAAS),
            @ClusterConfigProperty(key = EXTERNAL_PREFIX + MECHANISMS_PREFIX + SASL_JAAS_CONFIG, value = EXTERNAL_SASL_JAAS),
            @ClusterConfigProperty(key = SUPER_USERS_CONFIG, value = "User:" + CONTROLLER_USERNAME + ";User:" + EXTERNAL_USERNAME),
        }
    )
    public void testCustomLoginWithKafkaCluster(ClusterInstance cluster) {
        try (Admin admin = cluster.admin(saslConfig())) {
            int found = 0;
            for (Metric metric : admin.metrics().values()) {
                found += assertMetricName(
                    metric.metricName(),
                    ExternalCustomerLogin.METRIC_NAME,
                    ExternalCustomerLogin.METRIC_DESCRIPTION,
                    expectedTags(Map.of(
                        "class", ExternalCustomerLogin.class.getSimpleName(),
                        "client-id", CLIENT_ID
                    ))
                );
            }
            assertEquals(1, found, "Expected to find 1 metric");

            Map<MetricName, KafkaMetric> allMetrics = Stream.of(
                cluster.controllers().get(0).metrics().metrics(),
                cluster.brokers().get(0).metrics().metrics()
            ).collect(HashMap::new, Map::putAll, Map::putAll);
            assertMetrics(
                allMetrics,
                ExternalCustomerLogin.METRIC_NAME,
                ExternalCustomerLogin.METRIC_DESCRIPTION,
                expectedTags(Map.of(
                    "mechanism", MECHANISMS,
                    "listener", "EXTERNAL",
                    "networkProcessor", "0",
                    "class", ExternalCustomerLogin.class.getSimpleName()
                ))
            );
            assertMetrics(
                allMetrics,
                ControllerCustomerLogin.METRIC_NAME,
                ControllerCustomerLogin.METRIC_DESCRIPTION,
                expectedTags(Map.of(
                    "node-id", "0",
                    "component", "raft-channel",
                    "class", ControllerCustomerLogin.class.getSimpleName()
                ))
            );
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
        Map<MetricName, KafkaMetric> metrics,
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

    protected static Map<String, Object> saslConfig() {
        return Map.of(
            SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
            CLIENT_ID_CONFIG, CLIENT_ID,
            SASL_MECHANISM, MECHANISMS,
            SASL_LOGIN_CLASS, LoginTest.ExternalCustomerLogin.class.getName(),
            SASL_JAAS_CONFIG, CONTROLLER_SASL_JAAS
        );
    }

    public static class ControllerCustomerLogin implements Login, Monitorable {

        private static final String METRIC_NAME = "monitorable-controller-custom-login-name";
        private static final String METRIC_DESCRIPTION = "monitorable-controller-custom-login-description";

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
            subject.getPublicCredentials().add(CONTROLLER_USERNAME);
            subject.getPrivateCredentials().add(CONTROLLER_PASSWORD);
            return context;
        }

        @Override
        public Subject subject() {
            return subject;
        }

        @Override
        public String serviceName() {
            return "controller customer login";
        }

        @Override
        public void close() {

        }
    }

    public static class ExternalCustomerLogin implements Login, Monitorable {

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
            return "external customer login";
        }

        @Override
        public void close() {

        }
    }
}
