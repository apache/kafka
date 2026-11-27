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

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.security.AuthorizationTestUtils.BaseEndToEndAuthorizationTestcase;
import org.apache.kafka.clients.security.AuthorizationTestUtils.TestContext;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.api.BeforeEach;

import java.util.Map;

import static org.apache.kafka.clients.security.AuthorizationTestUtils.BROKER_COUNT;
import static org.apache.kafka.clients.security.AuthorizationTestUtils.TOPIC_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.test.JaasUtils.KAFKA_PLAIN_ADMIN;
import static org.apache.kafka.common.test.JaasUtils.KAFKA_PLAIN_USER1;
import static org.apache.kafka.common.test.JaasUtils.KAFKA_PLAIN_USER1_PASSWORD;
import static org.apache.kafka.common.test.JaasUtils.plainLoginModule;
import static org.apache.kafka.common.test.api.TestKitDefaults.CONTROLLER_ID_OFFSET;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
        @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:" + KAFKA_PLAIN_ADMIN),
        @ClusterConfigProperty(id = CONTROLLER_ID_OFFSET, key = StandardAuthorizer.SUPER_USERS_CONFIG, value = "User:" + KAFKA_PLAIN_ADMIN + ";User:ANONYMOUS"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG, value = "1500")
    }
)
public class EndToEndAuthorizationTest {
    private final ClusterInstance cluster;
    private final TestContext context;

    public EndToEndAuthorizationTest(ClusterInstance cluster) {
        this.cluster = cluster;
        this.context = new TestContext(
            cluster,
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, KAFKA_PLAIN_USER1),
            Map.of(
                SASL_MECHANISM, "PLAIN",
                SASL_JAAS_CONFIG, plainLoginModule(KAFKA_PLAIN_USER1, KAFKA_PLAIN_USER1_PASSWORD, false, Map.of()).toString()
            ),
            Map.of()
        );
    }

    @BeforeEach
    public void setUp() {
        cluster.createTopic(TOPIC_NAME, 1, (short) BROKER_COUNT);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicProduceConsumeViaAssign() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeViaAssign(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerProduceConsumeViaAssign() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeViaAssign(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicProduceConsumeViaSubscribe() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeViaSubscribe(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerProduceConsumeViaSubscribe() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeViaSubscribe(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicProduceConsumeWithWildcardAcls() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeWithWildcardAcls(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerProduceConsumeWithWildcardAcls() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeWithWildcardAcls(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicProduceConsumeWithPrefixedAcls() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeWithPrefixedAcls(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerProduceConsumeWithPrefixedAcls() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeWithPrefixedAcls(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicProduceConsumeTopicAutoCreateTopicCreateAcl() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeTopicAutoCreateTopicCreateAcl(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerProduceConsumeTopicAutoCreateTopicCreateAcl() throws Exception {
        BaseEndToEndAuthorizationTestcase.testProduceConsumeTopicAutoCreateTopicCreateAcl(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testNoDescribeProduceOrConsumeWithoutTopicDescribeAclWithIdempotence() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoDescribeProduceOrConsumeWithoutTopicDescribeAcl(context, GroupProtocol.CLASSIC, true);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testNoDescribeProduceOrConsumeWithoutTopicDescribeAclWithoutIdempotence() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoDescribeProduceOrConsumeWithoutTopicDescribeAcl(context, GroupProtocol.CLASSIC, false);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testNoProduceWithDescribeAclWithIdempotence() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoProduceWithDescribeAcl(context, true);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testNoProduceWithDescribeAclWithoutIdempotence() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoProduceWithDescribeAcl(context, false);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicNoConsumeWithoutDescribeAclViaAssign() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoConsumeWithoutDescribeAclViaAssign(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerNoConsumeWithoutDescribeAclViaAssign() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoConsumeWithoutDescribeAclViaAssign(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicNoConsumeWithoutDescribeAclViaSubscribe() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoConsumeWithoutDescribeAclViaSubscribe(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerNoConsumeWithoutDescribeAclViaSubscribe() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoConsumeWithoutDescribeAclViaSubscribe(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicNoConsumeWithDescribeAclViaAssign() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoConsumeWithDescribeAclViaAssign(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerNoConsumeWithDescribeAclViaAssign() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoConsumeWithDescribeAclViaAssign(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicNoConsumeWithDescribeAclViaSubscribe() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoConsumeWithDescribeAclViaSubscribe(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerNoConsumeWithDescribeAclViaSubscribe() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoConsumeWithDescribeAclViaSubscribe(context, GroupProtocol.CONSUMER);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testClassicNoGroupAcl() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoGroupAcl(context, GroupProtocol.CLASSIC);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testConsumerNoGroupAcl() throws Exception {
        BaseEndToEndAuthorizationTestcase.testNoGroupAcl(context, GroupProtocol.CONSUMER);
    }
}
