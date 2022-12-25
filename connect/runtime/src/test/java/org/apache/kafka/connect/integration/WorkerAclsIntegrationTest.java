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
package org.apache.kafka.connect.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectClusterAssertions;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;

@Category(IntegrationTest.class)
public class WorkerAclsIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(WorkerAclsIntegrationTest.class);
    private static final String GROUP_ID = "connect-test-cluster";
    private static final String CONFIG_TOPIC = "connect-config-topic";
    private static final String OFFSET_TOPIC = "connect-offset-topic";
    private static final String STATUS_TOPIC = "connect-status-topic";
    private static final String CONNECTOR_NAME = "test-connector";
    private EmbeddedConnectCluster connectCluster;
    private Admin adminClient;

    @Before
    public void setup() throws Exception {
        Properties brokerProps = new Properties();
        brokerProps.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
        brokerProps.put("sasl.enabled.mechanisms", "PLAIN");
        brokerProps.put("sasl.mechanism.inter.broker.protocol", "PLAIN");
        brokerProps.put("security.inter.broker.protocol", "SASL_PLAINTEXT");
        brokerProps.put("listeners", "SASL_PLAINTEXT://localhost:0");
        brokerProps.put("listener.name.sasl_plaintext.plain.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"super\" "
                        + "password=\"super_pwd\" "
                        + "user_worker=\"worker_pwd\" "
                        + "user_super=\"super_pwd\";");
        brokerProps.put("super.users", "User:super");

        Map<String, String> superUserClientConfig = new HashMap<>();
        addClientSaslConfigs(superUserClientConfig, "super", "super_pwd");

        Map<String, String> connectWorkerSaslConfig = new HashMap<>();
        // Give the worker a non superuser principal
        addClientSaslConfigs(connectWorkerSaslConfig, "worker", "worker_pwd");
        Map<String, String> workerProps = new HashMap<>();
        workerProps.putAll(connectWorkerSaslConfig);
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, GROUP_ID);
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, CONFIG_TOPIC);
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, OFFSET_TOPIC);
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, STATUS_TOPIC);

        connectCluster = new EmbeddedConnectCluster.Builder()
                .numWorkers(1)
                .numBrokers(1)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .clientConfigs(superUserClientConfig)
                .build();

        connectCluster.kafka().start();
        adminClient = connectCluster.kafka().createAdminClient();
        adminClient.createAcls(Collections.singletonList(
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:worker", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                )
        )).all().get();
    }

    @After
    public void tearDown() {
        connectCluster.stop();
    }

    @Test
    public void testConnectorCreationWithAndWithoutWritePermissionOnConfigTopic() throws Exception {

        // Add all required ACLs to the worker's principal for metadata topic operations (except write permissions
        // on the config topic)
        createWorkerTopicAcls(CONFIG_TOPIC, AclOperation.CREATE, AclPermissionType.ALLOW);
        createWorkerTopicAcls(CONFIG_TOPIC, AclOperation.READ, AclPermissionType.ALLOW);
        createWorkerTopicAcls(OFFSET_TOPIC, AclOperation.ALL, AclPermissionType.ALLOW);
        createWorkerTopicAcls(STATUS_TOPIC, AclOperation.ALL, AclPermissionType.ALLOW);

        connectCluster.startConnect();
        connectCluster.assertions().assertAtLeastNumWorkersAreUp(
                1,
                "Worker did not start in time."
        );

        ConnectRestException e = Assert.assertThrows(
                ConnectRestException.class,
                () -> connectCluster.configureConnector(CONNECTOR_NAME, connectorConfigs()));

        Assert.assertEquals(500, e.statusCode());
        Assert.assertTrue(e.getMessage().contains("Not authorized to access topics"));

        createWorkerTopicAcls(CONFIG_TOPIC, AclOperation.WRITE, AclPermissionType.ALLOW);

        connectCluster.configureConnector(CONNECTOR_NAME, connectorConfigs());
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(
                CONNECTOR_NAME,
                1,
                "Connector tasks did not start in time."
        );
    }

    @Test
    public void testConnectorPauseWithAndWithoutWritePermissionOnConfigTopic() throws Exception {

        // Add all required ACLs to the worker's principal for metadata topic operations
        createWorkerTopicAcls(CONFIG_TOPIC, AclOperation.ALL, AclPermissionType.ALLOW);
        createWorkerTopicAcls(OFFSET_TOPIC, AclOperation.ALL, AclPermissionType.ALLOW);
        createWorkerTopicAcls(STATUS_TOPIC, AclOperation.ALL, AclPermissionType.ALLOW);

        connectCluster.startConnect();
        connectCluster.assertions().assertAtLeastNumWorkersAreUp(
                1,
                "Worker did not start in time."
        );

        connectCluster.configureConnector(CONNECTOR_NAME, connectorConfigs());
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(
                CONNECTOR_NAME,
                1,
                "Connector tasks did not start in time."
        );

        // This essentially overrides the previously granted permission to WRITE to the config topic
        createWorkerTopicAcls(CONFIG_TOPIC, AclOperation.WRITE, AclPermissionType.DENY);

        ConnectRestException e = Assert.assertThrows(
                ConnectRestException.class,
                () -> connectCluster.pauseConnector(CONNECTOR_NAME));
        Assert.assertEquals(500, e.statusCode());
        Assert.assertTrue(e.getMessage().contains("Not authorized to access topics"));

        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(
                CONNECTOR_NAME,
                1,
                "Expected running connectors to continue running after failed pause attempt."
        );

        // Remove the DENY WRITE ACL on the config topic and retry the pause operation
        deleteWorkerTopicAcls(CONFIG_TOPIC, AclOperation.WRITE, AclPermissionType.DENY);

        connectCluster.pauseConnector(CONNECTOR_NAME);
        connectCluster.assertions().assertConnectorAndTasksAreStopped(
                CONNECTOR_NAME,
                "Expected connectors and tasks to be paused after successful pause operation."
        );
    }

    @Test
    public void testConnectorPauseWithAndWithoutWritePermissionOnStatusTopic() throws Exception {

        // Add all required ACLs to the worker's principal for metadata topic operations
        createWorkerTopicAcls(CONFIG_TOPIC, AclOperation.ALL, AclPermissionType.ALLOW);
        createWorkerTopicAcls(OFFSET_TOPIC, AclOperation.ALL, AclPermissionType.ALLOW);
        createWorkerTopicAcls(STATUS_TOPIC, AclOperation.ALL, AclPermissionType.ALLOW);

        connectCluster.startConnect();
        connectCluster.assertions().assertAtLeastNumWorkersAreUp(
                1,
                "Worker did not start in time."
        );

        // Use a unique connector name here because RuntimeHandles is a singleton and using the common connector name
        // can cause other tests to interfere with this one (we're relying on the ConnectorHandle to verify that the
        // connector's stop method was called)
        String uniqueConnectorName = CONNECTOR_NAME + UUID.randomUUID();
        connectCluster.configureConnector(uniqueConnectorName, connectorConfigs());

        ConnectorHandle connectorHandle = RuntimeHandles.get().connectorHandle(uniqueConnectorName);

        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(uniqueConnectorName, 1,
                "Connector tasks did not start in time.");

        // This essentially overrides the previously granted permission to WRITE to the status topic
        createWorkerTopicAcls(STATUS_TOPIC, AclOperation.WRITE, AclPermissionType.DENY);

        // Expect the pause request itself to be successful (i.e. pauseConnector shouldn't throw an exception)
        connectCluster.pauseConnector(uniqueConnectorName);
        connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(
                uniqueConnectorName,
                1,
                "Expected connector and task statuses to be RUNNING even though pause was issued due to DENY WRITE ACL on status topic.");

        // Ensure that the connector was actually paused even though the status doesn't reflect it yet.
        TestUtils.waitForCondition(
                () -> connectorHandle.startAndStopCounter().stops() == 1,
                EmbeddedConnectClusterAssertions.CONNECTOR_SHUTDOWN_DURATION_MS,
                "Expected connector's stop method to be called after issued pause operation."
        );

        // Remove the DENY WRITE ACL on the status topic and retry the pause operation
        deleteWorkerTopicAcls(STATUS_TOPIC, AclOperation.WRITE, AclPermissionType.DENY);

        // Remove and re-add the single worker so that connector and task statuses are refreshed
        connectCluster.removeWorker();
        connectCluster.addWorker();
        connectCluster.assertions().assertAtLeastNumWorkersAreUp(
                1,
                "Worker did not start in time."
        );

        connectCluster.assertions().assertConnectorAndTasksAreStopped(
                uniqueConnectorName,
                "Expected connector and task statuses to be PAUSED."
        );
    }

    private void createWorkerTopicAcls(String topic, AclOperation operation, AclPermissionType permissionType) throws Exception {
        adminClient.createAcls(Collections.singletonList(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                        new AccessControlEntry("User:worker", "*", operation, permissionType)
                )
        )).all().get();
    }

    private void deleteWorkerTopicAcls(String topic, AclOperation operation, AclPermissionType permissionType) throws Exception {
        adminClient.deleteAcls(Collections.singletonList(
                new AclBindingFilter(
                        new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.LITERAL),
                        new AccessControlEntryFilter("User:worker", "*", operation, permissionType)
                )
        )).all().get();
    }

    private void addClientSaslConfigs(Map<String, String> clientConfigs, String username, String password) {
        clientConfigs.put("sasl.mechanism", "PLAIN");
        clientConfigs.put("security.protocol", "SASL_PLAINTEXT");
        clientConfigs.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + String.format("username=\"%s\" ", username)
                        + String.format("password=\"%s\";", password));
    }

    private Map<String, String> connectorConfigs() {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPICS_CONFIG, "test-topic");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }
}
