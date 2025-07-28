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

import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.runtime.ConnectMetricsRegistry;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.MultiVersionTest;
import org.apache.kafka.connect.runtime.isolation.VersionedPluginBuilder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiVersionIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(MultiVersionIntegrationTest.class);
    private static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int NUM_WORKERS = 3;

    private EmbeddedConnectCluster connect;
    private Map<String, String> workerProps;
    private Properties brokerProps;

    @BeforeEach
    public void setup(TestInfo testInfo) {
        log.info("Starting test {}", testInfo.getDisplayName());
        // setup Connect worker properties
        workerProps = new HashMap<>();
        workerProps.put(OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(OFFSET_COMMIT_INTERVAL_MS));
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");
        Set<String> pluginPaths = MultiVersionTest.DEFAULT_ISOLATED_ARTIFACTS.keySet().stream().map(Path::toString).collect(Collectors.toSet());
        pluginPaths.addAll(MultiVersionTest.DEFAULT_COMBINED_ARTIFACT.keySet().stream().map(Path::toString).collect(Collectors.toSet()));
        workerProps.put(WorkerConfig.PLUGIN_PATH_CONFIG, String.join(",", pluginPaths));

        // setup Kafka broker properties
        brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", String.valueOf(false));

        // build a Connect cluster backed by a Kafka KRaft cluster
        connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .numWorkers(NUM_WORKERS)
            .workerProps(workerProps)
            .brokerProps(brokerProps)
            .maskExitProcedures(true)
            .build();
    }

    private static class VersionedConnectorConfig {

        private Map<String, String> connectorProps = new HashMap<>();
        private String name;
        private String className;
        private String version;
        private int taskCount;
        private String keyConverterVersion;
        private String valueConverterVersion;
        private String headerConverterVersion;
        private Map<String, String> transforms = new HashMap<>();
        private Map<String, String> predicates = new HashMap<>();

        private VersionedConnectorConfig() {
        }

        public VersionedConnectorConfig name(String name) {
            connectorProps.put(ConnectorConfig.NAME_CONFIG, name);
            this.name = name;
            return this;
        }

        public VersionedConnectorConfig type(ConnectorType type) {
            if (type == ConnectorType.SOURCE) {
                this.className = VersionedPluginBuilder.VersionedTestPlugin.SOURCE_CONNECTOR.className();
                connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, this.className);
                return this;
            }
            this.className = VersionedPluginBuilder.VersionedTestPlugin.SINK_CONNECTOR.className();
            connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, VersionedPluginBuilder.VersionedTestPlugin.SINK_CONNECTOR.className());
            connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "versioned-topic");
            return this;
        }

        private void maybeAddConfig(String key, String value) {
            if (value != null) {
                connectorProps.put(key, value);
            }
        }

        public VersionedConnectorConfig version(String version) {
            maybeAddConfig(ConnectorConfig.CONNECTOR_VERSION, version);
            this.version = version;
            return this;
        }

        public VersionedConnectorConfig taskCount(int taskCount) {
            connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(taskCount));
            this.taskCount = taskCount;
            return this;
        }

        public VersionedConnectorConfig keyConverter(String version) {
            connectorProps.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className());
            maybeAddConfig(ConnectorConfig.KEY_CONVERTER_VERSION_CONFIG, version);
            this.keyConverterVersion = version;
            return this;
        }

        public VersionedConnectorConfig valueConverter(String version) {
            connectorProps.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className());
            maybeAddConfig(ConnectorConfig.VALUE_CONVERTER_VERSION_CONFIG, version);
            this.valueConverterVersion = version;
            return this;
        }

        public VersionedConnectorConfig headerConverter(String version) {
            connectorProps.put(ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG, VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER.className());
            maybeAddConfig(ConnectorConfig.HEADER_CONVERTER_VERSION_CONFIG, version);
            this.headerConverterVersion = version;
            return this;
        }

        public VersionedConnectorConfig addOrUpdateTransform(String transformName, String version) {
            this.transforms.put(transformName, version);
            String existingTransforms = connectorProps.getOrDefault(ConnectorConfig.TRANSFORMS_CONFIG, "");
            if (!existingTransforms.contains(transformName)) {
                existingTransforms = existingTransforms + (existingTransforms.isEmpty() ? "" : ",") + transformName;
                connectorProps.put(ConnectorConfig.TRANSFORMS_CONFIG, existingTransforms);
            }
            String transformPrefix = "transforms." + transformName + ".";
            connectorProps.put(transformPrefix + "type", VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION.className());
            maybeAddConfig(transformPrefix + WorkerConfig.PLUGIN_VERSION_SUFFIX, version);
            return this;
        }

        public VersionedConnectorConfig addOrUpdatePredicate(String transformName, String predicateName, String version) {
            this.predicates.put(predicateName, version);
            String existingPredicates = connectorProps.getOrDefault(ConnectorConfig.PREDICATES_CONFIG, "");
            if (!existingPredicates.contains(predicateName)) {
                existingPredicates = existingPredicates + (existingPredicates.isEmpty() ? "" : ",") + predicateName;
                connectorProps.put(ConnectorConfig.PREDICATES_CONFIG, existingPredicates);
            }
            String predicatePrefix = "predicates." + predicateName + ".";
            connectorProps.put(predicatePrefix + "type", VersionedPluginBuilder.VersionedTestPlugin.PREDICATE.className());
            maybeAddConfig(predicatePrefix + WorkerConfig.PLUGIN_VERSION_SUFFIX, version);
            connectorProps.put("transforms." + transformName + ".predicate", predicateName);
            return this;
        }

        Map<String, String> connectorProps() {
            return connectorProps;
        }
    }

    private void assertCorrectVersions(VersionedConnectorConfig config) throws Exception {

        ConnectorStateInfo state = connect.connectorStatus(config.name);
        assertEquals(config.version, state.connector().version());
        assertEquals(config.taskCount, state.tasks().size());
        state.tasks().forEach(task -> assertEquals(config.version, task.version()));

        ConnectMetricsRegistry registry = new ConnectMetricsRegistry();
        ObjectName connectorTaskMetrics = new ObjectName("kafka.connect:type=connector-task-metrics,connector=" + config.name + ",task=0");
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        assertEquals(config.version, mBeanServer.getAttribute(connectorTaskMetrics, registry.taskConnectorClassVersion.name()));
        assertEquals(config.version, mBeanServer.getAttribute(connectorTaskMetrics, registry.taskVersion.name()));
        assertEquals(config.className, mBeanServer.getAttribute(connectorTaskMetrics, registry.taskConnectorClass.name()));

        if (config.keyConverterVersion != null) {
            assertEquals(config.keyConverterVersion, mBeanServer.getAttribute(connectorTaskMetrics, registry.taskKeyConverterVersion.name()));
            assertEquals(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className(), mBeanServer.getAttribute(connectorTaskMetrics, registry.taskKeyConverterClass.name()));
        }
        if (config.valueConverterVersion != null) {
            assertEquals(config.valueConverterVersion, mBeanServer.getAttribute(connectorTaskMetrics, registry.taskValueConverterVersion.name()));
            assertEquals(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER.className(), mBeanServer.getAttribute(connectorTaskMetrics, registry.taskValueConverterClass.name()));
        }
        if (config.headerConverterVersion != null) {
            assertEquals(config.headerConverterVersion, mBeanServer.getAttribute(connectorTaskMetrics, registry.taskHeaderConverterVersion.name()));
            assertEquals(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER.className(), mBeanServer.getAttribute(connectorTaskMetrics, registry.taskHeaderConverterClass.name()));
        }

        for (Map.Entry<String, String> transform : config.transforms.entrySet()) {
            ObjectName transformMetrics = new ObjectName("kafka.connect:type=connector-transform-metrics,connector=" + config.name + ",task=0,transform=" + transform.getKey());
            assertEquals(transform.getValue(), mBeanServer.getAttribute(transformMetrics, registry.transformVersion.name()));
            assertEquals(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION.className(), mBeanServer.getAttribute(transformMetrics, registry.transformClass.name()));
        }

        for (Map.Entry<String, String> predicate : config.predicates.entrySet()) {
            ObjectName predicateMetrics = new ObjectName("kafka.connect:type=connector-predicate-metrics,connector=" + config.name + ",task=0,predicate=" + predicate.getKey());
            assertEquals(predicate.getValue(), mBeanServer.getAttribute(predicateMetrics, registry.predicateVersion.name()));
            assertEquals(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE.className(), mBeanServer.getAttribute(predicateMetrics, registry.predicateClass.name()));
        }
    }

    @Test
    public void testMultipleVersionedConnector() throws Exception {
        // start the clusters
        connect.start();

        Random random = new Random();
        int versions = MultiVersionTest.DEFAULT_ISOLATED_ARTIFACTS_VERSIONS.size();
        int versionIndex = random.nextInt(versions);
        String version = MultiVersionTest.DEFAULT_ISOLATED_ARTIFACTS_VERSIONS.toArray()[versionIndex].toString();

        VersionedConnectorConfig connector1 = new VersionedConnectorConfig().name("versioned-connector-1")
            .type(ConnectorType.SOURCE).version(version).keyConverter(version).valueConverter(version).headerConverter(version)
            .addOrUpdateTransform("t1", version).addOrUpdatePredicate("t1", "p1", version)
            .taskCount(1);

        version = MultiVersionTest.DEFAULT_ISOLATED_ARTIFACTS_VERSIONS.toArray()[(versionIndex + 1) % versions].toString();
        VersionedConnectorConfig connector2 = new VersionedConnectorConfig().name("versioned-connector-2")
            .type(ConnectorType.SOURCE).version(version).keyConverter(version).valueConverter(version).headerConverter(version)
            .addOrUpdateTransform("t1", version).addOrUpdatePredicate("t1", "p1", version)
            .taskCount(3);

        version = MultiVersionTest.DEFAULT_ISOLATED_ARTIFACTS_VERSIONS.toArray()[(versionIndex + 2) % versions].toString();
        VersionedConnectorConfig connector3 = new VersionedConnectorConfig().name("versioned-connector-3")
            .type(ConnectorType.SINK).version(version).keyConverter(version).valueConverter(version).headerConverter(version)
            .addOrUpdateTransform("t1", version).addOrUpdatePredicate("t1", "p1", version)
            .taskCount(2);

        version = MultiVersionTest.DEFAULT_ISOLATED_ARTIFACTS_VERSIONS.toArray()[(versionIndex + 3) % versions].toString();
        VersionedConnectorConfig connector4 = new VersionedConnectorConfig().name("versioned-connector-4")
            .type(ConnectorType.SINK).version(version).keyConverter(version).valueConverter(version).headerConverter(version)
            .addOrUpdateTransform("t1", version).addOrUpdatePredicate("t1", "p1", version)
            .taskCount(4);

        connect.configureConnector(connector1.name, connector1.connectorProps());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connector1.name, connector1.taskCount,
            "Connector tasks did not start in time.");
        connect.configureConnector(connector2.name, connector2.connectorProps());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connector2.name, connector2.taskCount,
            "Connector tasks did not start in time.");
        connect.configureConnector(connector3.name, connector3.connectorProps());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connector3.name, connector3.taskCount,
            "Connector tasks did not start in time.");
        connect.configureConnector(connector4.name, connector4.connectorProps());
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connector4.name, connector4.taskCount,
            "Connector tasks did not start in time.");

        assertCorrectVersions(connector1);
        assertCorrectVersions(connector2);
        assertCorrectVersions(connector3);
        assertCorrectVersions(connector4);
        // stop the clusters
        connect.stop();
    }

    @Test
    public void testUpgradeVersion() throws Exception {
        connect.start();

        VersionedConnectorConfig config = new VersionedConnectorConfig().name("versioned-connector").type(ConnectorType.SOURCE);
        int taskCount = 1;
        for (String version: MultiVersionTest.DEFAULT_ISOLATED_ARTIFACTS_VERSIONS.stream().toList()) {
            config.version(version).keyConverter(version).valueConverter(version).headerConverter(version)
                .addOrUpdateTransform("t1", version).addOrUpdatePredicate("t1", "p1", version)
                .taskCount(taskCount);
            connect.configureConnector(config.name, config.connectorProps());
            connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(config.name, config.taskCount,
                "Connector tasks did not start in time.");
            assertCorrectVersions(config);
            taskCount++;
        }

        connect.stop();
    }
}
