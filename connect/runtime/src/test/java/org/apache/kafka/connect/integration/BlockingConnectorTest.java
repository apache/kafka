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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.Assert.assertThrows;

public class BlockingConnectorTest {

    private static final Logger log = LoggerFactory.getLogger(BlockingConnectorTest.class);

    private static final int NUM_WORKERS = 1;
    private static final String BLOCKING_CONNECTOR_NAME = "blocking-connector";
    private static final String NORMAL_CONNECTOR_NAME = "normal-connector";
    private static final long REST_REQUEST_TIMEOUT = Worker.CONNECTOR_GRACEFUL_SHUTDOWN_TIMEOUT_MS * 2;

    private EmbeddedConnectCluster connect;

    @Before
    public void setup() {
        // Artificially reduce the REST request timeout so that these don't take forever
        ConnectorsResource.setRequestTimeout(REST_REQUEST_TIMEOUT);
        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(NUM_WORKERS)
                .numBrokers(1)
                .workerProps(new HashMap<>())
                .brokerProps(new Properties())
                .build();

        // start the clusters
        connect.start();
    }

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
        ConnectorsResource.resetRequestTimeout();
        BlockingConnector.resetBlockLatch();
    }

    @Test
    public void testBlockInConnectorValidate() throws Exception {
        log.info("Starting test testBlockInConnectorValidate");
        assertThrows(ConnectRestException.class, () -> createConnectorWithBlock(ValidateBlockingConnector.class));
        // Will NOT assert that connector has failed, since the request should fail before it's even created

        // Connector should already be blocked so this should return immediately, but check just to
        // make sure that it actually did block
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    public void testBlockInConnectorConfig() throws Exception {
        log.info("Starting test testBlockInConnectorConfig");
        assertThrows(ConnectRestException.class, () -> createConnectorWithBlock(ConfigBlockingConnector.class));
        // Will NOT assert that connector has failed, since the request should fail before it's even created

        // Connector should already be blocked so this should return immediately, but check just to
        // make sure that it actually did block
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    public void testBlockInConnectorInitialize() throws Exception {
        log.info("Starting test testBlockInConnectorInitialize");
        createConnectorWithBlock(InitializeBlockingConnector.class);
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    public void testBlockInConnectorStart() throws Exception {
        log.info("Starting test testBlockInConnectorStart");
        createConnectorWithBlock(BlockingConnector.START);
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    public void testBlockInConnectorStop() throws Exception {
        log.info("Starting test testBlockInConnectorStop");
        createConnectorWithBlock(BlockingConnector.STOP);
        waitForConnectorStart(BLOCKING_CONNECTOR_NAME);
        connect.deleteConnector(BLOCKING_CONNECTOR_NAME);
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    public void testWorkerRestartWithBlockInConnectorStart() throws Exception {
        log.info("Starting test testWorkerRestartWithBlockInConnectorStart");
        createNormalConnector();
        createConnectorWithBlock(BlockingConnector.START);
        // First instance of the connector should block on startup
        BlockingConnector.waitForBlock();
        connect.removeWorker();

        connect.addWorker();
        // After stopping the only worker and restarting it, a new instance of the blocking
        // connector should be created and we can ensure that it blocks again
        BlockingConnector.waitForBlock();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    public void testWorkerRestartWithBlockInConnectorStop() throws Exception {
        log.info("Starting test testWorkerRestartWithBlockInConnectorStop");
        createNormalConnector();
        createConnectorWithBlock(BlockingConnector.STOP);
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
        waitForConnectorStart(BLOCKING_CONNECTOR_NAME);
        connect.removeWorker();
        BlockingConnector.waitForBlock();

        connect.addWorker();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
        waitForConnectorStart(BLOCKING_CONNECTOR_NAME);
    }

    @Test
    @Ignore("Interaction with this connector method has not been made asynchronous yet and this test will fail")
    public void testBlockInConnectorTaskClass() throws Exception {
        createConnectorWithBlock(BlockingConnector.TASK_CLASS);
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    @Ignore("Interaction with this connector method has not been made asynchronous yet and this test will fail")
    public void testBlockInConnectorTaskConfigs() throws Exception {
        createConnectorWithBlock(BlockingConnector.TASK_CONFIGS);
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    @Ignore("Interaction with this connector method has not been made asynchronous yet and this test will fail")
    public void testConnectorRestartWithBlockInConnectorTaskConfigs() throws Exception {
        createConnectorWithBlock(BlockingConnector.TASK_CONFIGS);
        // Test both explicit restart and implicit (pause then resume) restart
        connect.restartConnector(BLOCKING_CONNECTOR_NAME);
        connect.pauseConnector(BLOCKING_CONNECTOR_NAME);
        connect.resumeConnector(BLOCKING_CONNECTOR_NAME);
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    // TODO: Consider patching plugin scanning to handle connectors that block in their version methods
    // @Test
    // public void testBlockInConnectorVersion() throws Exception {
    //     createConnectorWithBlock(VersionBlockingConnector.class);
    //     createNormalConnector();
    //     waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    // }

    @Test
    @Ignore("This connector method is never invoked by the framework")
    public void testBlockInConnectorInitializeWithTaskConfigs() throws Exception {
        createConnectorWithBlock(BlockingConnector.INITIALIZE_WITH_TASK_CONFIGS);
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    @Test
    @Ignore("This connector method is never invoked by the framework")
    public void testBlockInConnectorReconfigure() throws Exception {
        createConnectorWithBlock(BlockingConnector.RECONFIGURE);
        Map<String, String> newProps = baseBlockingConnectorProps();
        newProps.put("foo", "bar");

        connect.configureConnector(BLOCKING_CONNECTOR_NAME, newProps);
        BlockingConnector.waitForBlock();

        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
    }

    private void createConnectorWithBlock(String block) {
        Map<String, String> props = baseBlockingConnectorProps();
        props.put(BlockingConnector.BLOCK_CONFIG, block);
        log.info("Creating connector with block during {}", block);
        try {
            connect.configureConnector(BLOCKING_CONNECTOR_NAME, props);
        } catch (RuntimeException e) {
            log.info("Failed to create connector", e);
            throw e;
        }
    }

    private void createConnectorWithBlock(Class<? extends BlockingConnector> connectorClass) {
        Map<String, String> props = baseBlockingConnectorProps();
        props.put(CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        log.info("Creating blocking connector of type {}", connectorClass.getSimpleName());
        try {
            connect.configureConnector(BLOCKING_CONNECTOR_NAME, props);
        } catch (RuntimeException e) {
            log.info("Failed to create connector", e);
            throw e;
        }
    }

    private Map<String, String> baseBlockingConnectorProps() {
        Map<String, String> result = new HashMap<>();
        result.put(CONNECTOR_CLASS_CONFIG, BlockingConnector.class.getName());
        result.put(TASKS_MAX_CONFIG, "1");
        return result;
    }

    private void createNormalConnector() {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPICS_CONFIG, "normal-topic");
        log.info("Creating normal connector");
        try {
            connect.configureConnector(NORMAL_CONNECTOR_NAME, props);
        } catch (RuntimeException e) {
            log.info("Failed to create connector", e);
            throw e;
        }
    }

    private void waitForConnectorStart(String connector) throws InterruptedException {
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(
            connector,
            0,
            String.format(
                "Failed to observe transition to 'RUNNING' state for connector '%s' in time",
                connector
            )
        );
    }

    public static class BlockingConnector extends SourceConnector {

        private static CountDownLatch blockLatch;

        private String block;

        public static final String BLOCK_CONFIG = "block";

        public static final String INITIALIZE = "initialize";
        public static final String INITIALIZE_WITH_TASK_CONFIGS = "initializeWithTaskConfigs";
        public static final String START = "start";
        public static final String RECONFIGURE = "reconfigure";
        public static final String TASK_CLASS = "taskClass";
        public static final String TASK_CONFIGS = "taskConfigs";
        public static final String STOP = "stop";
        public static final String VALIDATE = "validate";
        public static final String CONFIG = "config";
        public static final String VERSION = "version";

        private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                BLOCK_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.MEDIUM,
                "Where to block indefinitely, e.g., 'start', 'initialize', 'taskConfigs', 'version'"
            );

        // No-args constructor required by the framework
        public BlockingConnector() {
            this(null);
        }

        protected BlockingConnector(String block) {
            this.block = block;
            synchronized (BlockingConnector.class) {
                if (blockLatch != null) {
                    blockLatch.countDown();
                }
                blockLatch = new CountDownLatch(1);
            }
        }

        public static void waitForBlock() throws InterruptedException {
            synchronized (BlockingConnector.class) {
                if (blockLatch == null) {
                    throw new IllegalArgumentException("No connector has been created yet");
                }
            }
            
            log.debug("Waiting for connector to block");
            blockLatch.await();
            log.debug("Connector should now be blocked");
        }

        public static void resetBlockLatch() {
            synchronized (BlockingConnector.class) {
                if (blockLatch != null) {
                    blockLatch.countDown();
                    blockLatch = null;
                }
            }
        }

        @Override
        public void initialize(ConnectorContext ctx) {
            maybeBlockOn(INITIALIZE);
            super.initialize(ctx);
        }

        @Override
        public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
            maybeBlockOn(INITIALIZE_WITH_TASK_CONFIGS);
            super.initialize(ctx, taskConfigs);
        }

        @Override
        public void start(Map<String, String> props) {
            this.block = new AbstractConfig(CONFIG_DEF, props).getString(BLOCK_CONFIG);
            maybeBlockOn(START);
        }

        @Override
        public void reconfigure(Map<String, String> props) {
            super.reconfigure(props);
            maybeBlockOn(RECONFIGURE);
        }

        @Override
        public Class<? extends Task> taskClass() {
            maybeBlockOn(TASK_CLASS);
            return BlockingTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            maybeBlockOn(TASK_CONFIGS);
            return Collections.singletonList(Collections.emptyMap());
        }

        @Override
        public void stop() {
            maybeBlockOn(STOP);
        }

        @Override
        public Config validate(Map<String, String> connectorConfigs) {
            maybeBlockOn(VALIDATE);
            return super.validate(connectorConfigs);
        }

        @Override
        public ConfigDef config() {
            maybeBlockOn(CONFIG);
            return CONFIG_DEF;
        }

        @Override
        public String version() {
            maybeBlockOn(VERSION);
            return "0.0.0";
        }

        protected void maybeBlockOn(String block) {
            if (block.equals(this.block)) {
                log.info("Will block on {}", block);
                blockLatch.countDown();
                while (true) {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        // No-op. Just keep blocking.
                    }
                }
            } else {
                log.debug("Will not block on {}", block);
            }
        }

        public static class BlockingTask extends SourceTask {
            @Override
            public void start(Map<String, String> props) {
            }

            @Override
            public List<SourceRecord> poll() {
                return null;
            }

            @Override
            public void stop() {
            }

            @Override
            public String version() {
                return "0.0.0";
            }
        }
    }

    // Some methods are called before Connector::start, so we use this as a workaround
    public static class InitializeBlockingConnector extends BlockingConnector {
        public InitializeBlockingConnector() {
            super(INITIALIZE);
        }
    }

    public static class ConfigBlockingConnector extends BlockingConnector {
        public ConfigBlockingConnector() {
            super(CONFIG);
        }
    }

    public static class ValidateBlockingConnector extends BlockingConnector {
        public ValidateBlockingConnector() {
            super(VALIDATE);
        }
    }

    // Commented out in order for class not to be picked up during plugin scanning (otherwise, it
    // will be picked up with every test run, even those that are not meant to test blocks in the
    // Connector::version method, and since the worker does not yet gracefully handle connectors
    // that block in that method during plugin scanning, all of those tests would fail as their
    // workers would hang indefinitely during plugin scanning)
    // public static class VersionBlockingConnector extends BlockingConnector {
    //     public VersionBlockingConnector() {
    //         super(VERSION);
    //     }
    // }
}
