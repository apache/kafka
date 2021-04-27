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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertThrows;

/**
 * Tests situations during which certain connector operations, such as start, validation,
 * configuration and others, take longer than expected.
 */
@Category(IntegrationTest.class)
public class BlockingConnectorTest {

    private static final Logger log = LoggerFactory.getLogger(BlockingConnectorTest.class);

    private static final int NUM_WORKERS = 1;
    private static final String BLOCKING_CONNECTOR_NAME = "blocking-connector";
    private static final String NORMAL_CONNECTOR_NAME = "normal-connector";
    private static final String TEST_TOPIC = "normal-topic";
    private static final int NUM_RECORDS_PRODUCED = 100;
    private static final long CONNECT_WORKER_STARTUP_TIMEOUT = TimeUnit.SECONDS.toMillis(60);
    private static final long RECORD_TRANSFER_DURATION_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long REST_REQUEST_TIMEOUT = Worker.CONNECTOR_GRACEFUL_SHUTDOWN_TIMEOUT_MS * 2;

    private static final String CONNECTOR_INITIALIZE = "Connector::initialize";
    private static final String CONNECTOR_INITIALIZE_WITH_TASK_CONFIGS = "Connector::initializeWithTaskConfigs";
    private static final String CONNECTOR_START = "Connector::start";
    private static final String CONNECTOR_RECONFIGURE = "Connector::reconfigure";
    private static final String CONNECTOR_TASK_CLASS = "Connector::taskClass";
    private static final String CONNECTOR_TASK_CONFIGS = "Connector::taskConfigs";
    private static final String CONNECTOR_STOP = "Connector::stop";
    private static final String CONNECTOR_VALIDATE = "Connector::validate";
    private static final String CONNECTOR_CONFIG = "Connector::config";
    private static final String CONNECTOR_VERSION = "Connector::version";
    private static final String TASK_START = "Task::start";
    private static final String TASK_STOP = "Task::stop";
    private static final String TASK_VERSION = "Task::version";
    private static final String SINK_TASK_INITIALIZE = "SinkTask::initialize";
    private static final String SINK_TASK_PUT = "SinkTask::put";
    private static final String SINK_TASK_FLUSH = "SinkTask::flush";
    private static final String SINK_TASK_PRE_COMMIT = "SinkTask::preCommit";
    private static final String SINK_TASK_OPEN = "SinkTask::open";
    private static final String SINK_TASK_ON_PARTITIONS_ASSIGNED = "SinkTask::onPartitionsAssigned";
    private static final String SINK_TASK_CLOSE = "SinkTask::close";
    private static final String SINK_TASK_ON_PARTITIONS_REVOKED = "SinkTask::onPartitionsRevoked";
    private static final String SOURCE_TASK_INITIALIZE = "SourceTask::initialize";
    private static final String SOURCE_TASK_POLL = "SourceTask::poll";
    private static final String SOURCE_TASK_COMMIT = "SourceTask::commit";
    private static final String SOURCE_TASK_COMMIT_RECORD = "SourceTask::commitRecord";
    private static final String SOURCE_TASK_COMMIT_RECORD_WITH_METADATA = "SourceTask::commitRecordWithMetadata";

    private EmbeddedConnectCluster connect;
    private ConnectorHandle normalConnectorHandle;

    @Before
    public void setup() throws Exception {
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

        // wait for the Connect REST API to become available. necessary because of the reduced REST
        // request timeout; otherwise, we may get an unexpected 500 with our first real REST request
        // if the worker is still getting on its feet.
        waitForCondition(
            () -> connect.requestGet(connect.endpointForResource("connectors/nonexistent")).getStatus() == 404,
            CONNECT_WORKER_STARTUP_TIMEOUT,
            "Worker did not complete startup in time"
        );
    }

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
        ConnectorsResource.resetRequestTimeout();
        Block.resetBlockLatch();
    }

    @Test
    public void testBlockInConnectorValidate() throws Exception {
        log.info("Starting test testBlockInConnectorValidate");
        assertThrows(ConnectRestException.class, () -> createConnectorWithBlock(ValidateBlockingConnector.class, CONNECTOR_VALIDATE));
        // Will NOT assert that connector has failed, since the request should fail before it's even created

        // Connector should already be blocked so this should return immediately, but check just to
        // make sure that it actually did block
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testBlockInConnectorConfig() throws Exception {
        log.info("Starting test testBlockInConnectorConfig");
        assertThrows(ConnectRestException.class, () -> createConnectorWithBlock(ConfigBlockingConnector.class, CONNECTOR_CONFIG));
        // Will NOT assert that connector has failed, since the request should fail before it's even created

        // Connector should already be blocked so this should return immediately, but check just to
        // make sure that it actually did block
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testBlockInConnectorInitialize() throws Exception {
        log.info("Starting test testBlockInConnectorInitialize");
        createConnectorWithBlock(InitializeBlockingConnector.class, CONNECTOR_INITIALIZE);
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testBlockInConnectorStart() throws Exception {
        log.info("Starting test testBlockInConnectorStart");
        createConnectorWithBlock(BlockingConnector.class, CONNECTOR_START);
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testBlockInConnectorStop() throws Exception {
        log.info("Starting test testBlockInConnectorStop");
        createConnectorWithBlock(BlockingConnector.class, CONNECTOR_STOP);
        waitForConnectorStart(BLOCKING_CONNECTOR_NAME);
        connect.deleteConnector(BLOCKING_CONNECTOR_NAME);
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testBlockInSourceTaskStart() throws Exception {
        log.info("Starting test testBlockInSourceTaskStart");
        createConnectorWithBlock(BlockingSourceConnector.class, TASK_START);
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testBlockInSourceTaskStop() throws Exception {
        log.info("Starting test testBlockInSourceTaskStop");
        createConnectorWithBlock(BlockingSourceConnector.class, TASK_STOP);
        waitForConnectorStart(BLOCKING_CONNECTOR_NAME);
        connect.deleteConnector(BLOCKING_CONNECTOR_NAME);
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testBlockInSinkTaskStart() throws Exception {
        log.info("Starting test testBlockInSinkTaskStart");
        createConnectorWithBlock(BlockingSinkConnector.class, TASK_START);
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testBlockInSinkTaskStop() throws Exception {
        log.info("Starting test testBlockInSinkTaskStop");
        createConnectorWithBlock(BlockingSinkConnector.class, TASK_STOP);
        waitForConnectorStart(BLOCKING_CONNECTOR_NAME);
        connect.deleteConnector(BLOCKING_CONNECTOR_NAME);
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();
    }

    @Test
    public void testWorkerRestartWithBlockInConnectorStart() throws Exception {
        log.info("Starting test testWorkerRestartWithBlockInConnectorStart");
        createConnectorWithBlock(BlockingConnector.class, CONNECTOR_START);
        // First instance of the connector should block on startup
        Block.waitForBlock();
        createNormalConnector();
        connect.removeWorker();

        connect.addWorker();
        // After stopping the only worker and restarting it, a new instance of the blocking
        // connector should be created and we can ensure that it blocks again
        Block.waitForBlock();
        verifyNormalConnector();
    }

    @Test
    public void testWorkerRestartWithBlockInConnectorStop() throws Exception {
        log.info("Starting test testWorkerRestartWithBlockInConnectorStop");
        createConnectorWithBlock(BlockingConnector.class, CONNECTOR_STOP);
        waitForConnectorStart(BLOCKING_CONNECTOR_NAME);
        createNormalConnector();
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
        connect.removeWorker();
        Block.waitForBlock();

        connect.addWorker();
        waitForConnectorStart(BLOCKING_CONNECTOR_NAME);
        verifyNormalConnector();
    }

    private void createConnectorWithBlock(Class<? extends Connector> connectorClass, String block) {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(TOPICS_CONFIG, "t1"); // Required for sink connectors
        props.put(Block.BLOCK_CONFIG, Objects.requireNonNull(block));
        log.info("Creating blocking connector of type {} with block in {}", connectorClass.getSimpleName(), block);
        try {
            connect.configureConnector(BLOCKING_CONNECTOR_NAME, props);
        } catch (RuntimeException e) {
            log.info("Failed to create connector", e);
            throw e;
        }
    }

    private void createNormalConnector() {
        connect.kafka().createTopic(TEST_TOPIC, 3);

        normalConnectorHandle = RuntimeHandles.get().connectorHandle(NORMAL_CONNECTOR_NAME);
        normalConnectorHandle.expectedRecords(NUM_RECORDS_PRODUCED);
        normalConnectorHandle.expectedCommits(NUM_RECORDS_PRODUCED);

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");
        props.put(MonitorableSourceConnector.TOPIC_CONFIG, TEST_TOPIC);
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

    private void verifyNormalConnector() throws InterruptedException {
        waitForConnectorStart(NORMAL_CONNECTOR_NAME);
        normalConnectorHandle.awaitRecords(RECORD_TRANSFER_DURATION_MS);
        normalConnectorHandle.awaitCommits(RECORD_TRANSFER_DURATION_MS);
    }

    private static class Block {
        private static CountDownLatch blockLatch;

        private final String block;

        public static final String BLOCK_CONFIG = "block";

        private static ConfigDef config() {
            return new ConfigDef()
                .define(
                    BLOCK_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Where to block indefinitely, e.g., 'Connector::start', 'Connector::initialize', " 
                        + "'Connector::taskConfigs', 'Task::version', 'SinkTask::put', 'SourceTask::poll'"
                );
        }

        public static void waitForBlock() throws InterruptedException {
            synchronized (Block.class) {
                if (blockLatch == null) {
                    throw new IllegalArgumentException("No connector has been created yet");
                }
            }

            log.debug("Waiting for connector to block");
            blockLatch.await();
            log.debug("Connector should now be blocked");
        }

        // Note that there is only ever at most one global block latch at a time, which makes tests that
        // use blocks in multiple places impossible. If necessary, this can be addressed in the future by
        // adding support for multiple block latches at a time, possibly identifiable by a connector/task
        // ID, the location of the expected block, or both.
        public static void resetBlockLatch() {
            synchronized (Block.class) {
                if (blockLatch != null) {
                    blockLatch.countDown();
                    blockLatch = null;
                }
            }
        }

        public Block(Map<String, String> props) {
            this(new AbstractConfig(config(), props).getString(BLOCK_CONFIG));
        }

        public Block(String block) {
            this.block = block;
            synchronized (Block.class) {
                if (blockLatch != null) {
                    blockLatch.countDown();
                }
                blockLatch = new CountDownLatch(1);
            }
        }

        public Map<String, String> taskConfig() {
            return Collections.singletonMap(BLOCK_CONFIG, block);
        }

        public void maybeBlockOn(String block) {
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
    }

    // Used to test blocks in Connector (as opposed to Task) methods
    public static class BlockingConnector extends SourceConnector {

        private Block block;

        // No-args constructor required by the framework
        public BlockingConnector() {
            this(null);
        }

        protected BlockingConnector(String block) {
            this.block = new Block(block);
        }

        @Override
        public void initialize(ConnectorContext ctx) {
            block.maybeBlockOn(CONNECTOR_INITIALIZE);
            super.initialize(ctx);
        }

        @Override
        public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
            block.maybeBlockOn(CONNECTOR_INITIALIZE_WITH_TASK_CONFIGS);
            super.initialize(ctx, taskConfigs);
        }

        @Override
        public void start(Map<String, String> props) {
            this.block = new Block(props);
            block.maybeBlockOn(CONNECTOR_START);
        }

        @Override
        public void reconfigure(Map<String, String> props) {
            block.maybeBlockOn(CONNECTOR_RECONFIGURE);
            super.reconfigure(props);
        }

        @Override
        public Class<? extends Task> taskClass() {
            block.maybeBlockOn(CONNECTOR_TASK_CLASS);
            return BlockingTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            block.maybeBlockOn(CONNECTOR_TASK_CONFIGS);
            return Collections.singletonList(Collections.emptyMap());
        }

        @Override
        public void stop() {
            block.maybeBlockOn(CONNECTOR_STOP);
        }

        @Override
        public Config validate(Map<String, String> connectorConfigs) {
            block.maybeBlockOn(CONNECTOR_VALIDATE);
            return super.validate(connectorConfigs);
        }

        @Override
        public ConfigDef config() {
            block.maybeBlockOn(CONNECTOR_CONFIG);
            return Block.config();
        }

        @Override
        public String version() {
            block.maybeBlockOn(CONNECTOR_VERSION);
            return "0.0.0";
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
            super(CONNECTOR_INITIALIZE);
        }
    }

    public static class ConfigBlockingConnector extends BlockingConnector {
        public ConfigBlockingConnector() {
            super(CONNECTOR_CONFIG);
        }
    }

    public static class ValidateBlockingConnector extends BlockingConnector {
        public ValidateBlockingConnector() {
            super(CONNECTOR_VALIDATE);
        }
    }

    // Used to test blocks in SourceTask methods
    public static class BlockingSourceConnector extends SourceConnector {

        private Map<String, String> props;
        private final Class<? extends BlockingSourceTask> taskClass;

        // No-args constructor required by the framework
        public BlockingSourceConnector() {
            this(BlockingSourceTask.class);
        }

        protected BlockingSourceConnector(Class<? extends BlockingSourceTask> taskClass) {
            this.taskClass = taskClass;
        }

        @Override
        public void start(Map<String, String> props) {
            this.props = props;
        }

        @Override
        public Class<? extends Task> taskClass() {
            return taskClass;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return IntStream.range(0, maxTasks)
                .mapToObj(i -> new HashMap<>(props))
                .collect(Collectors.toList());
        }

        @Override
        public void stop() {
        }

        @Override
        public Config validate(Map<String, String> connectorConfigs) {
            return super.validate(connectorConfigs);
        }

        @Override
        public ConfigDef config() {
            return Block.config();
        }

        @Override
        public String version() {
            return "0.0.0";
        }

        public static class BlockingSourceTask extends SourceTask {
            private Block block;

            // No-args constructor required by the framework
            public BlockingSourceTask() {
                this(null);
            }

            protected BlockingSourceTask(String block) {
                this.block = new Block(block);
            }

            @Override
            public void start(Map<String, String> props) {
                this.block = new Block(props);
                block.maybeBlockOn(TASK_START);
            }

            @Override
            public List<SourceRecord> poll() {
                block.maybeBlockOn(SOURCE_TASK_POLL);
                return null;
            }

            @Override
            public void stop() {
                block.maybeBlockOn(TASK_STOP);
            }

            @Override
            public String version() {
                block.maybeBlockOn(TASK_VERSION);
                return "0.0.0";
            }

            @Override
            public void initialize(SourceTaskContext context) {
                block.maybeBlockOn(SOURCE_TASK_INITIALIZE);
                super.initialize(context);
            }

            @Override
            public void commit() throws InterruptedException {
                block.maybeBlockOn(SOURCE_TASK_COMMIT);
                super.commit();
            }

            @Override
            @SuppressWarnings("deprecation")
            public void commitRecord(SourceRecord record) throws InterruptedException {
                block.maybeBlockOn(SOURCE_TASK_COMMIT_RECORD);
                super.commitRecord(record);
            }

            @Override
            public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
                block.maybeBlockOn(SOURCE_TASK_COMMIT_RECORD_WITH_METADATA);
                super.commitRecord(record, metadata);
            }
        }
    }

    public static class TaskInitializeBlockingSourceConnector extends BlockingSourceConnector {
        public TaskInitializeBlockingSourceConnector() {
            super(InitializeBlockingSourceTask.class);
        }

        public static class InitializeBlockingSourceTask extends BlockingSourceTask {
            public InitializeBlockingSourceTask() {
                super(SOURCE_TASK_INITIALIZE);
            }
        }
    }

    // Used to test blocks in SinkTask methods
    public static class BlockingSinkConnector extends SinkConnector {

        private Map<String, String> props;
        private final Class<? extends BlockingSinkTask> taskClass;

        // No-args constructor required by the framework
        public BlockingSinkConnector() {
            this(BlockingSinkTask.class);
        }

        protected BlockingSinkConnector(Class<? extends BlockingSinkTask> taskClass) {
            this.taskClass = taskClass;
        }

        @Override
        public void start(Map<String, String> props) {
            this.props = props;
        }

        @Override
        public Class<? extends Task> taskClass() {
            return taskClass;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return IntStream.rangeClosed(0, maxTasks)
                .mapToObj(i -> new HashMap<>(props))
                .collect(Collectors.toList());
        }

        @Override
        public void stop() {
        }

        @Override
        public Config validate(Map<String, String> connectorConfigs) {
            return super.validate(connectorConfigs);
        }

        @Override
        public ConfigDef config() {
            return Block.config();
        }

        @Override
        public String version() {
            return "0.0.0";
        }

        public static class BlockingSinkTask extends SinkTask {
            private Block block;

            // No-args constructor required by the framework
            public BlockingSinkTask() {
                this(null);
            }

            protected BlockingSinkTask(String block) {
                this.block = new Block(block);
            }

            @Override
            public void start(Map<String, String> props) {
                this.block = new Block(props);
                block.maybeBlockOn(TASK_START);
            }

            @Override
            public void put(Collection<SinkRecord> records) {
                block.maybeBlockOn(SINK_TASK_PUT);
            }

            @Override
            public void stop() {
                block.maybeBlockOn(TASK_STOP);
            }

            @Override
            public String version() {
                block.maybeBlockOn(TASK_VERSION);
                return "0.0.0";
            }

            @Override
            public void initialize(SinkTaskContext context) {
                block.maybeBlockOn(SINK_TASK_INITIALIZE);
                super.initialize(context);
            }

            @Override
            public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
                block.maybeBlockOn(SINK_TASK_FLUSH);
                super.flush(currentOffsets);
            }

            @Override
            public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
                block.maybeBlockOn(SINK_TASK_PRE_COMMIT);
                return super.preCommit(currentOffsets);
            }

            @Override
            public void open(Collection<TopicPartition> partitions) {
                block.maybeBlockOn(SINK_TASK_OPEN);
                super.open(partitions);
            }

            @Override
            @SuppressWarnings("deprecation")
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                block.maybeBlockOn(SINK_TASK_ON_PARTITIONS_ASSIGNED);
                super.onPartitionsAssigned(partitions);
            }

            @Override
            public void close(Collection<TopicPartition> partitions) {
                block.maybeBlockOn(SINK_TASK_CLOSE);
                super.close(partitions);
            }

            @Override
            @SuppressWarnings("deprecation")
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                block.maybeBlockOn(SINK_TASK_ON_PARTITIONS_REVOKED);
                super.onPartitionsRevoked(partitions);
            }
        }
    }

    public static class TaskInitializeBlockingSinkConnector extends BlockingSinkConnector {
        public TaskInitializeBlockingSinkConnector() {
            super(InitializeBlockingSinkTask.class);
        }

        public static class InitializeBlockingSinkTask extends BlockingSinkTask {
            public InitializeBlockingSinkTask() {
                super(SINK_TASK_INITIALIZE);
            }
        }
    }

    // We don't declare a class here that blocks in the version() method since that method is used
    // in plugin path scanning. Until/unless plugin path scanning is altered to not block completely
    // on connectors' version() methods, we can't even declare a class that does that without
    // causing the workers in this test to hang on startup.
}
