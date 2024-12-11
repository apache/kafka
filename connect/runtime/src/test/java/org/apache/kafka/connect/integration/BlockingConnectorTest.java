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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import jakarta.ws.rs.core.Response;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests situations during which certain connector operations, such as start, validation,
 * configuration and others, take longer than expected.
 */
@Tag("integration")
public class BlockingConnectorTest {

    private static final Logger log = LoggerFactory.getLogger(BlockingConnectorTest.class);

    private static final int NUM_WORKERS = 1;
    private static final String BLOCKING_CONNECTOR_NAME = "blocking-connector";
    private static final String NORMAL_CONNECTOR_NAME = "normal-connector";
    private static final String TEST_TOPIC = "normal-topic";
    private static final int NUM_RECORDS_PRODUCED = 100;
    private static final long CONNECTOR_BLOCK_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long RECORD_TRANSFER_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long REDUCED_REST_REQUEST_TIMEOUT = Worker.CONNECTOR_GRACEFUL_SHUTDOWN_TIMEOUT_MS * 2;

    private static final String CONNECTOR_INITIALIZE = "Connector::initialize";
    private static final String CONNECTOR_INITIALIZE_WITH_TASK_CONFIGS = "Connector::initializeWithTaskConfigs";
    static final String CONNECTOR_START = "Connector::start";
    private static final String CONNECTOR_RECONFIGURE = "Connector::reconfigure";
    private static final String CONNECTOR_TASK_CLASS = "Connector::taskClass";
    static final String CONNECTOR_TASK_CONFIGS = "Connector::taskConfigs";
    private static final String CONNECTOR_STOP = "Connector::stop";
    private static final String CONNECTOR_VALIDATE = "Connector::validate";
    private static final String CONNECTOR_CONFIG = "Connector::config";
    private static final String CONNECTOR_VERSION = "Connector::version";
    private static final String TASK_START = "Task::start";
    static final String TASK_STOP = "Task::stop";
    private static final String TASK_VERSION = "Task::version";
    private static final String SINK_TASK_INITIALIZE = "SinkTask::initialize";
    private static final String SINK_TASK_PUT = "SinkTask::put";
    private static final String SINK_TASK_FLUSH = "SinkTask::flush";
    private static final String SINK_TASK_PRE_COMMIT = "SinkTask::preCommit";
    private static final String SINK_TASK_OPEN = "SinkTask::open";
    private static final String SINK_TASK_CLOSE = "SinkTask::close";
    private static final String SOURCE_TASK_INITIALIZE = "SourceTask::initialize";
    private static final String SOURCE_TASK_POLL = "SourceTask::poll";
    private static final String SOURCE_TASK_COMMIT = "SourceTask::commit";
    private static final String SOURCE_TASK_COMMIT_RECORD = "SourceTask::commitRecord";
    private static final String SOURCE_TASK_COMMIT_RECORD_WITH_METADATA = "SourceTask::commitRecordWithMetadata";

    private EmbeddedConnectCluster connect;
    private ConnectorHandle normalConnectorHandle;

    @BeforeEach
    public void setup() throws Exception {
        // build a Connect cluster backed by a Kafka KRaft cluster
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

    @AfterEach
    public void close() {
        // stop the Connect cluster and its backing Kafka cluster.
        connect.stop();
        // unblock everything so that we don't leak threads after each test run
        Block.reset();
        Block.join();
    }

    @Test
    public void testBlockInConnectorValidate() throws Exception {
        log.info("Starting test testBlockInConnectorValidate");
        assertRequestTimesOut(
                "create connector that blocks during validation",
                () -> createConnectorWithBlock(ValidateBlockingConnector.class, CONNECTOR_VALIDATE),
                "The worker is currently performing multi-property validation for the connector"
        );
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
        assertRequestTimesOut(
                "create connector that blocks while getting config",
                () -> createConnectorWithBlock(ConfigBlockingConnector.class, CONNECTOR_CONFIG),
                "The worker is currently retrieving the configuration definition from the connector"
        );
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

        // Try to restart the connector
        assertRequestTimesOut(
                "restart connector that blocks during initialize",
                () -> connect.restartConnector(BLOCKING_CONNECTOR_NAME),
                "The worker is currently starting the connector"
        );
    }

    @Test
    public void testBlockInConnectorStart() throws Exception {
        log.info("Starting test testBlockInConnectorStart");
        createConnectorWithBlock(BlockingConnector.class, CONNECTOR_START);
        Block.waitForBlock();

        createNormalConnector();
        verifyNormalConnector();

        // Try to restart the connector
        assertRequestTimesOut(
                "restart connector that blocks during start",
                () -> connect.restartConnector(BLOCKING_CONNECTOR_NAME),
                "The worker is currently starting the connector"
        );
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
        normalConnectorHandle.awaitRecords(RECORD_TRANSFER_TIMEOUT_MS);
        normalConnectorHandle.awaitCommits(RECORD_TRANSFER_TIMEOUT_MS);
    }

    private void assertRequestTimesOut(String requestDescription, Executable request, String expectedTimeoutMessage) {
        // Artificially reduce the REST request timeout so that these don't take 90 seconds
        connect.requestTimeout(REDUCED_REST_REQUEST_TIMEOUT);
        ConnectRestException exception = assertThrows(
                ConnectRestException.class, request,
                "Should have failed to " + requestDescription
        );
        assertEquals(
                Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), exception.statusCode(),
                "Should have gotten 500 error from trying to " + requestDescription
        );
        assertTrue(
                exception.getMessage().contains("Request timed out"),
                "Should have gotten timeout message from trying to " + requestDescription
                        + "; instead, message was: " + exception.getMessage()
        );
        if (expectedTimeoutMessage != null) {
            assertTrue(
                    exception.getMessage().contains(expectedTimeoutMessage),
                    "Timeout error message '" + exception.getMessage() + "' does not match expected format"
            );
        }
        // Reset the REST request timeout so that other requests aren't impacted
        connect.resetRequestTimeout();
    }

    public static class Block {
        // All latches that blocking connectors/tasks are or will be waiting on during a test case
        private static final Set<CountDownLatch> BLOCK_LATCHES = new HashSet<>();
        // All threads that are or were at one point blocked
        private static final Set<Thread> BLOCKED_THREADS = new HashSet<>();
        // The latch that can be used to wait for a connector/task to reach the most-recently-registered blocking point
        private static CountDownLatch awaitBlockLatch;

        private final String block;

        public static final String BLOCK_CONFIG = "block";

        public static ConfigDef config() {
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

        /**
         * {@link CountDownLatch#await() Wait} for the connector/task to reach the point in its lifecycle where
         * it will block.
         */
        public static void waitForBlock() throws InterruptedException, TimeoutException {
            Timer timer = Time.SYSTEM.timer(CONNECTOR_BLOCK_TIMEOUT_MS);

            CountDownLatch awaitBlockLatch;
            synchronized (Block.class) {
                while (Block.awaitBlockLatch == null) {
                    timer.update();
                    if (timer.isExpired()) {
                        throw new TimeoutException("Timed out waiting for connector to block.");
                    }
                    Block.class.wait(timer.remainingMs());
                }
                awaitBlockLatch = Block.awaitBlockLatch;
            }

            log.debug("Waiting for connector to block");
            timer.update();
            if (!awaitBlockLatch.await(timer.remainingMs(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Timed out waiting for connector to block.");
            }
            log.debug("Connector should now be blocked");
        }

        /**
         * {@link CountDownLatch#countDown() Release} any latches allocated over the course of a test
         * to either await a connector/task reaching a blocking point, or cause a connector/task to block.
         */
        public static synchronized void reset() {
            resetAwaitBlockLatch();
            BLOCK_LATCHES.forEach(CountDownLatch::countDown);
            BLOCK_LATCHES.clear();
        }

        /**
         * {@link Thread#join(long millis) Await} the termination of all threads that have been
         * intentionally blocked either since the last invocation of this method or, if this method
         * has never been invoked, all threads that have ever been blocked.
         */
        public static synchronized void join() {
            BLOCKED_THREADS.forEach(t -> {
                try {
                    t.join(30_000);
                    if (t.isAlive()) {
                        log.warn(
                                "Thread {} failed to finish in time; current stack trace:\n{}",
                                t,
                                Stream.of(t.getStackTrace())
                                        .map(s -> String.format(
                                                "\t%s.%s:%d",
                                                s.getClassName(),
                                                s.getMethodName(),
                                                s.getLineNumber()
                                        )).collect(Collectors.joining("\n"))
                        );
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while waiting for blocked thread " + t + " to finish");
                }
            });
            BLOCKED_THREADS.clear();
        }

        // Note that there is only ever at most one global await-block latch at a time, which makes tests that
        // use blocks in multiple places impossible. If necessary, this can be addressed in the future by
        // adding support for multiple await-block latches at a time, possibly identifiable by a connector/task
        // ID, the location of the expected block, or both.
        private static synchronized void resetAwaitBlockLatch() {
            if (awaitBlockLatch != null) {
                awaitBlockLatch.countDown();
                awaitBlockLatch = null;
            }
        }

        private static CountDownLatch newBlockLatch() {
            CountDownLatch result = new CountDownLatch(1);
            synchronized (Block.class) {
                BLOCK_LATCHES.add(result);
            }
            return result;
        }

        public Block(Map<String, String> props) {
            this(new AbstractConfig(config(), props).getString(BLOCK_CONFIG));
        }

        public Block(String block) {
            this.block = block;
            if (block != null) {
                synchronized (Block.class) {
                    resetAwaitBlockLatch();
                    awaitBlockLatch = new CountDownLatch(1);
                    Block.class.notify();
                }
            }
        }

        public void maybeBlockOn(String block) {
            if (block.equals(this.block)) {
                log.info("Will block on {}", block);
                CountDownLatch blockLatch;
                synchronized (Block.class) {
                    assertNotNull(
                            awaitBlockLatch,
                            "Block was reset prematurely"
                    );
                    awaitBlockLatch.countDown();
                    blockLatch = newBlockLatch();
                    BLOCKED_THREADS.add(Thread.currentThread());
                }
                while (true) {
                    try {
                        blockLatch.await();
                        log.debug("Instructed to stop blocking; will resume normal execution");
                        return;
                    } catch (InterruptedException e) {
                        log.debug("Interrupted while blocking; will continue blocking until instructed to stop");
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
            return IntStream.range(0, maxTasks)
                .mapToObj(i -> new HashMap<>(props))
                .collect(Collectors.toList());
        }

        @Override
        public void stop() {
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
            public void close(Collection<TopicPartition> partitions) {
                block.maybeBlockOn(SINK_TASK_CLOSE);
                super.close(partitions);
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
