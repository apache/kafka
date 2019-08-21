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
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
public class KafkaStreamsTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_THREADS = 2;
    // We need this to avoid the KafkaConsumer hanging on poll
    // (this may occur if the test doesn't complete quickly enough)
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final StreamsBuilder builder = new StreamsBuilder();
    private KafkaStreams globalStreams;
    private Properties props;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "clientId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
        globalStreams = new KafkaStreams(builder.build(), props);
    }

    @After
    public void cleanup() {
        if (globalStreams != null) {
            globalStreams.close();
        }
    }

    @Test
    public void stateShouldTransitToNotRunningIfCloseRightAfterCreated() {
        globalStreams.close();
        assertThat(globalStreams.state(), is(KafkaStreams.State.NOT_RUNNING));
    }

    @Test
    public void stateShouldTransitToRunningIfNonDeadThreadsBackToRunning() throws InterruptedException {
        final StateListenerStub stateListener = new StateListenerStub();
        globalStreams.setStateListener(stateListener);

        assertThat(stateListener.numChanges, is(0));
        assertThat(globalStreams.state(), is(KafkaStreams.State.CREATED));

        globalStreams.start();

        TestUtils.waitForCondition(
            () -> stateListener.numChanges == 2,
            "Streams never started.");
        assertThat(globalStreams.state(), is(KafkaStreams.State.RUNNING));

        for (final StreamThread thread: globalStreams.threads) {
            thread.stateListener().onChange(
                thread,
                StreamThread.State.PARTITIONS_REVOKED,
                StreamThread.State.RUNNING);
        }

        assertThat(stateListener.numChanges, is(3));
        assertThat(globalStreams.state(), is(KafkaStreams.State.REBALANCING));

        for (final StreamThread thread : globalStreams.threads) {
            thread.stateListener().onChange(
                thread,
                StreamThread.State.PARTITIONS_ASSIGNED,
                StreamThread.State.PARTITIONS_REVOKED);
        }

        assertThat(stateListener.numChanges, is(3));
        assertThat(globalStreams.state(), is(KafkaStreams.State.REBALANCING));

        globalStreams.threads[NUM_THREADS - 1].stateListener().onChange(
            globalStreams.threads[NUM_THREADS - 1],
            StreamThread.State.PENDING_SHUTDOWN,
            StreamThread.State.PARTITIONS_ASSIGNED);

        globalStreams.threads[NUM_THREADS - 1].stateListener().onChange(
            globalStreams.threads[NUM_THREADS - 1],
            StreamThread.State.DEAD,
            StreamThread.State.PENDING_SHUTDOWN);

        assertThat(stateListener.numChanges, is(3));
        assertThat(globalStreams.state(), is(KafkaStreams.State.REBALANCING));

        for (final StreamThread thread : globalStreams.threads) {
            if (thread != globalStreams.threads[NUM_THREADS - 1]) {
                thread.stateListener().onChange(
                    thread,
                    StreamThread.State.RUNNING,
                    StreamThread.State.PARTITIONS_ASSIGNED);
            }
        }

        assertThat(stateListener.numChanges, is(4));
        assertThat(globalStreams.state(), is(KafkaStreams.State.RUNNING));

        globalStreams.close();

        TestUtils.waitForCondition(
            () -> stateListener.numChanges == 6,
            "Streams never closed.");
        assertThat(globalStreams.state(), is(KafkaStreams.State.NOT_RUNNING));
    }

    @Test
    public void stateShouldTransitToErrorIfAllThreadsDead() throws InterruptedException {
        final StateListenerStub stateListener = new StateListenerStub();
        globalStreams.setStateListener(stateListener);

        assertThat(stateListener.numChanges, is(0));
        assertThat(globalStreams.state(), is(KafkaStreams.State.CREATED));

        globalStreams.start();

        TestUtils.waitForCondition(
            () -> stateListener.numChanges == 2,
            "Streams never started.");
        assertThat(globalStreams.state(), is(KafkaStreams.State.RUNNING));

        for (final StreamThread thread : globalStreams.threads) {
            thread.stateListener().onChange(
                thread,
                StreamThread.State.PARTITIONS_REVOKED,
                StreamThread.State.RUNNING);
        }

        assertThat(stateListener.numChanges, is(3));
        assertThat(globalStreams.state(), is(KafkaStreams.State.REBALANCING));

        globalStreams.threads[NUM_THREADS - 1].stateListener().onChange(
            globalStreams.threads[NUM_THREADS - 1],
            StreamThread.State.PENDING_SHUTDOWN,
            StreamThread.State.PARTITIONS_REVOKED);

        globalStreams.threads[NUM_THREADS - 1].stateListener().onChange(
            globalStreams.threads[NUM_THREADS - 1],
            StreamThread.State.DEAD,
            StreamThread.State.PENDING_SHUTDOWN);

        assertThat(stateListener.numChanges, is(3));
        assertThat(globalStreams.state(), is(KafkaStreams.State.REBALANCING));

        for (final StreamThread thread : globalStreams.threads) {
            if (thread != globalStreams.threads[NUM_THREADS - 1]) {
                thread.stateListener().onChange(
                    thread,
                    StreamThread.State.PENDING_SHUTDOWN,
                    StreamThread.State.PARTITIONS_REVOKED);

                thread.stateListener().onChange(
                    thread,
                    StreamThread.State.DEAD,
                    StreamThread.State.PENDING_SHUTDOWN);
            }
        }

        assertThat(stateListener.numChanges, is(4));
        assertThat(globalStreams.state(), is(KafkaStreams.State.ERROR));

        globalStreams.close();

        // the state should not stuck with ERROR, but transit to NOT_RUNNING in the end
        TestUtils.waitForCondition(
            () -> stateListener.numChanges == 6,
            "Streams never closed.");
        assertThat(globalStreams.state(), is(KafkaStreams.State.NOT_RUNNING));
    }

    @Test
    public void shouldCleanupResourcesOnCloseWithoutPreviousStart() throws Exception {
        builder.globalTable("anyTopic");
        final List<Node> nodes = Collections.singletonList(new Node(0, "localhost", 8121));
        final Cluster cluster = new Cluster("mockClusterId", nodes,
                                            Collections.emptySet(), Collections.emptySet(),
                                            Collections.emptySet(), nodes.get(0));
        final MockClientSupplier clientSupplier = new MockClientSupplier();
        clientSupplier.setClusterForAdminClient(cluster);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, clientSupplier);
        streams.close();
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams never stopped.");

        // Ensure that any created clients are closed
        assertTrue(clientSupplier.consumer.closed());
        assertTrue(clientSupplier.restoreConsumer.closed());
        for (final MockProducer p : clientSupplier.producers) {
            assertTrue(p.closed());
        }
    }

    @Test
    public void testStateThreadClose() throws Exception {
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            final java.lang.reflect.Field threadsField = streams.getClass().getDeclaredField("threads");
            threadsField.setAccessible(true);
            final StreamThread[] threads = (StreamThread[]) threadsField.get(streams);

            assertThat(threads.length, is(NUM_THREADS));
            assertThat(streams.state(), is(KafkaStreams.State.CREATED));

            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");

            for (int i = 0; i < NUM_THREADS; i++) {
                final StreamThread tmpThread = threads[i];
                tmpThread.shutdown();
                TestUtils.waitForCondition(
                    () -> tmpThread.state() == StreamThread.State.DEAD,
                    "Thread never stopped.");
                threads[i].join();
            }
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.ERROR,
                "Streams never stopped.");
        } finally {
            streams.close();
        }

        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.NOT_RUNNING,
            "Streams never stopped.");

        final java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
        globalThreadField.setAccessible(true);
        final GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
        assertNull(globalStreamThread);
    }

    @Test
    public void testStateGlobalThreadClose() throws Exception {
        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            streams.start();
            TestUtils.waitForCondition(
                () -> streams.state() == KafkaStreams.State.RUNNING,
                "Streams never started.");
            final java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
            globalThreadField.setAccessible(true);
            final GlobalStreamThread globalStreamThread = (GlobalStreamThread) globalThreadField.get(streams);
            globalStreamThread.shutdown();
            TestUtils.waitForCondition(
                () -> globalStreamThread.state() == GlobalStreamThread.State.DEAD,
                "Thread never stopped.");
            globalStreamThread.join();
            assertThat(streams.state(), is(KafkaStreams.State.ERROR));
        } finally {
            streams.close();
        }

        assertThat(streams.state(), is(KafkaStreams.State.NOT_RUNNING));
    }

    @Test
    public void globalThreadShouldTimeoutWhenBrokerConnectionCannotBeEstablished() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 200);

        // make sure we have the global state thread running too
        builder.globalTable("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            assertThrows(StreamsException.class, streams::start);
        }
    }

    @Test
    public void testLocalThreadCloseWithoutConnectingToBroker() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.setProperty(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        // make sure we have the global state thread running too
        builder.table("anyTopic");
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.start();
        }
        // There's nothing to assert... We're testing that this operation actually completes.
    }


    @Test
    public void testInitializesAndDestroysMetricsReporters() {
        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            final int newInitCount = MockMetricsReporter.INIT_COUNT.get();
            final int initDiff = newInitCount - oldInitCount;
            assertThat("some reporters should be initialized by calling on construction",
                initDiff, greaterThan(0));

            streams.start();
            final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
            streams.close();
            assertThat(MockMetricsReporter.CLOSE_COUNT.get(), is(oldCloseCount + initDiff));
        }
    }

    @Test
    public void testCloseIsIdempotent() {
        globalStreams.close();
        final int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        globalStreams.close();
        assertThat("subsequent close() calls should do nothing",
            MockMetricsReporter.CLOSE_COUNT.get(), is(closeCount));
    }

    @Test
    public void testCannotStartOnceClosed() {
        globalStreams.start();
        globalStreams.close();
        assertThrows(IllegalStateException.class, globalStreams::start);
    }

    @Test
    public void testCannotStartTwice() {
        globalStreams.start();
        assertThrows(IllegalStateException.class, globalStreams::start);
    }

    @Test
    public void shouldNotSetGlobalRestoreListenerAfterStarting() {
        globalStreams.start();
        assertThrows(IllegalStateException.class, () -> globalStreams.setGlobalStateRestoreListener(new MockStateRestoreListener()));
    }

    @Test
    public void shouldThrowExceptionSettingUncaughtExceptionHandlerNotInCreateState() {
        globalStreams.start();
        assertThrows(IllegalStateException.class, () -> globalStreams.setUncaughtExceptionHandler(null));
    }

    @Test
    public void shouldThrowExceptionSettingStateListenerNotInCreateState() {
        globalStreams.start();
        assertThrows(IllegalStateException.class, () -> globalStreams.setStateListener(null));
    }

    @Test
    public void shouldNotGetAllTasksWhenNotRunning() {
        assertThrows(IllegalStateException.class, globalStreams::allMetadata);
    }

    @Test
    public void shouldNotGetAllTasksWithStoreWhenNotRunning() {
        assertThrows(IllegalStateException.class, () -> globalStreams.allMetadataForStore("store"));
    }

    @Test
    public void shouldNotGetTaskWithKeyAndSerializerWhenNotRunning() {
        assertThrows(IllegalStateException.class, () -> globalStreams.metadataForKey("store", "key", Serdes.String().serializer()));
    }

    @Test
    public void shouldNotGetTaskWithKeyAndPartitionerWhenNotRunning() {
        assertThrows(IllegalStateException.class, () -> globalStreams.metadataForKey("store", "key", (topic, key, value, numPartitions) -> 0));
    }

    @Test
    public void shouldReturnFalseOnCloseWhenThreadsHaveNotTerminated() throws Exception {
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        KafkaStreams streams = null;
        try {
            final StreamsBuilder builder = new StreamsBuilder();
            final CountDownLatch latch = new CountDownLatch(1);
            final String topic = "input";
            CLUSTER.createTopics(topic);

            builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
                    .foreach((key, value) -> {
                        try {
                            latch.countDown();
                            while (keepRunning.get()) {
                                Thread.sleep(10);
                            }
                        } catch (final InterruptedException e) {
                            // no-op
                        }
                    });
            streams = new KafkaStreams(builder.build(), props);
            streams.start();
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic,
                Collections.singletonList(new KeyValue<>("A", "A")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    StringSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                System.currentTimeMillis());

            assertTrue("Timed out waiting to receive single message", latch.await(30, TimeUnit.SECONDS));
            assertFalse(streams.close(Duration.ofMillis(10)));
        } finally {
            // stop the thread so we don't interfere with other tests etc
            keepRunning.set(false);
            if (streams != null) {
                streams.close();
            }
        }
    }

    @Test
    public void shouldReturnThreadMetadata() {
        globalStreams.start();
        final Set<ThreadMetadata> threadMetadata = globalStreams.localThreadsMetadata();
        assertThat(threadMetadata.size(), is(2));
        for (final ThreadMetadata metadata : threadMetadata) {
            assertTrue("#threadState() was: " + metadata.threadState() + "; expected either RUNNING, STARTING, PARTITIONS_REVOKED, PARTITIONS_ASSIGNED, or CREATED",
                asList("RUNNING", "STARTING", "PARTITIONS_REVOKED", "PARTITIONS_ASSIGNED", "CREATED").contains(metadata.threadState()));
            assertThat(metadata.standbyTasks().size(), is(0));
            assertThat(metadata.activeTasks().size(), is(0));
            final String threadName = metadata.threadName();
            assertTrue(threadName.startsWith("clientId-StreamThread-"));
            assertThat(metadata.consumerClientId(), is(threadName + "-consumer"));
            assertThat(metadata.restoreConsumerClientId(), is(threadName + "-restore-consumer"));
            assertThat(metadata.producerClientIds(), is(Collections.singleton(threadName + "-producer")));
            assertThat(metadata.adminClientId(), is("clientId-admin"));
        }
    }

    @Test
    public void shouldAllowCleanupBeforeStartAndAfterClose() throws Exception {
        final KafkaStreams streams = getStatefulApp();
        try {
            streams.cleanUp();
            streams.start();
        } finally {
            streams.close();
        }
        streams.cleanUp();
    }

    @Test
    public void shouldAllowCleanupIfApplicationIsDown() throws Exception {
        final KafkaStreams streams = getStatefulApp();

        final File taskDir = new File(
            props.getProperty(StreamsConfig.STATE_DIR_CONFIG)
                + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
                + File.separator + new TaskId(0, 0));
        final File testFile = new File(taskDir, "test-file");

        assertTrue(taskDir.mkdirs());
        assertTrue(testFile.createNewFile());

        streams.cleanUp();
        assertFalse(testFile.exists());
        assertFalse(taskDir.exists());

        streams.start();
        streams.close();

        assertTrue(taskDir.mkdirs());
        assertTrue(testFile.createNewFile());

        streams.cleanUp();
        assertFalse(testFile.exists());
        assertFalse(taskDir.exists());
    }

    @Test
    public void shouldAllowCleanupIfLockIsReleased() throws Exception {
        final KafkaStreams streams = getStatefulApp();

        final File taskDir = new File(
            props.getProperty(StreamsConfig.STATE_DIR_CONFIG)
                + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
                + File.separator + new TaskId(0, 0));
        final File testFile = new File(taskDir, "test-file");
        assertTrue(taskDir.mkdirs());
        assertTrue(testFile.createNewFile());

        final File lockFile = new File(taskDir, ".lock");
        final FileChannel channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        final FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (final Exception e) {
            if (channel != null) {
                channel.close();
            }
            throw e;
        }
        try {
            lock.release();
        } finally {
            channel.close();
        }

        assertTrue(lockFile.exists());

        streams.cleanUp();

        assertFalse(testFile.exists());
        assertFalse(taskDir.exists());
    }

    @Test
    public void shouldAllowCleanupIfLockIsReleasedFromDifferentThread() throws Exception {
        final KafkaStreams streams = getStatefulApp();

        final File taskDir = new File(
            props.getProperty(StreamsConfig.STATE_DIR_CONFIG)
                + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
                + File.separator + new TaskId(0, 0));
        final File testFile = new File(taskDir, "test-file");
        assertTrue(taskDir.mkdirs());
        assertTrue(testFile.createNewFile());

        final File lockFile = new File(taskDir, ".lock");

        final LockThread lockThread = new LockThread(lockFile);
        lockThread.start();

        while (!lockThread.lockAcquired) {
            Utils.sleep(100);
        }

        lockThread.releaseLock = true;

        while (lockThread.lockAcquired) {
            Utils.sleep(100);
        }
        assertNull(lockThread.error);

        assertTrue(lockFile.exists());

        streams.cleanUp();

        assertFalse(testFile.exists());
        assertFalse(taskDir.exists());

        lockThread.isRunning = false;
        lockThread.join();
    }

    @Test
    public void shouldFailWithOverlappingFileLockException() throws Exception {
        final KafkaStreams streams = getStatefulApp();

        final File taskDir = new File(
            props.getProperty(StreamsConfig.STATE_DIR_CONFIG)
                + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
                + File.separator + new TaskId(0, 0));
        assertTrue(taskDir.mkdirs());

        final File lockFile = new File(taskDir, ".lock");
        try (final FileChannel channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.tryLock();

            final StreamsException expected = assertThrows(StreamsException.class, streams::cleanUp);
            assertThat(expected.getCause(), instanceOf(OverlappingFileLockException.class));
        } finally {
            streams.close();
        }
    }

    @Test
    public void shouldFailWithOverlappingFileLockExceptionWhenLockedByDifferentThread() throws Exception {
        final KafkaStreams streams = getStatefulApp();

        final File taskDir = new File(
            props.getProperty(StreamsConfig.STATE_DIR_CONFIG)
                + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
                + File.separator + new TaskId(0, 0));
        assertTrue(taskDir.mkdirs());

        final File lockFile = new File(taskDir, ".lock");

        final LockThread lockThread = new LockThread(lockFile);
        lockThread.start();

        while (!lockThread.lockAcquired) {
            Utils.sleep(100);
        }

        try {
            final StreamsException expected = assertThrows(StreamsException.class, streams::cleanUp);
            assertThat(expected.getCause(), instanceOf(OverlappingFileLockException.class));
        } finally {
            lockThread.releaseLock = true;
            lockThread.isRunning = false;
            lockThread.join();
        }
        assertNull(lockThread.error);
    }

    @Test
    public void shouldFailWithOverlappingFileLockExceptionForGlobalTask() throws Exception {
        final KafkaStreams streams = getStatefulApp();

        final File taskDir = new File(
            props.getProperty(StreamsConfig.STATE_DIR_CONFIG)
                + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
                + File.separator + "global");
        assertTrue(taskDir.mkdirs());

        final File lockFile = new File(taskDir, ".lock");

        try (final FileChannel channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.tryLock();

            final StreamsException expected = assertThrows(StreamsException.class, streams::cleanUp);
            assertThat(expected.getCause(), instanceOf(OverlappingFileLockException.class));
        }
    }

    @Test
    public void shouldFailWithOverlappingFileLockExceptionForGlobalTaskWhenLockedByDifferentThread() throws Exception {
        final KafkaStreams streams = getStatefulApp();

        final File taskDir = new File(
            props.getProperty(StreamsConfig.STATE_DIR_CONFIG)
                + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)
                + File.separator + "global");
        assertTrue(taskDir.mkdirs());

        final File lockFile = new File(taskDir, ".lock");

        final LockThread lockThread = new LockThread(lockFile);
        lockThread.start();

        while (!lockThread.lockAcquired) {
            Utils.sleep(100);
        }

        try {
            final StreamsException expected = assertThrows(StreamsException.class, streams::cleanUp);
            assertThat(expected.getCause(), instanceOf(OverlappingFileLockException.class));
        } finally {
            lockThread.releaseLock = true;
            lockThread.isRunning = false;
            lockThread.join();
        }
        assertNull(lockThread.error);
    }

    @Test
    public void shouldThrowOnCleanupWhileRunning() throws InterruptedException {
        globalStreams.start();
        TestUtils.waitForCondition(
            () -> globalStreams.state() == KafkaStreams.State.RUNNING,
            "Streams never started.");

        final IllegalStateException expected = assertThrows(IllegalStateException.class, globalStreams::cleanUp);
        assertThat(expected.getMessage(), is("Cannot clean up while running."));
    }

    @Test
    public void shouldCleanupOldStateDirs() throws InterruptedException {
        props.setProperty(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, "1");

        final String topic = "topic";
        CLUSTER.createTopic(topic);
        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(topic, Materialized.as("store"));

        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch latch = new CountDownLatch(1);
            streams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                    latch.countDown();
                }
            });
            final String appDir = props.getProperty(StreamsConfig.STATE_DIR_CONFIG) + File.separator + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
            final File oldTaskDir = new File(appDir, "10_1");
            assertTrue(oldTaskDir.mkdirs());

            streams.start();
            latch.await(30, TimeUnit.SECONDS);
            verifyCleanupStateDir(appDir, oldTaskDir);
            assertTrue(oldTaskDir.mkdirs());
            verifyCleanupStateDir(appDir, oldTaskDir);
        }
    }

    @Test
    public void shouldThrowOnNegativeTimeoutForClose() {
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            streams.close(Duration.ofMillis(-1L));
            fail("should not accept negative close parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void shouldNotBlockInCloseForZeroDuration() throws InterruptedException {
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final Thread th = new Thread(() -> streams.close(Duration.ofMillis(0L)));

        th.start();

        try {
            th.join(30_000L);
            assertFalse(th.isAlive());
        } finally {
            streams.close();
        }
    }

    @Test
    public void statelessTopologyShouldNotCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        CLUSTER.createTopics(inputTopic, outputTopic);

        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
                .addProcessor("process", () -> new AbstractProcessor<String, String>() {
                    @Override
                    public void process(final String key, final String value) {
                        if (value.length() % 2 == 0) {
                            context().forward(key, key + value);
                        }
                    }
                }, "source")
                .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");
        startStreamsAndCheckDirExists(topology, Collections.singleton(inputTopic), outputTopic, false);
    }

    @Test
    public void inMemoryStatefulTopologyShouldNotCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, false);
        startStreamsAndCheckDirExists(topology, asList(inputTopic, globalTopicName), outputTopic, false);
    }

    @Test
    public void statefulTopologyShouldCreateStateDirectory() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, true);
        startStreamsAndCheckDirExists(topology, asList(inputTopic, globalTopicName), outputTopic, true);
    }

    private KafkaStreams getStatefulApp() throws Exception {
        final String inputTopic = testName.getMethodName() + "-input";
        final String outputTopic = testName.getMethodName() + "-output";
        final String globalTopicName = testName.getMethodName() + "-global";
        final String storeName = testName.getMethodName() + "-counts";
        final String globalStoreName = testName.getMethodName() + "-globalStore";
        final Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, true);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addGlobalStore(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore("storeName"),
                Serdes.ByteArray(),
                Serdes.ByteArray()
            ),
            "topicName",
            Consumed.as("someName"),
            () -> new Processor() {
                @Override
                public void init(final ProcessorContext context) {}

                @Override
                public void process(final Object key, final Object value) {}

                @Override
                public void close() {}
            });
        return new KafkaStreams(builder.build(), props);
    }

    @SuppressWarnings("unchecked")
    private Topology getStatefulTopology(final String inputTopic,
                                         final String outputTopic,
                                         final String globalTopicName,
                                         final String storeName,
                                         final String globalStoreName,
                                         final boolean isPersistentStore) throws Exception {
        CLUSTER.createTopics(inputTopic, outputTopic, globalTopicName);
        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
            isPersistentStore ?
                Stores.persistentKeyValueStore(storeName)
                : Stores.inMemoryKeyValueStore(storeName),
            Serdes.String(),
            Serdes.Long());
        final Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
                .addProcessor("process", () -> new AbstractProcessor<String, String>() {
                    @Override
                    public void process(final String key, final String value) {
                        final KeyValueStore<String, Long> kvStore =
                                (KeyValueStore<String, Long>) context().getStateStore(storeName);
                        kvStore.put(key, 5L);

                        context().forward(key, "5");
                        context().commit();
                    }
                }, "source")
                .addStateStore(storeBuilder, "process")
                .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");

        final StoreBuilder<KeyValueStore<String, String>> globalStoreBuilder = Stores.keyValueStoreBuilder(
                isPersistentStore ? Stores.persistentKeyValueStore(globalStoreName) : Stores.inMemoryKeyValueStore(globalStoreName),
                Serdes.String(), Serdes.String()).withLoggingDisabled();
        topology.addGlobalStore(globalStoreBuilder,
                "global",
                Serdes.String().deserializer(),
                Serdes.String().deserializer(),
                globalTopicName,
                globalTopicName + "-processor",
                new MockProcessorSupplier());
        return topology;
    }

    private void startStreamsAndCheckDirExists(final Topology topology,
                                               final Collection<String> inputTopics,
                                               final String outputTopic,
                                               final boolean shouldFilesExist) throws Exception {
        final File baseDir = new File(TestUtils.IO_TMP_DIR + File.separator + "kafka-" + TestUtils.randomString(5));
        final Path basePath = baseDir.toPath();
        if (!baseDir.exists()) {
            Files.createDirectory(basePath);
        }
        // changing the path of state directory to make sure that it should not clash with other test cases.
        final Properties localProps = new Properties();
        localProps.putAll(props);
        localProps.put(StreamsConfig.STATE_DIR_CONFIG, baseDir.getAbsolutePath());

        final KafkaStreams streams = new KafkaStreams(topology, localProps);
        streams.start();

        for (final String topic : inputTopics) {
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic,
                    Collections.singletonList(new KeyValue<>("A", "A")),
                    TestUtils.producerConfig(
                            CLUSTER.bootstrapServers(),
                            StringSerializer.class,
                            StringSerializer.class,
                            new Properties()),
                    System.currentTimeMillis());
        }

        IntegrationTestUtils.readKeyValues(outputTopic,
                TestUtils.consumerConfig(
                        CLUSTER.bootstrapServers(),
                        outputTopic + "-group",
                        StringDeserializer.class,
                        StringDeserializer.class),
                5000, 1);

        try {
            final List<Path> files = Files.find(basePath, 999, (p, bfa) -> !p.equals(basePath)).collect(Collectors.toList());
            if (shouldFilesExist && files.isEmpty()) {
                fail("Files should have existed, but it didn't: " + files);
            }
            if (!shouldFilesExist && !files.isEmpty()) {
                fail("Files should not have existed, but it did: " + files);
            }
        } catch (final IOException e) {
            fail("Couldn't read the state directory : " + baseDir.getPath());
        } finally {
            streams.close();
            streams.cleanUp();
            Utils.delete(baseDir);
        }
    }

    private void verifyCleanupStateDir(final String appDir,
                                       final File oldTaskDir) throws InterruptedException {
        final File taskDir = new File(appDir, "0_0");
        TestUtils.waitForCondition(
            () -> !oldTaskDir.exists() && taskDir.exists(),
            "cleanup has not successfully run");
        assertTrue(taskDir.exists());
    }

    public static class StateListenerStub implements KafkaStreams.StateListener {
        int numChanges = 0;
        KafkaStreams.State oldState;
        KafkaStreams.State newState;
        public Map<KafkaStreams.State, Long> mapStates = new HashMap<>();

        @Override
        public void onChange(final KafkaStreams.State newState,
                             final KafkaStreams.State oldState) {
            final long prevCount = mapStates.containsKey(newState) ? mapStates.get(newState) : 0;
            numChanges++;
            this.oldState = oldState;
            this.newState = newState;
            mapStates.put(newState, prevCount + 1);
        }
    }

    private static class LockThread extends Thread {
        private final File lockFile;
        volatile boolean isRunning = true;
        volatile boolean releaseLock = false;
        volatile boolean lockAcquired = false;
        volatile Exception error = null;

        LockThread(final File lockFile) {
            this.lockFile = lockFile;
        }

        @Override
        public void run() {
            try {
                try (final FileChannel channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    channel.tryLock();
                    lockAcquired = true;
                    while (!releaseLock) {
                        Utils.sleep(100);
                    }
                }
                lockAcquired = false;
            } catch (final Exception e) {
                error = e;
            }
            while (isRunning) {
                Utils.sleep(100);
            }
        }
    }

}
