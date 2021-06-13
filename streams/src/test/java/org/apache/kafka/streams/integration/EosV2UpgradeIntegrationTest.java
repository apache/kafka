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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils.StableAssignmentListener;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class EosV2UpgradeIntegrationTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
            {false},
            {true}
        });
    }

    @Parameterized.Parameter
    public boolean injectError;

    private static final int NUM_BROKERS = 3;
    private static final int MAX_POLL_INTERVAL_MS = (int) Duration.ofSeconds(100L).toMillis();
    private static final long MAX_WAIT_TIME_MS = Duration.ofMinutes(1L).toMillis();

    private static final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> CLOSE =
        Collections.unmodifiableList(
            Arrays.asList(
                KeyValue.pair(KafkaStreams.State.RUNNING, KafkaStreams.State.PENDING_SHUTDOWN),
                KeyValue.pair(KafkaStreams.State.PENDING_SHUTDOWN, KafkaStreams.State.NOT_RUNNING)
            )
        );
    private static final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> CRASH =
        Collections.unmodifiableList(
            Collections.singletonList(
                KeyValue.pair(State.PENDING_ERROR, State.ERROR)
            )
        );

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "false"))
    );


    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static String applicationId;
    private final static int NUM_TOPIC_PARTITIONS = 4;
    private final static String CONSUMER_GROUP_ID = "readCommitted";
    private final static String MULTI_PARTITION_INPUT_TOPIC = "multiPartitionInputTopic";
    private final static String MULTI_PARTITION_OUTPUT_TOPIC = "multiPartitionOutputTopic";
    private final static String APP_DIR_1 = "appDir1";
    private final static String APP_DIR_2 = "appDir2";
    private final static String UNEXPECTED_EXCEPTION_MSG = "Fail the test since we got an unexpected exception, or " +
        "there are too many exceptions thrown, please check standard error log for more info.";
    private final String storeName = "store";

    private final StableAssignmentListener assignmentListener = new StableAssignmentListener();

    private final AtomicBoolean errorInjectedClient1 = new AtomicBoolean(false);
    private final AtomicBoolean errorInjectedClient2 = new AtomicBoolean(false);
    private final AtomicBoolean commitErrorInjectedClient1 = new AtomicBoolean(false);
    private final AtomicBoolean commitErrorInjectedClient2 = new AtomicBoolean(false);
    private final AtomicInteger commitCounterClient1 = new AtomicInteger(-1);
    private final AtomicInteger commitCounterClient2 = new AtomicInteger(-1);
    private final AtomicInteger commitRequested = new AtomicInteger(0);

    private int testNumber = 0;
    private Map<String, Integer> exceptionCounts = new HashMap<String, Integer>() {
        {
            put(APP_DIR_1, 0);
            put(APP_DIR_2, 0);
        }
    };

    private volatile boolean hasUnexpectedError = false;

    @Before
    public void createTopics() throws Exception {
        applicationId = "appId-" + ++testNumber;
        CLUSTER.deleteTopicsAndWait(
            MULTI_PARTITION_INPUT_TOPIC,
            MULTI_PARTITION_OUTPUT_TOPIC,
            applicationId + "-" + storeName + "-changelog"
        );

        CLUSTER.createTopic(MULTI_PARTITION_INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
        CLUSTER.createTopic(MULTI_PARTITION_OUTPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUpgradeFromEosAlphaToEosV2() throws Exception {
        // We use two KafkaStreams clients that we upgrade from eos-alpha to eos-V2. During the upgrade,
        // we ensure that there are pending transaction and verify that data is processed correctly.
        //
        // We either close clients cleanly (`injectError = false`) or let them crash (`injectError = true`) during
        // the upgrade. For both cases, EOS should not be violated.
        //
        // Additionally, we inject errors while one client is on eos-alpha while the other client is on eos-V2:
        // For this case, we inject the error during task commit phase, i.e., after offsets are appended to a TX,
        // and before the TX is committed. The goal is to verify that the written but uncommitted offsets are not
        // picked up, i.e., GroupCoordinator fencing works correctly.
        //
        // The commit interval is set to MAX_VALUE and the used `Processor` request commits manually so we have full
        // control when a commit actually happens. We use an input topic with 4 partitions and each task will request
        // a commit after processing 10 records.
        //
        // 1.  start both clients and wait until rebalance stabilizes
        // 2.  write 10 records per input topic partition and verify that the result was committed
        // 3.  write 5 records per input topic partition to get pending transactions (verified via "read_uncommitted" mode)
        //      - all 4 pending transactions are based on task producers
        //      - we will get only 4 pending writes for one partition for the crash case as we crash processing the 5th record
        // 4.  stop/crash the first client, wait until rebalance stabilizes:
        //      - stop case:
        //        * verify that the stopped client did commit its pending transaction during shutdown
        //        * the second client will still have two pending transaction
        //      - crash case:
        //        * the pending transactions of the crashed client got aborted
        //        * the second client will have four pending transactions
        // 5.  restart the first client with eos-V2 enabled and wait until rebalance stabilizes
        //       - the rebalance should result in a commit of all tasks
        // 6.  write 5 record per input topic partition
        //       - stop case:
        //         * verify that the result was committed
        //       - crash case:
        //         * fail the second (i.e., eos-alpha) client during commit
        //         * the eos-V2 client should not pickup the pending offsets
        //         * verify uncommitted and committed result
        // 7.  only for crash case:
        //     7a. restart the second client in eos-alpha mode and wait until rebalance stabilizes
        //     7b. write 10 records per input topic partition
        //         * fail the first (i.e., eos-V2) client during commit
        //         * the eos-alpha client should not pickup the pending offsets
        //         * verify uncommitted and committed result
        //     7c. restart the first client in eos-V2 mode and wait until rebalance stabilizes
        // 8.  write 5 records per input topic partition to get pending transactions (verified via "read_uncommitted" mode)
        //      - 2 transaction are base on a task producer; one transaction is based on a thread producer
        //      - we will get 4 pending writes for the crash case as we crash processing the 5th record
        // 9.  stop/crash the second client and wait until rebalance stabilizes:
        //      - stop only:
        //        * verify that the stopped client did commit its pending transaction during shutdown
        //        * the first client will still have one pending transaction
        //      - crash case:
        //        * the pending transactions of the crashed client got aborted
        //        * the first client will have one pending transactions
        // 10. restart the second client with eos-V2 enabled and wait until rebalance stabilizes
        //       - the rebalance should result in a commit of all tasks
        // 11. write 5 record per input topic partition and verify that the result was committed

        final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> stateTransitions1 = new LinkedList<>();
        KafkaStreams streams1Alpha = null;
        KafkaStreams streams1V2 = null;
        KafkaStreams streams1V2Two = null;

        final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> stateTransitions2 = new LinkedList<>();
        KafkaStreams streams2Alpha = null;
        KafkaStreams streams2AlphaTwo = null;
        KafkaStreams streams2V2 = null;

        try {
            // phase 1: start both clients
            streams1Alpha = getKafkaStreams(APP_DIR_1, StreamsConfig.EXACTLY_ONCE);
            streams1Alpha.setStateListener(
                (newState, oldState) -> stateTransitions1.add(KeyValue.pair(oldState, newState))
            );

            assignmentListener.prepareForRebalance();
            streams1Alpha.cleanUp();
            streams1Alpha.start();
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);

            streams2Alpha = getKafkaStreams(APP_DIR_2, StreamsConfig.EXACTLY_ONCE);
            streams2Alpha.setStateListener(
                (newState, oldState) -> stateTransitions2.add(KeyValue.pair(oldState, newState))
            );
            stateTransitions1.clear();

            assignmentListener.prepareForRebalance();
            streams2Alpha.cleanUp();
            streams2Alpha.start();
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);
            waitForRunning(stateTransitions2);

            // in all phases, we write comments that assume that p-0/p-1 are assigned to the first client
            // and p-2/p-3 are assigned to the second client (in reality the assignment might be different though)

            // phase 2: (write first batch of data)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C
            // p-1: ---> 10 rec + C
            // p-2: ---> 10 rec + C
            // p-3: ---> 10 rec + C
            final List<KeyValue<Long, Long>> committedInputDataBeforeUpgrade =
                prepareData(0L, 10L, 0L, 1L, 2L, 3L);
            writeInputData(committedInputDataBeforeUpgrade);

            waitForCondition(
                () -> commitRequested.get() == 4,
                MAX_WAIT_TIME_MS,
                "SteamsTasks did not request commit."
            );

            final Map<Long, Long> committedState = new HashMap<>();
            final List<KeyValue<Long, Long>> expectedUncommittedResult =
                computeExpectedResult(committedInputDataBeforeUpgrade, committedState);
            verifyCommitted(expectedUncommittedResult);

            // phase 3: (write partial second batch of data)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C ---> 5 rec (pending)
            //   p-1: 10 rec + C ---> 5 rec (pending)
            //   p-2: 10 rec + C ---> 5 rec (pending)
            //   p-3: 10 rec + C ---> 5 rec (pending)
            // crash case: (we just assumes that we inject the error for p-0; in reality it might be a different partition)
            //             (we don't crash right away and write one record less)
            //   p-0: 10 rec + C ---> 4 rec (pending)
            //   p-1: 10 rec + C ---> 5 rec (pending)
            //   p-2: 10 rec + C ---> 5 rec (pending)
            //   p-3: 10 rec + C ---> 5 rec (pending)
            final Set<Long> cleanKeys = mkSet(0L, 1L, 2L, 3L);
            final Set<Long> keysFirstClientAlpha = keysFromInstance(streams1Alpha);
            final long firstFailingKeyForCrashCase = keysFirstClientAlpha.iterator().next();
            cleanKeys.remove(firstFailingKeyForCrashCase);

            final List<KeyValue<Long, Long>> uncommittedInputDataBeforeFirstUpgrade = new LinkedList<>();
            final HashMap<Long, Long> uncommittedState = new HashMap<>(committedState);
            if (!injectError) {
                uncommittedInputDataBeforeFirstUpgrade.addAll(
                    prepareData(10L, 15L, 0L, 1L, 2L, 3L)
                );
                writeInputData(uncommittedInputDataBeforeFirstUpgrade);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataBeforeFirstUpgrade, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);
            } else {
                final List<KeyValue<Long, Long>> uncommittedInputDataWithoutFailingKey = new LinkedList<>();
                for (final long key : cleanKeys) {
                    uncommittedInputDataWithoutFailingKey.addAll(prepareData(10L, 15L, key));
                }
                uncommittedInputDataWithoutFailingKey.addAll(
                    prepareData(10L, 14L, firstFailingKeyForCrashCase)
                );
                uncommittedInputDataBeforeFirstUpgrade.addAll(uncommittedInputDataWithoutFailingKey);
                writeInputData(uncommittedInputDataWithoutFailingKey);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataWithoutFailingKey, new HashMap<>(committedState))
                );
                verifyUncommitted(expectedUncommittedResult);
            }

            // phase 4: (stop first client)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case: (client 1 will commit its two tasks on close())
            //   p-0: 10 rec + C   +   5 rec ---> C
            //   p-1: 10 rec + C   +   5 rec ---> C
            //   p-2: 10 rec + C   +   5 rec (pending)
            //   p-3: 10 rec + C   +   5 rec (pending)
            // crash case: (we write the last record that will trigger the crash; both TX from client 1 will be aborted
            //              during fail over by client 2 and retried)
            //   p-0: 10 rec + C   +   4 rec + A + 5 rec (pending)
            //   p-1: 10 rec + C   +   5 rec + A + 5 rec (pending)
            //   p-2: 10 rec + C   +   5 rec (pending)
            //   p-3: 10 rec + C   +   5 rec (pending)
            stateTransitions2.clear();
            assignmentListener.prepareForRebalance();

            if (!injectError) {
                stateTransitions1.clear();
                streams1Alpha.close();
                waitForStateTransition(stateTransitions1, CLOSE);
            } else {
                errorInjectedClient1.set(true);

                final List<KeyValue<Long, Long>> dataPotentiallyFirstFailingKey =
                    prepareData(14L, 15L, firstFailingKeyForCrashCase);
                uncommittedInputDataBeforeFirstUpgrade.addAll(dataPotentiallyFirstFailingKey);
                writeInputData(dataPotentiallyFirstFailingKey);
            }
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions2);

            if (!injectError) {
                final List<KeyValue<Long, Long>> committedInputDataDuringFirstUpgrade =
                    uncommittedInputDataBeforeFirstUpgrade
                        .stream()
                        .filter(pair -> keysFirstClientAlpha.contains(pair.key))
                        .collect(Collectors.toList());

                final List<KeyValue<Long, Long>> expectedCommittedResult =
                    computeExpectedResult(committedInputDataDuringFirstUpgrade, committedState);
                verifyCommitted(expectedCommittedResult);
            } else {
                // retrying TX
                expectedUncommittedResult.addAll(computeExpectedResult(
                    uncommittedInputDataBeforeFirstUpgrade
                        .stream()
                        .filter(pair -> keysFirstClientAlpha.contains(pair.key))
                        .collect(Collectors.toList()),
                    new HashMap<>(committedState)
                ));
                verifyUncommitted(expectedUncommittedResult);
                waitForStateTransitionContains(stateTransitions1, CRASH);

                errorInjectedClient1.set(false);
                stateTransitions1.clear();
                streams1Alpha.close();
                assertFalse(UNEXPECTED_EXCEPTION_MSG, hasUnexpectedError);
            }

            // phase 5: (restart first client)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case: (client 2 (alpha) will commit the two revoked task that migrate back to client 1)
            //            (note: we may or may not get newly committed data, depending if the already committed tasks
            //             migrate back to client 1, or different tasks)
            //            (below we show the case for which we don't get newly committed data)
            //   p-0: 10 rec + C   +   5 rec ---> C
            //   p-1: 10 rec + C   +   5 rec ---> C
            //   p-2: 10 rec + C   +   5 rec (pending)
            //   p-3: 10 rec + C   +   5 rec (pending)
            // crash case: (client 2 (alpha) will commit all tasks even only two tasks are revoked and migrate back to client 1)
            //             (note: because nothing was committed originally, we always get newly committed data)
            //   p-0: 10 rec + C   +   4 rec + A + 5 rec ---> C
            //   p-1: 10 rec + C   +   5 rec + A + 5 rec ---> C
            //   p-2: 10 rec + C   +   5 rec ---> C
            //   p-3: 10 rec + C   +   5 rec ---> C
            commitRequested.set(0);
            stateTransitions1.clear();
            stateTransitions2.clear();
            streams1V2 = getKafkaStreams(APP_DIR_1, StreamsConfig.EXACTLY_ONCE_V2);
            streams1V2.setStateListener((newState, oldState) -> stateTransitions1.add(KeyValue.pair(oldState, newState)));
            assignmentListener.prepareForRebalance();
            streams1V2.start();
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);
            waitForRunning(stateTransitions2);

            final Set<Long> newlyCommittedKeys;
            if (!injectError) {
                newlyCommittedKeys = keysFromInstance(streams1V2);
                newlyCommittedKeys.removeAll(keysFirstClientAlpha);
            } else {
                newlyCommittedKeys = mkSet(0L, 1L, 2L, 3L);
            }

            final List<KeyValue<Long, Long>> expectedCommittedResultAfterRestartFirstClient = computeExpectedResult(
                uncommittedInputDataBeforeFirstUpgrade
                    .stream()
                    .filter(pair -> newlyCommittedKeys.contains(pair.key))
                    .collect(Collectors.toList()),
                committedState
            );
            verifyCommitted(expectedCommittedResultAfterRestartFirstClient);

            // phase 6: (complete second batch of data; crash: let second client fail on commit)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case: (both client commit regularly)
            //            (depending on the task movement in phase 5, we may or may not get newly committed data;
            //             we show the case for which p-2 and p-3 are newly committed below)
            //   p-0: 10 rec + C   +   5 rec + C ---> 5 rec + C
            //   p-1: 10 rec + C   +   5 rec + C ---> 5 rec + C
            //   p-2: 10 rec + C   +   5 rec     ---> 5 rec + C
            //   p-3: 10 rec + C   +   5 rec     ---> 5 rec + C
            // crash case: (second/alpha client fails and both TX are aborted)
            //             (first/V2 client reprocessed the 10 records and commits TX)
            //   p-0: 10 rec + C   +   4 rec + A + 5 rec + C ---> 5 rec + C
            //   p-1: 10 rec + C   +   5 rec + A + 5 rec + C ---> 5 rec + C
            //   p-2: 10 rec + C   +   5 rec + C             ---> 5 rec + A + 5 rec + C
            //   p-3: 10 rec + C   +   5 rec + C             ---> 5 rec + A + 5 rec + C
            commitCounterClient1.set(0);

            if (!injectError) {
                final List<KeyValue<Long, Long>> finishSecondBatch = prepareData(15L, 20L, 0L, 1L, 2L, 3L);
                writeInputData(finishSecondBatch);

                final List<KeyValue<Long, Long>> committedInputDataDuringUpgrade = uncommittedInputDataBeforeFirstUpgrade
                    .stream()
                    .filter(pair -> !keysFirstClientAlpha.contains(pair.key))
                    .filter(pair -> !newlyCommittedKeys.contains(pair.key))
                    .collect(Collectors.toList());
                committedInputDataDuringUpgrade.addAll(
                    finishSecondBatch
                );

                expectedUncommittedResult.addAll(
                    computeExpectedResult(finishSecondBatch, uncommittedState)
                );
                final List<KeyValue<Long, Long>> expectedCommittedResult =
                    computeExpectedResult(committedInputDataDuringUpgrade, committedState);
                verifyCommitted(expectedCommittedResult);
            } else {
                final Set<Long> keysFirstClientV2 = keysFromInstance(streams1V2);
                final Set<Long> keysSecondClientAlpha = keysFromInstance(streams2Alpha);

                final List<KeyValue<Long, Long>> committedInputDataAfterFirstUpgrade =
                    prepareData(15L, 20L, keysFirstClientV2.toArray(new Long[0]));
                writeInputData(committedInputDataAfterFirstUpgrade);

                final List<KeyValue<Long, Long>> expectedCommittedResultBeforeFailure =
                    computeExpectedResult(committedInputDataAfterFirstUpgrade, committedState);
                verifyCommitted(expectedCommittedResultBeforeFailure);
                expectedUncommittedResult.addAll(expectedCommittedResultBeforeFailure);

                commitCounterClient2.set(0);

                final Iterator<Long> it = keysSecondClientAlpha.iterator();
                final Long otherKey = it.next();
                final Long failingKey = it.next();

                final List<KeyValue<Long, Long>> uncommittedInputDataAfterFirstUpgrade =
                    prepareData(15L, 19L, keysSecondClientAlpha.toArray(new Long[0]));
                uncommittedInputDataAfterFirstUpgrade.addAll(prepareData(19L, 20L, otherKey));
                writeInputData(uncommittedInputDataAfterFirstUpgrade);

                uncommittedState.putAll(committedState);
                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataAfterFirstUpgrade, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);

                stateTransitions1.clear();
                stateTransitions2.clear();
                assignmentListener.prepareForRebalance();

                commitCounterClient1.set(0);
                commitErrorInjectedClient2.set(true);

                final List<KeyValue<Long, Long>> dataFailingKey = prepareData(19L, 20L, failingKey);
                uncommittedInputDataAfterFirstUpgrade.addAll(dataFailingKey);
                writeInputData(dataFailingKey);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(dataFailingKey, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);

                assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);

                waitForStateTransitionContains(stateTransitions2, CRASH);

                commitErrorInjectedClient2.set(false);
                stateTransitions2.clear();
                streams2Alpha.close();
                assertFalse(UNEXPECTED_EXCEPTION_MSG, hasUnexpectedError);

                final List<KeyValue<Long, Long>> expectedCommittedResultAfterFailure =
                    computeExpectedResult(uncommittedInputDataAfterFirstUpgrade, committedState);
                verifyCommitted(expectedCommittedResultAfterFailure);
                expectedUncommittedResult.addAll(expectedCommittedResultAfterFailure);
            }

            // 7. only for crash case:
            //     7a. restart the failed second client in eos-alpha mode and wait until rebalance stabilizes
            //     7b. write third batch of input data
            //         * fail the first (i.e., eos-V2) client during commit
            //         * the eos-alpha client should not pickup the pending offsets
            //         * verify uncommitted and committed result
            //     7c. restart the first client in eos-V2 mode and wait until rebalance stabilizes
            //
            // crash case:
            //   p-0: 10 rec + C   +   4 rec + A + 5 rec + C + 5 rec + C ---> 10 rec + A + 10 rec + C
            //   p-1: 10 rec + C   +   5 rec + A + 5 rec + C + 5 rec + C ---> 10 rec + A + 10 rec + C
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C ---> 10 rec + C
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C ---> 10 rec + C
            if (!injectError) {
                streams2AlphaTwo = streams2Alpha;
            } else {
                // 7a restart the second client in eos-alpha mode and wait until rebalance stabilizes
                commitCounterClient1.set(0);
                commitCounterClient2.set(-1);
                stateTransitions1.clear();
                stateTransitions2.clear();
                streams2AlphaTwo = getKafkaStreams(APP_DIR_2, StreamsConfig.EXACTLY_ONCE);
                streams2AlphaTwo.setStateListener(
                    (newState, oldState) -> stateTransitions2.add(KeyValue.pair(oldState, newState))
                );
                assignmentListener.prepareForRebalance();
                streams2AlphaTwo.start();
                assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
                waitForRunning(stateTransitions1);
                waitForRunning(stateTransitions2);

                // 7b. write third batch of input data
                final Set<Long> keysFirstClientV2 = keysFromInstance(streams1V2);
                final Set<Long> keysSecondClientAlphaTwo = keysFromInstance(streams2AlphaTwo);

                final List<KeyValue<Long, Long>> committedInputDataBetweenUpgrades =
                    prepareData(20L, 30L, keysSecondClientAlphaTwo.toArray(new Long[0]));
                writeInputData(committedInputDataBetweenUpgrades);

                final List<KeyValue<Long, Long>> expectedCommittedResultBeforeFailure =
                    computeExpectedResult(committedInputDataBetweenUpgrades, committedState);
                verifyCommitted(expectedCommittedResultBeforeFailure);
                expectedUncommittedResult.addAll(expectedCommittedResultBeforeFailure);

                commitCounterClient2.set(0);

                final Iterator<Long> it = keysFirstClientV2.iterator();
                final Long otherKey = it.next();
                final Long failingKey = it.next();

                final List<KeyValue<Long, Long>> uncommittedInputDataBetweenUpgrade =
                    prepareData(20L, 29L, keysFirstClientV2.toArray(new Long[0]));
                uncommittedInputDataBetweenUpgrade.addAll(prepareData(29L, 30L, otherKey));
                writeInputData(uncommittedInputDataBetweenUpgrade);

                uncommittedState.putAll(committedState);
                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataBetweenUpgrade, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);

                stateTransitions1.clear();
                stateTransitions2.clear();
                assignmentListener.prepareForRebalance();
                commitCounterClient2.set(0);
                commitErrorInjectedClient1.set(true);

                final List<KeyValue<Long, Long>> dataFailingKey = prepareData(29L, 30L, failingKey);
                uncommittedInputDataBetweenUpgrade.addAll(dataFailingKey);
                writeInputData(dataFailingKey);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(dataFailingKey, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);

                assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);

                waitForStateTransitionContains(stateTransitions1, CRASH);

                commitErrorInjectedClient1.set(false);
                stateTransitions1.clear();
                streams1V2.close();
                assertFalse(UNEXPECTED_EXCEPTION_MSG, hasUnexpectedError);

                final List<KeyValue<Long, Long>> expectedCommittedResultAfterFailure =
                    computeExpectedResult(uncommittedInputDataBetweenUpgrade, committedState);
                verifyCommitted(expectedCommittedResultAfterFailure);
                expectedUncommittedResult.addAll(expectedCommittedResultAfterFailure);

                // 7c. restart the first client in eos-V2 mode and wait until rebalance stabilizes
                stateTransitions1.clear();
                stateTransitions2.clear();
                streams1V2Two = getKafkaStreams(APP_DIR_1, StreamsConfig.EXACTLY_ONCE_V2);
                streams1V2Two.setStateListener((newState, oldState) -> stateTransitions1.add(KeyValue.pair(oldState, newState)));
                assignmentListener.prepareForRebalance();
                streams1V2Two.start();
                assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
                waitForRunning(stateTransitions1);
                waitForRunning(stateTransitions2);
            }

            // phase 8: (write partial last batch of data)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C   +   5 rec + C + 5 rec + C ---> 5 rec (pending)
            //   p-1: 10 rec + C   +   5 rec + C + 5 rec + C ---> 5 rec (pending)
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + C ---> 5 rec (pending)
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + C ---> 5 rec (pending)
            // crash case: (we just assumes that we inject the error for p-2; in reality it might be a different partition)
            //             (we don't crash right away and write one record less)
            //   p-0: 10 rec + C   +   4 rec + A + 5 rec + C + 5 rec + C   +   10 rec + A + 10 rec + C ---> 5 rec (pending)
            //   p-1: 10 rec + C   +   5 rec + A + 5 rec + C + 5 rec + C   +   10 rec + A + 10 rec + C ---> 5 rec (pending)
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C   +   10 rec + C              ---> 4 rec (pending)
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C   +   10 rec + C              ---> 5 rec (pending)
            cleanKeys.addAll(mkSet(0L, 1L, 2L, 3L));
            final Set<Long> keysSecondClientAlphaTwo = keysFromInstance(streams2AlphaTwo);
            final long secondFailingKeyForCrashCase = keysSecondClientAlphaTwo.iterator().next();
            cleanKeys.remove(secondFailingKeyForCrashCase);

            final List<KeyValue<Long, Long>> uncommittedInputDataBeforeSecondUpgrade = new LinkedList<>();
            if (!injectError) {
                uncommittedInputDataBeforeSecondUpgrade.addAll(
                    prepareData(30L, 35L, 0L, 1L, 2L, 3L)
                );
                writeInputData(uncommittedInputDataBeforeSecondUpgrade);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataBeforeSecondUpgrade, new HashMap<>(committedState))
                );
                verifyUncommitted(expectedUncommittedResult);
            } else {
                final List<KeyValue<Long, Long>> uncommittedInputDataWithoutFailingKey = new LinkedList<>();
                for (final long key : cleanKeys) {
                    uncommittedInputDataWithoutFailingKey.addAll(prepareData(30L, 35L, key));
                }
                uncommittedInputDataWithoutFailingKey.addAll(
                    prepareData(30L, 34L, secondFailingKeyForCrashCase)
                );
                uncommittedInputDataBeforeSecondUpgrade.addAll(uncommittedInputDataWithoutFailingKey);
                writeInputData(uncommittedInputDataWithoutFailingKey);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataWithoutFailingKey, new HashMap<>(committedState))
                );
                verifyUncommitted(expectedUncommittedResult);
            }

            // phase 9: (stop/crash second client)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case: (client 2 (alpha) will commit its two tasks on close())
            //   p-0: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec (pending)
            //   p-1: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec (pending)
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec ---> C
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec ---> C
            // crash case: (we write the last record that will trigger the crash; both TX from client 2 will be aborted
            //              during fail over by client 1 and retried)
            //   p-0: 10 rec + C   +   4 rec + A + 5 rec + C + 5 rec + C   +   10 rec + A + 10 rec + C   +   5 rec (pending)
            //   p-1: 10 rec + C   +   5 rec + A + 5 rec + C + 5 rec + C   +   10 rec + A + 10 rec + C   +   5 rec (pending)
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C   +   10 rec + C                +   4 rec ---> A + 5 rec (pending)
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C   +   10 rec + C                +   5 rec ---> A + 5 rec (pending)
            stateTransitions1.clear();
            assignmentListener.prepareForRebalance();
            if (!injectError) {
                stateTransitions2.clear();
                streams2AlphaTwo.close();
                waitForStateTransition(stateTransitions2, CLOSE);
            } else {
                errorInjectedClient2.set(true);

                final List<KeyValue<Long, Long>> dataPotentiallySecondFailingKey =
                    prepareData(34L, 35L, secondFailingKeyForCrashCase);
                uncommittedInputDataBeforeSecondUpgrade.addAll(dataPotentiallySecondFailingKey);
                writeInputData(dataPotentiallySecondFailingKey);
            }
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);

            if (!injectError) {
                final List<KeyValue<Long, Long>> committedInputDataDuringSecondUpgrade =
                    uncommittedInputDataBeforeSecondUpgrade
                        .stream()
                        .filter(pair -> keysSecondClientAlphaTwo.contains(pair.key))
                        .collect(Collectors.toList());

                final List<KeyValue<Long, Long>> expectedCommittedResult =
                    computeExpectedResult(committedInputDataDuringSecondUpgrade, committedState);
                verifyCommitted(expectedCommittedResult);
            } else {
                // retrying TX
                expectedUncommittedResult.addAll(computeExpectedResult(
                    uncommittedInputDataBeforeSecondUpgrade
                        .stream()
                        .filter(pair -> keysSecondClientAlphaTwo.contains(pair.key))
                        .collect(Collectors.toList()),
                    new HashMap<>(committedState)
                ));
                verifyUncommitted(expectedUncommittedResult);
                waitForStateTransitionContains(stateTransitions2, CRASH);

                errorInjectedClient2.set(false);
                stateTransitions2.clear();
                streams2AlphaTwo.close();
                assertFalse(UNEXPECTED_EXCEPTION_MSG, hasUnexpectedError);
            }

            // phase 10: (restart second client)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // the state below indicate the case for which the "original" tasks of client2 are migrated back to client2
            // if a task "switch" happens, we might get additional commits (omitted in the comment for brevity)
            //
            // stop case: (client 1 (V2) will commit all four tasks if at least one revoked and migrate task needs committing back to client 2)
            //   p-0: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec ---> C
            //   p-1: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec ---> C
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec + C
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec + C
            // crash case: (client 1 (V2) will commit all four tasks even only two are migrate back to client 2)
            //   p-0: 10 rec + C   +   4 rec + A + 5 rec + C + 5 rec + C   +   10 rec + A + 10 rec + C   +   5 rec ---> C
            //   p-1: 10 rec + C   +   5 rec + A + 5 rec + C + 5 rec + C   +   10 rec + A + 10 rec + C   +   5 rec ---> C
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C   +   10 rec + C                +   4 rec + A + 5 rec ---> C
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C   +   10 rec + C                +   5 rec + A + 5 rec ---> C
            commitRequested.set(0);
            stateTransitions1.clear();
            stateTransitions2.clear();
            streams2V2 = getKafkaStreams(APP_DIR_1, StreamsConfig.EXACTLY_ONCE_V2);
            streams2V2.setStateListener(
                (newState, oldState) -> stateTransitions2.add(KeyValue.pair(oldState, newState))
            );
            assignmentListener.prepareForRebalance();
            streams2V2.start();
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);
            waitForRunning(stateTransitions2);

            newlyCommittedKeys.clear();
            if (!injectError) {
                newlyCommittedKeys.addAll(keysFromInstance(streams2V2));
                newlyCommittedKeys.removeAll(keysSecondClientAlphaTwo);
            } else {
                newlyCommittedKeys.addAll(mkSet(0L, 1L, 2L, 3L));
            }

            final List<KeyValue<Long, Long>> expectedCommittedResultAfterRestartSecondClient = computeExpectedResult(
                uncommittedInputDataBeforeSecondUpgrade
                    .stream()
                    .filter(pair -> newlyCommittedKeys.contains(pair.key))
                    .collect(Collectors.toList()),
                committedState
            );
            verifyCommitted(expectedCommittedResultAfterRestartSecondClient);

            // phase 11: (complete fourth batch of data)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec + C ---> 5 rec + C
            //   p-1: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec + C ---> 5 rec + C
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec + C ---> 5 rec + C
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + C   +   5 rec + C ---> 5 rec + C
            // crash case:  (we just assumes that we inject the error for p-2; in reality it might be a different partition)
            //   p-0: 10 rec + C   +   4 rec + A + 5 rec + C + 5 rec + C   +   10 rec + A + 10 rec + C   +   5 rec + C             ---> 5 rec + C
            //   p-1: 10 rec + C   +   5 rec + A + 5 rec + C + 5 rec + C   +   10 rec + A + 10 rec + C   +   5 rec + C             ---> 5 rec + C
            //   p-2: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C   +   10 rec + C                +   4 rec + A + 5 rec + C ---> 5 rec + C
            //   p-3: 10 rec + C   +   5 rec + C + 5 rec + A + 5 rec + C   +   10 rec + C                +   5 rec + A + 5 rec + C ---> 5 rec + C
            commitCounterClient1.set(-1);
            commitCounterClient2.set(-1);

            final List<KeyValue<Long, Long>> finishLastBatch =
                prepareData(35L, 40L, 0L, 1L, 2L, 3L);
            writeInputData(finishLastBatch);

            final Set<Long> uncommittedKeys = mkSet(0L, 1L, 2L, 3L);
            uncommittedKeys.removeAll(keysSecondClientAlphaTwo);
            uncommittedKeys.removeAll(newlyCommittedKeys);
            final List<KeyValue<Long, Long>> committedInputDataDuringUpgrade = uncommittedInputDataBeforeSecondUpgrade
                .stream()
                .filter(pair -> uncommittedKeys.contains(pair.key))
                .collect(Collectors.toList());
            committedInputDataDuringUpgrade.addAll(
                finishLastBatch
            );

            final List<KeyValue<Long, Long>> expectedCommittedResult =
                computeExpectedResult(committedInputDataDuringUpgrade, committedState);
            verifyCommitted(expectedCommittedResult);
        } finally {
            if (streams1Alpha != null) {
                streams1Alpha.close();
            }
            if (streams1V2 != null) {
                streams1V2.close();
            }
            if (streams1V2Two != null) {
                streams1V2Two.close();
            }
            if (streams2Alpha != null) {
                streams2Alpha.close();
            }
            if (streams2AlphaTwo != null) {
                streams2AlphaTwo.close();
            }
            if (streams2V2 != null) {
                streams2V2.close();
            }
        }
    }

    private KafkaStreams getKafkaStreams(final String appDir,
                                         final String processingGuarantee) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String[] storeNames = new String[] {storeName};
        final StoreBuilder<KeyValueStore<Long, Long>> storeBuilder = Stores
            .keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long())
            .withCachingEnabled();

        builder.addStateStore(storeBuilder);

        final KStream<Long, Long> input = builder.stream(MULTI_PARTITION_INPUT_TOPIC);
        input.transform(new TransformerSupplier<Long, Long, KeyValue<Long, Long>>() {
            @Override
            public Transformer<Long, Long, KeyValue<Long, Long>> get() {
                return new Transformer<Long, Long, KeyValue<Long, Long>>() {
                    ProcessorContext context;
                    KeyValueStore<Long, Long> state = null;
                    AtomicBoolean crash;
                    AtomicInteger sharedCommit;

                    @Override
                    public void init(final ProcessorContext context) {
                        this.context = context;
                        state = context.getStateStore(storeName);
                        final String clientId = context.appConfigs().get(StreamsConfig.CLIENT_ID_CONFIG).toString();
                        if (APP_DIR_1.equals(clientId)) {
                            crash = errorInjectedClient1;
                            sharedCommit = commitCounterClient1;
                        } else {
                            crash = errorInjectedClient2;
                            sharedCommit = commitCounterClient2;
                        }
                    }

                    @Override
                    public KeyValue<Long, Long> transform(final Long key, final Long value) {
                        if ((value + 1) % 10 == 0) {
                            if (sharedCommit.get() < 0 ||
                                sharedCommit.incrementAndGet() == 2) {

                                context.commit();
                            }
                            commitRequested.incrementAndGet();
                        }

                        Long sum = state.get(key);
                        if (sum == null) {
                            sum = value;
                        } else {
                            sum += value;
                        }
                        state.put(key, sum);
                        state.flush();

                        if (value % 10 == 4 && // potentially crash when processing 5th, 15th, or 25th record (etc.)
                            crash != null && crash.compareAndSet(true, false)) {
                            // only crash a single task
                            throw new RuntimeException("Injected test exception.");
                        }

                        return new KeyValue<>(key, state.get(key));
                    }

                    @Override
                    public void close() {}
                };
            } }, storeNames)
            .to(MULTI_PARTITION_OUTPUT_TOPIC);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, appDir);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        final long commitInterval = Duration.ofMinutes(1L).toMillis();
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitInterval);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), Duration.ofSeconds(1L).toMillis());
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), (int) Duration.ofSeconds(5L).toMillis());
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), (int) Duration.ofSeconds(5L).minusMillis(1L).toMillis());
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_INTERVAL_MS);
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), (int) commitInterval);
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.PARTITIONER_CLASS_CONFIG), KeyPartitioner.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath() + File.separator + appDir);
        properties.put(InternalConfig.ASSIGNMENT_LISTENER, assignmentListener);

        final Properties config = StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
            Serdes.LongSerde.class.getName(),
            Serdes.LongSerde.class.getName(),
            properties
        );

        final KafkaStreams streams = new KafkaStreams(builder.build(), config, new TestKafkaClientSupplier());
        streams.setUncaughtExceptionHandler(e -> {
            if (!injectError) {
                // we don't expect any exception thrown in stop case
                e.printStackTrace(System.err);
                hasUnexpectedError = true;
            } else {
                int exceptionCount = (int) exceptionCounts.get(appDir);
                // should only have our injected exception or commit exception, and 2 exceptions for each stream
                if (++exceptionCount > 2 || !(e instanceof RuntimeException) ||
                    !(e.getMessage().contains("test exception"))) {
                    // The exception won't cause the test fail since we actually "expected" exception thrown and failed the stream.
                    // So, log to stderr for debugging when the exception is not what we expected, and fail in the main thread
                    e.printStackTrace(System.err);
                    hasUnexpectedError = true;
                }
                exceptionCounts.put(appDir, exceptionCount);
            }
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        return streams;
    }

    private void waitForRunning(final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> observed) throws Exception {
        waitForCondition(
            () -> !observed.isEmpty() && observed.get(observed.size() - 1).value.equals(State.RUNNING),
            MAX_WAIT_TIME_MS,
            () -> "Client did not startup on time. Observers transitions: " + observed
        );
    }

    private void waitForStateTransition(final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> observed,
                                        final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> expected)
            throws Exception {

        waitForCondition(
            () -> observed.equals(expected),
            MAX_WAIT_TIME_MS,
            () -> "Client did not have the expected state transition on time. Observers transitions: " + observed
                    + "Expected transitions: " + expected
        );
    }

    private void waitForStateTransitionContains(final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> observed,
                                                final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> expected)
            throws Exception {

        waitForCondition(
            () -> observed.containsAll(expected),
            MAX_WAIT_TIME_MS,
            () -> "Client did not have the expected state transition on time. Observers transitions: " + observed
                    + "Expected transitions: " + expected
        );
    }

    private List<KeyValue<Long, Long>> prepareData(final long fromInclusive,
                                                   final long toExclusive,
                                                   final Long... keys) {
        final List<KeyValue<Long, Long>> data = new ArrayList<>();

        for (final Long k : keys) {
            for (long v = fromInclusive; v < toExclusive; ++v) {
                data.add(new KeyValue<>(k, v));
            }
        }

        return data;
    }

    private void writeInputData(final List<KeyValue<Long, Long>> records) {
        final Properties config = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            LongSerializer.class,
            LongSerializer.class
        );
        config.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, KeyPartitioner.class.getName());
        IntegrationTestUtils.produceKeyValuesSynchronously(
            MULTI_PARTITION_INPUT_TOPIC,
            records,
            config,
            CLUSTER.time
        );
    }

    private void verifyCommitted(final List<KeyValue<Long, Long>> expectedResult) throws Exception {
        final List<KeyValue<Long, Long>> committedOutput = readResult(expectedResult.size(), true);
        checkResultPerKey(committedOutput, expectedResult);
    }

    private void verifyUncommitted(final List<KeyValue<Long, Long>> expectedResult) throws Exception {
        final List<KeyValue<Long, Long>> uncommittedOutput = readResult(expectedResult.size(), false);
        checkResultPerKey(uncommittedOutput, expectedResult);
    }

    private List<KeyValue<Long, Long>> readResult(final int numberOfRecords,
                                                  final boolean readCommitted) throws Exception {
        if (readCommitted) {
            return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    CONSUMER_GROUP_ID,
                    LongDeserializer.class,
                    LongDeserializer.class,
                    Utils.mkProperties(Collections.singletonMap(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT))
                    )
                ),
                MULTI_PARTITION_OUTPUT_TOPIC,
                numberOfRecords,
                MAX_WAIT_TIME_MS
            );
        }

        // read uncommitted
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(CLUSTER.bootstrapServers(), LongDeserializer.class, LongDeserializer.class),
            MULTI_PARTITION_OUTPUT_TOPIC,
            numberOfRecords
        );
    }

    private void checkResultPerKey(final List<KeyValue<Long, Long>> result,
                                   final List<KeyValue<Long, Long>> expectedResult) {
        final Set<Long> allKeys = new HashSet<>();
        addAllKeys(allKeys, result);
        addAllKeys(allKeys, expectedResult);

        for (final Long key : allKeys) {
            try {
                assertThat(getAllRecordPerKey(key, result), equalTo(getAllRecordPerKey(key, expectedResult)));
            } catch (final AssertionError error) {
                throw new AssertionError(
                    "expected result: " + expectedResult.stream().map(KeyValue::toString).collect(Collectors.joining(", ")) +
                    "\nreceived records: " + result.stream().map(KeyValue::toString).collect(Collectors.joining(", ")),
                    error
                );
            }
        }
    }

    private void addAllKeys(final Set<Long> allKeys, final List<KeyValue<Long, Long>> records) {
        for (final KeyValue<Long, Long> record : records) {
            allKeys.add(record.key);
        }
    }

    private List<KeyValue<Long, Long>> getAllRecordPerKey(final Long key, final List<KeyValue<Long, Long>> records) {
        final List<KeyValue<Long, Long>> recordsPerKey = new ArrayList<>(records.size());

        for (final KeyValue<Long, Long> record : records) {
            if (record.key.equals(key)) {
                recordsPerKey.add(record);
            }
        }

        return recordsPerKey;
    }

    private List<KeyValue<Long, Long>> computeExpectedResult(final List<KeyValue<Long, Long>> input,
                                                             final Map<Long, Long> currentState) {
        final List<KeyValue<Long, Long>> expectedResult = new ArrayList<>(input.size());

        for (final KeyValue<Long, Long> record : input) {
            final long sum = currentState.getOrDefault(record.key, 0L);
            currentState.put(record.key, sum + record.value);
            expectedResult.add(new KeyValue<>(record.key, sum + record.value));
        }

        return expectedResult;
    }

    private Set<Long> keysFromInstance(final KafkaStreams streams) throws Exception {
        final Set<Long> keys = new HashSet<>();
        waitForCondition(
            () -> {
                final ReadOnlyKeyValueStore<Long, Long> store = streams.store(
                    StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())
                );

                keys.clear();
                try (final KeyValueIterator<Long, Long> it = store.all()) {
                    while (it.hasNext()) {
                        final KeyValue<Long, Long> row = it.next();
                        keys.add(row.key);
                    }
                }

                return true;
            },
            MAX_WAIT_TIME_MS,
            "Could not get keys from store: " + storeName
        );

        return keys;
    }

    // must be public to allow KafkaProducer to instantiate it
    public static class KeyPartitioner implements Partitioner {
        private final static LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

        @Override
        public int partition(final String topic,
                             final Object key,
                             final byte[] keyBytes,
                             final Object value,
                             final byte[] valueBytes,
                             final Cluster cluster) {
            return LONG_DESERIALIZER.deserialize(topic, keyBytes).intValue() % NUM_TOPIC_PARTITIONS;
        }

        @Override
        public void close() {}

        @Override
        public void configure(final Map<String, ?> configs) {}
    }

    private class TestKafkaClientSupplier extends DefaultKafkaClientSupplier {
        @Override
        public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
            return new ErrorInjector(config);
        }
    }

    private class ErrorInjector extends KafkaProducer<byte[], byte[]> {
        private final AtomicBoolean crash;

        public ErrorInjector(final Map<String, Object> configs) {
            super(configs, new ByteArraySerializer(), new ByteArraySerializer());
            final String clientId = configs.get(ProducerConfig.CLIENT_ID_CONFIG).toString();
            if (clientId.contains(APP_DIR_1)) {
                crash = commitErrorInjectedClient1;
            } else {
                crash = commitErrorInjectedClient2;
            }
        }

        @Override
        public void commitTransaction() {
            super.flush(); // we flush to ensure that the offsets are written
            if (!crash.compareAndSet(true, false)) {
                super.commitTransaction();
            } else {
                throw new RuntimeException("Injected producer commit test exception.");
            }
        }
    }
}
