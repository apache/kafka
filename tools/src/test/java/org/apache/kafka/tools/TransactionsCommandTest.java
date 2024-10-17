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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionsCommandTest {

    private final ToolsTestUtils.MockExitProcedure exitProcedure = new ToolsTestUtils.MockExitProcedure();
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(outputStream);
    private final MockTime time = new MockTime();
    private final Admin admin = Mockito.mock(Admin.class);

    @BeforeEach
    public void setupExitProcedure() {
        Exit.setExitProcedure(exitProcedure);
    }

    @AfterEach
    public void resetExitProcedure() {
        Exit.resetExitProcedure();
    }

    @Test
    public void testDescribeProducersTopicRequired() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "describe-producers",
            "--partition",
            "0"
        });
    }

    @Test
    public void testDescribeProducersPartitionRequired() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "describe-producers",
            "--topic",
            "foo"
        });
    }

    @Test
    public void testDescribeProducersLeader() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        String[] args = new String[] {
            "--bootstrap-server",
            "localhost:9092",
            "describe-producers",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition())
        };

        testDescribeProducers(topicPartition, args, new DescribeProducersOptions());
    }

    @Test
    public void testDescribeProducersSpecificReplica() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        int brokerId = 5;

        String[] args = new String[] {
            "--bootstrap-server",
            "localhost:9092",
            "describe-producers",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition()),
            "--broker-id",
            String.valueOf(brokerId)
        };

        testDescribeProducers(topicPartition, args, new DescribeProducersOptions().brokerId(brokerId));
    }

    private void testDescribeProducers(
        TopicPartition topicPartition,
        String[] args,
        DescribeProducersOptions expectedOptions
    ) throws Exception {
        DescribeProducersResult describeResult = Mockito.mock(DescribeProducersResult.class);
        KafkaFuture<PartitionProducerState> describeFuture = completedFuture(
            new PartitionProducerState(asList(
                new ProducerState(12345L, 15, 1300, 1599509565L,
                    OptionalInt.of(20), OptionalLong.of(990)),
                new ProducerState(98765L, 30, 2300, 1599509599L,
                    OptionalInt.empty(), OptionalLong.empty())
            )));


        Mockito.when(describeResult.partitionResult(topicPartition)).thenReturn(describeFuture);
        Mockito.when(admin.describeProducers(singleton(topicPartition), expectedOptions)).thenReturn(describeResult);

        execute(args);
        assertNormalExit();

        List<List<String>> table = readOutputAsTable();
        assertEquals(3, table.size());

        List<String> expectedHeaders = TransactionsCommand.DescribeProducersCommand.HEADERS;
        assertEquals(expectedHeaders, table.get(0));

        Set<List<String>> expectedRows = Set.of(
            asList("12345", "15", "20", "1300", "1599509565", "990"),
            asList("98765", "30", "-1", "2300", "1599509599", "None")
        );
        assertEquals(expectedRows, new HashSet<>(table.subList(1, table.size())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testListTransactions(boolean hasDurationFilter) throws Exception {
        String[] args = new String[] {
            "--bootstrap-server",
            "localhost:9092",
            "list"
        };

        if (hasDurationFilter) {
            args = new String[] {
                "--bootstrap-server",
                "localhost:9092",
                "list",
                "--duration-filter",
                Long.toString(Long.MAX_VALUE)
            };
        }

        Map<Integer, Collection<TransactionListing>> transactions = new HashMap<>();
        transactions.put(0, asList(
            new TransactionListing("foo", 12345L, TransactionState.ONGOING),
            new TransactionListing("bar", 98765L, TransactionState.PREPARE_ABORT)
        ));
        transactions.put(1, singletonList(
            new TransactionListing("baz", 13579L, TransactionState.COMPLETE_COMMIT)
        ));

        if (hasDurationFilter) {
            expectListTransactions(new ListTransactionsOptions().filterOnDuration(Long.MAX_VALUE), transactions);
        } else {
            expectListTransactions(transactions);
        }

        execute(args);
        assertNormalExit();

        List<List<String>> table = readOutputAsTable();
        assertEquals(4, table.size());

        // Assert expected headers
        List<String> expectedHeaders = TransactionsCommand.ListTransactionsCommand.HEADERS;
        assertEquals(expectedHeaders, table.get(0));

        Set<List<String>> expectedRows = Set.of(
            asList("foo", "0", "12345", "Ongoing"),
            asList("bar", "0", "98765", "PrepareAbort"),
            asList("baz", "1", "13579", "CompleteCommit")
        );
        assertEquals(expectedRows, new HashSet<>(table.subList(1, table.size())));
    }

    @Test
    public void testDescribeTransactionsTransactionalIdRequired() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "describe"
        });
    }

    @Test
    public void testDescribeTransaction() throws Exception {
        String transactionalId = "foo";
        String[] args = new String[] {
            "--bootstrap-server",
            "localhost:9092",
            "describe",
            "--transactional-id",
            transactionalId
        };

        DescribeTransactionsResult describeResult = Mockito.mock(DescribeTransactionsResult.class);

        int coordinatorId = 5;
        long transactionStartTime = time.milliseconds();

        KafkaFuture<TransactionDescription> describeFuture = completedFuture(
            new TransactionDescription(
                coordinatorId,
                TransactionState.ONGOING,
                12345L,
                15,
                10000,
                OptionalLong.of(transactionStartTime),
                singleton(new TopicPartition("bar", 0))
        ));

        Mockito.when(describeResult.description(transactionalId)).thenReturn(describeFuture);
        Mockito.when(admin.describeTransactions(singleton(transactionalId))).thenReturn(describeResult);

        // Add a little time so that we can see a positive transaction duration in the output
        time.sleep(5000);

        execute(args);
        assertNormalExit();

        List<List<String>> table = readOutputAsTable();
        assertEquals(2, table.size());

        List<String> expectedHeaders = TransactionsCommand.DescribeTransactionsCommand.HEADERS;
        assertEquals(expectedHeaders, table.get(0));

        List<String> expectedRow = asList(
            String.valueOf(coordinatorId),
            transactionalId,
            "12345",
            "15",
            "Ongoing",
            "10000",
            String.valueOf(transactionStartTime),
            "5000",
            "bar-0"
        );
        assertEquals(expectedRow, table.get(1));
    }

    @Test
    public void testDescribeTransactionsStartOffsetOrProducerIdRequired() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "abort",
            "--topic",
            "foo",
            "--partition",
            "0"
        });
    }

    @Test
    public void testDescribeTransactionsTopicRequired() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "abort",
            "--partition",
            "0",
            "--start-offset",
            "9990"
        });
    }

    @Test
    public void testDescribeTransactionsPartitionRequired() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "abort",
            "--topic",
            "foo",
            "--start-offset",
            "9990"
        });
    }

    @Test
    public void testDescribeTransactionsProducerEpochRequiredWithProducerId() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "abort",
            "--topic",
            "foo",
            "--partition",
            "0",
            "--producer-id",
            "12345"
        });
    }

    @Test
    public void testDescribeTransactionsCoordinatorEpochRequiredWithProducerId() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "abort",
            "--topic",
            "foo",
            "--partition",
            "0",
            "--producer-id",
            "12345",
            "--producer-epoch",
            "15"
        });
    }

    @Test
    public void testNewBrokerAbortTransaction() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        long startOffset = 9173;
        long producerId = 12345L;
        short producerEpoch = 15;
        int coordinatorEpoch = 76;

        String[] args = new String[] {
            "--bootstrap-server",
            "localhost:9092",
            "abort",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition()),
            "--start-offset",
            String.valueOf(startOffset)
        };

        DescribeProducersResult describeResult = Mockito.mock(DescribeProducersResult.class);
        KafkaFuture<PartitionProducerState> describeFuture = completedFuture(
            new PartitionProducerState(singletonList(
                new ProducerState(producerId, producerEpoch, 1300, 1599509565L,
                    OptionalInt.of(coordinatorEpoch), OptionalLong.of(startOffset))
            )));

        AbortTransactionResult abortTransactionResult = Mockito.mock(AbortTransactionResult.class);
        KafkaFuture<Void> abortFuture = completedFuture(null);
        AbortTransactionSpec expectedAbortSpec = new AbortTransactionSpec(
            topicPartition, producerId, producerEpoch, coordinatorEpoch);

        Mockito.when(describeResult.partitionResult(topicPartition)).thenReturn(describeFuture);
        Mockito.when(admin.describeProducers(singleton(topicPartition))).thenReturn(describeResult);

        Mockito.when(abortTransactionResult.all()).thenReturn(abortFuture);
        Mockito.when(admin.abortTransaction(expectedAbortSpec)).thenReturn(abortTransactionResult);

        execute(args);
        assertNormalExit();
    }

    @ParameterizedTest
    @ValueSource(ints = {29, -1})
    public void testOldBrokerAbortTransactionWithUnknownCoordinatorEpoch(int coordinatorEpoch) throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);
        long producerId = 12345L;
        short producerEpoch = 15;

        String[] args = new String[] {
            "--bootstrap-server",
            "localhost:9092",
            "abort",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition()),
            "--producer-id",
            String.valueOf(producerId),
            "--producer-epoch",
            String.valueOf(producerEpoch),
            "--coordinator-epoch",
            String.valueOf(coordinatorEpoch)
        };

        AbortTransactionResult abortTransactionResult = Mockito.mock(AbortTransactionResult.class);
        KafkaFuture<Void> abortFuture = completedFuture(null);

        int expectedCoordinatorEpoch = Math.max(coordinatorEpoch, 0);
        AbortTransactionSpec expectedAbortSpec = new AbortTransactionSpec(
            topicPartition, producerId, producerEpoch, expectedCoordinatorEpoch);

        Mockito.when(abortTransactionResult.all()).thenReturn(abortFuture);
        Mockito.when(admin.abortTransaction(expectedAbortSpec)).thenReturn(abortTransactionResult);

        execute(args);
        assertNormalExit();
    }

    @Test
    public void testFindHangingRequiresEitherBrokerIdOrTopic() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging"
        });
    }

    @Test
    public void testFindHangingRequiresTopicIfPartitionIsSpecified() throws Exception {
        assertCommandFailure(new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--broker-id",
            "0",
            "--partition",
            "5"
        });
    }

    private void expectListTransactions(
        Map<Integer, Collection<TransactionListing>> listingsByBroker
    ) {
        expectListTransactions(new ListTransactionsOptions(), listingsByBroker);
    }

    private void expectListTransactions(
        ListTransactionsOptions options,
        Map<Integer, Collection<TransactionListing>> listingsByBroker
    ) {
        ListTransactionsResult listResult = Mockito.mock(ListTransactionsResult.class);
        Mockito.when(admin.listTransactions(options)).thenReturn(listResult);

        List<TransactionListing> allListings = new ArrayList<>();
        listingsByBroker.values().forEach(allListings::addAll);

        Mockito.when(listResult.all()).thenReturn(completedFuture(allListings));
        Mockito.when(listResult.allByBrokerId()).thenReturn(completedFuture(listingsByBroker));
    }

    private void expectDescribeProducers(
        TopicPartition topicPartition,
        long producerId,
        short producerEpoch,
        long lastTimestamp,
        OptionalInt coordinatorEpoch,
        OptionalLong txnStartOffset
    ) {
        PartitionProducerState partitionProducerState = new PartitionProducerState(singletonList(
            new ProducerState(
                producerId,
                producerEpoch,
                500,
                lastTimestamp,
                coordinatorEpoch,
                txnStartOffset
            )
        ));

        DescribeProducersResult result = Mockito.mock(DescribeProducersResult.class);
        Mockito.when(result.all()).thenReturn(
            completedFuture(singletonMap(topicPartition, partitionProducerState))
        );

        Mockito.when(admin.describeProducers(
            Collections.singletonList(topicPartition),
            new DescribeProducersOptions()
        )).thenReturn(result);
    }

    private void expectDescribeTransactions(
        Map<String, TransactionDescription> descriptions
    ) {
        DescribeTransactionsResult result = Mockito.mock(DescribeTransactionsResult.class);
        descriptions.forEach((transactionalId, description) -> {
            Mockito.when(result.description(transactionalId))
                .thenReturn(completedFuture(description));
        });
        Mockito.when(result.all()).thenReturn(completedFuture(descriptions));
        Mockito.when(admin.describeTransactions(descriptions.keySet())).thenReturn(result);
    }

    private void expectListTopics(
        Set<String> topics
    ) {
        ListTopicsResult result = Mockito.mock(ListTopicsResult.class);
        Mockito.when(result.names()).thenReturn(completedFuture(topics));
        ListTopicsOptions listOptions = new ListTopicsOptions().listInternal(true);
        Mockito.when(admin.listTopics(listOptions)).thenReturn(result);
    }

    private void expectDescribeTopics(
        Map<String, TopicDescription> descriptions
    ) {
        DescribeTopicsResult result = Mockito.mock(DescribeTopicsResult.class);
        Mockito.when(result.allTopicNames()).thenReturn(completedFuture(descriptions));
        Mockito.when(admin.describeTopics(new ArrayList<>(descriptions.keySet()))).thenReturn(result);
    }

    @Test
    public void testFindHangingLookupTopicPartitionsForBroker() throws Exception {
        int brokerId = 5;

        String[] args = new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--broker-id",
            String.valueOf(brokerId)
        };

        String topic = "foo";
        expectListTopics(singleton(topic));

        Node node0 = new Node(0, "localhost", 9092);
        Node node1 = new Node(1, "localhost", 9093);
        Node node5 = new Node(5, "localhost", 9097);

        TopicPartitionInfo partition0 = new TopicPartitionInfo(
            0,
            node0,
            Arrays.asList(node0, node1),
            Arrays.asList(node0, node1)
        );
        TopicPartitionInfo partition1 = new TopicPartitionInfo(
            1,
            node1,
            Arrays.asList(node1, node5),
            Arrays.asList(node1, node5)
        );

        TopicDescription description = new TopicDescription(
            topic,
            false,
            Arrays.asList(partition0, partition1)
        );
        expectDescribeTopics(singletonMap(topic, description));

        DescribeProducersResult result = Mockito.mock(DescribeProducersResult.class);
        Mockito.when(result.all()).thenReturn(completedFuture(emptyMap()));

        Mockito.when(admin.describeProducers(
            Collections.singletonList(new TopicPartition(topic, 1)),
            new DescribeProducersOptions().brokerId(brokerId)
        )).thenReturn(result);

        execute(args);
        assertNormalExit();
        assertNoHangingTransactions();
    }

    @Test
    public void testFindHangingLookupTopicAndBrokerId() throws Exception {
        int brokerId = 5;
        String topic = "foo";

        String[] args = new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--broker-id",
            String.valueOf(brokerId),
            "--topic",
            topic
        };

        Node node0 = new Node(0, "localhost", 9092);
        Node node1 = new Node(1, "localhost", 9093);
        Node node5 = new Node(5, "localhost", 9097);

        TopicPartitionInfo partition0 = new TopicPartitionInfo(
            0,
            node0,
            Arrays.asList(node0, node1),
            Arrays.asList(node0, node1)
        );
        TopicPartitionInfo partition1 = new TopicPartitionInfo(
            1,
            node1,
            Arrays.asList(node1, node5),
            Arrays.asList(node1, node5)
        );

        TopicDescription description = new TopicDescription(
            topic,
            false,
            Arrays.asList(partition0, partition1)
        );
        expectDescribeTopics(singletonMap(topic, description));

        DescribeProducersResult result = Mockito.mock(DescribeProducersResult.class);
        Mockito.when(result.all()).thenReturn(completedFuture(emptyMap()));

        Mockito.when(admin.describeProducers(
            Collections.singletonList(new TopicPartition(topic, 1)),
            new DescribeProducersOptions().brokerId(brokerId)
        )).thenReturn(result);

        execute(args);
        assertNormalExit();
        assertNoHangingTransactions();
    }

    @Test
    public void testFindHangingLookupTopicPartitionsForTopic() throws Exception {
        String topic = "foo";

        String[] args = new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--topic",
            topic
        };

        Node node0 = new Node(0, "localhost", 9092);
        Node node1 = new Node(1, "localhost", 9093);
        Node node5 = new Node(5, "localhost", 9097);

        TopicPartitionInfo partition0 = new TopicPartitionInfo(
            0,
            node0,
            Arrays.asList(node0, node1),
            Arrays.asList(node0, node1)
        );
        TopicPartitionInfo partition1 = new TopicPartitionInfo(
            1,
            node1,
            Arrays.asList(node1, node5),
            Arrays.asList(node1, node5)
        );

        TopicDescription description = new TopicDescription(
            topic,
            false,
            Arrays.asList(partition0, partition1)
        );
        expectDescribeTopics(singletonMap(topic, description));

        DescribeProducersResult result = Mockito.mock(DescribeProducersResult.class);
        Mockito.when(result.all()).thenReturn(completedFuture(emptyMap()));

        Mockito.when(admin.describeProducers(
            Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)),
            new DescribeProducersOptions()
        )).thenReturn(result);

        execute(args);
        assertNormalExit();
        assertNoHangingTransactions();
    }

    private void assertNoHangingTransactions() throws Exception {
        List<List<String>> table = readOutputAsTable();
        assertEquals(1, table.size());

        List<String> expectedHeaders = TransactionsCommand.FindHangingTransactionsCommand.HEADERS;
        assertEquals(expectedHeaders, table.get(0));
    }

    @Test
    public void testFindHangingSpecifiedTopicPartition() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);

        String[] args = new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition())
        };

        long producerId = 132L;
        short producerEpoch = 5;
        long lastTimestamp = time.milliseconds();
        OptionalInt coordinatorEpoch = OptionalInt.of(19);
        OptionalLong txnStartOffset = OptionalLong.of(29384L);

        expectDescribeProducers(
            topicPartition,
            producerId,
            producerEpoch,
            lastTimestamp,
            coordinatorEpoch,
            txnStartOffset
        );

        execute(args);
        assertNormalExit();

        List<List<String>> table = readOutputAsTable();
        assertEquals(1, table.size());

        List<String> expectedHeaders = TransactionsCommand.FindHangingTransactionsCommand.HEADERS;
        assertEquals(expectedHeaders, table.get(0));
    }

    @Test
    public void testFindHangingNoMappedTransactionalId() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);

        String[] args = new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition())
        };

        long producerId = 132L;
        short producerEpoch = 5;
        long lastTimestamp = time.milliseconds() - TimeUnit.MINUTES.toMillis(60);
        int coordinatorEpoch = 19;
        long txnStartOffset = 29384L;

        expectDescribeProducers(
            topicPartition,
            producerId,
            producerEpoch,
            lastTimestamp,
            OptionalInt.of(coordinatorEpoch),
            OptionalLong.of(txnStartOffset)
        );

        expectListTransactions(
            new ListTransactionsOptions().filterProducerIds(singleton(producerId)),
            singletonMap(1, Collections.emptyList())
        );

        expectDescribeTransactions(Collections.emptyMap());

        execute(args);
        assertNormalExit();

        assertHangingTransaction(
            topicPartition,
            producerId,
            producerEpoch,
            coordinatorEpoch,
            txnStartOffset,
            lastTimestamp
        );
    }

    @Test
    public void testFindHangingWithNoTransactionDescription() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);

        String[] args = new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition())
        };

        long producerId = 132L;
        short producerEpoch = 5;
        long lastTimestamp = time.milliseconds() - TimeUnit.MINUTES.toMillis(60);
        int coordinatorEpoch = 19;
        long txnStartOffset = 29384L;

        expectDescribeProducers(
            topicPartition,
            producerId,
            producerEpoch,
            lastTimestamp,
            OptionalInt.of(coordinatorEpoch),
            OptionalLong.of(txnStartOffset)
        );

        String transactionalId = "bar";
        TransactionListing listing = new TransactionListing(
            transactionalId,
            producerId,
            TransactionState.ONGOING
        );

        expectListTransactions(
            new ListTransactionsOptions().filterProducerIds(singleton(producerId)),
            singletonMap(1, Collections.singletonList(listing))
        );

        DescribeTransactionsResult result = Mockito.mock(DescribeTransactionsResult.class);
        Mockito.when(result.description(transactionalId))
            .thenReturn(failedFuture(new TransactionalIdNotFoundException(transactionalId + " not found")));
        Mockito.when(admin.describeTransactions(singleton(transactionalId))).thenReturn(result);

        execute(args);
        assertNormalExit();

        assertHangingTransaction(
            topicPartition,
            producerId,
            producerEpoch,
            coordinatorEpoch,
            txnStartOffset,
            lastTimestamp
        );
    }

    private <T> KafkaFuture<T> failedFuture(Exception e) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(e);
        return future;
    }

    @Test
    public void testFindHangingDoesNotFilterByTransactionInProgressWithDifferentPartitions() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);

        String[] args = new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition())
        };

        long producerId = 132L;
        short producerEpoch = 5;
        long lastTimestamp = time.milliseconds() - TimeUnit.MINUTES.toMillis(60);
        int coordinatorEpoch = 19;
        long txnStartOffset = 29384L;

        expectDescribeProducers(
            topicPartition,
            producerId,
            producerEpoch,
            lastTimestamp,
            OptionalInt.of(coordinatorEpoch),
            OptionalLong.of(txnStartOffset)
        );

        String transactionalId = "bar";
        TransactionListing listing = new TransactionListing(
            transactionalId,
            producerId,
            TransactionState.ONGOING
        );

        expectListTransactions(
            new ListTransactionsOptions().filterProducerIds(singleton(producerId)),
            singletonMap(1, Collections.singletonList(listing))
        );

        // Although there is a transaction in progress from the same
        // producer epoch, it does not include the topic partition we
        // found when describing producers.
        TransactionDescription description = new TransactionDescription(
            1,
            TransactionState.ONGOING,
            producerId,
            producerEpoch,
            60000,
            OptionalLong.of(time.milliseconds()),
            singleton(new TopicPartition("foo", 10))
        );

        expectDescribeTransactions(singletonMap(transactionalId, description));

        execute(args);
        assertNormalExit();

        assertHangingTransaction(
            topicPartition,
            producerId,
            producerEpoch,
            coordinatorEpoch,
            txnStartOffset,
            lastTimestamp
        );
    }

    private void assertHangingTransaction(
        TopicPartition topicPartition,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch,
        long txnStartOffset,
        long lastTimestamp
    ) throws Exception {
        List<List<String>> table = readOutputAsTable();
        assertEquals(2, table.size());

        List<String> expectedHeaders = TransactionsCommand.FindHangingTransactionsCommand.HEADERS;
        assertEquals(expectedHeaders, table.get(0));

        long durationMinutes = TimeUnit.MILLISECONDS.toMinutes(time.milliseconds() - lastTimestamp);

        List<String> expectedRow = asList(
            topicPartition.topic(),
            String.valueOf(topicPartition.partition()),
            String.valueOf(producerId),
            String.valueOf(producerEpoch),
            String.valueOf(coordinatorEpoch),
            String.valueOf(txnStartOffset),
            String.valueOf(lastTimestamp),
            String.valueOf(durationMinutes)
        );
        assertEquals(expectedRow, table.get(1));
    }

    @Test
    public void testFindHangingFilterByTransactionInProgressWithSamePartition() throws Exception {
        TopicPartition topicPartition = new TopicPartition("foo", 5);

        String[] args = new String[]{
            "--bootstrap-server",
            "localhost:9092",
            "find-hanging",
            "--topic",
            topicPartition.topic(),
            "--partition",
            String.valueOf(topicPartition.partition())
        };

        long producerId = 132L;
        short producerEpoch = 5;
        long lastTimestamp = time.milliseconds() - TimeUnit.MINUTES.toMillis(60);
        int coordinatorEpoch = 19;
        long txnStartOffset = 29384L;

        expectDescribeProducers(
            topicPartition,
            producerId,
            producerEpoch,
            lastTimestamp,
            OptionalInt.of(coordinatorEpoch),
            OptionalLong.of(txnStartOffset)
        );

        String transactionalId = "bar";
        TransactionListing listing = new TransactionListing(
            transactionalId,
            producerId,
            TransactionState.ONGOING
        );

        expectListTransactions(
            new ListTransactionsOptions().filterProducerIds(singleton(producerId)),
            singletonMap(1, Collections.singletonList(listing))
        );

        // The coordinator shows an active transaction with the same epoch
        // which includes the partition, so no hanging transaction should
        // be detected.
        TransactionDescription description = new TransactionDescription(
            1,
            TransactionState.ONGOING,
            producerId,
            producerEpoch,
            60000,
            OptionalLong.of(lastTimestamp),
            singleton(topicPartition)
        );

        expectDescribeTransactions(singletonMap(transactionalId, description));

        execute(args);
        assertNormalExit();
        assertNoHangingTransactions();
    }

    private void execute(String[] args) throws Exception {
        TransactionsCommand.execute(args, ns -> admin, out, time);
    }

    private List<List<String>> readOutputAsTable() throws IOException {
        List<List<String>> table = new ArrayList<>();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        while (true) {
            List<String> row = readRow(reader);
            if (row == null) {
                break;
            }
            table.add(row);
        }
        return table;
    }

    private List<String> readRow(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        } else {
            return asList(line.split("\\s+"));
        }
    }

    private void assertNormalExit() {
        assertTrue(exitProcedure.hasExited());
        assertEquals(0, exitProcedure.statusCode());
    }

    private void assertCommandFailure(String[] args) throws Exception {
        execute(args);
        assertTrue(exitProcedure.hasExited());
        assertEquals(1, exitProcedure.statusCode());
    }

}
