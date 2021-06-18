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
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionsCommandTest {

    private final MockExitProcedure exitProcedure = new MockExitProcedure();
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
        KafkaFuture<PartitionProducerState> describeFuture = KafkaFutureImpl.completedFuture(
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

        List<String> expectedHeaders = asList(TransactionsCommand.DescribeProducersCommand.HEADERS);
        assertEquals(expectedHeaders, table.get(0));

        Set<List<String>> expectedRows = Utils.mkSet(
            asList("12345", "15", "20", "1300", "1599509565", "990"),
            asList("98765", "30", "-1", "2300", "1599509599", "None")
        );
        assertEquals(expectedRows, new HashSet<>(table.subList(1, table.size())));
    }

    @Test
    public void testListTransactions() throws Exception {
        String[] args = new String[] {
            "--bootstrap-server",
            "localhost:9092",
            "list"
        };

        ListTransactionsResult listResult = Mockito.mock(ListTransactionsResult.class);

        Map<Integer, Collection<TransactionListing>> transactions = new HashMap<>();
        transactions.put(0, asList(
            new TransactionListing("foo", 12345L, TransactionState.ONGOING),
            new TransactionListing("bar", 98765L, TransactionState.PREPARE_ABORT)
        ));
        transactions.put(1, singletonList(
            new TransactionListing("baz", 13579L, TransactionState.COMPLETE_COMMIT)
        ));

        KafkaFuture<Map<Integer, Collection<TransactionListing>>> listTransactionsFuture =
            KafkaFutureImpl.completedFuture(transactions);

        Mockito.when(admin.listTransactions()).thenReturn(listResult);
        Mockito.when(listResult.allByBrokerId()).thenReturn(listTransactionsFuture);

        execute(args);
        assertNormalExit();

        List<List<String>> table = readOutputAsTable();
        assertEquals(4, table.size());

        // Assert expected headers
        List<String> expectedHeaders = asList(TransactionsCommand.ListTransactionsCommand.HEADERS);
        assertEquals(expectedHeaders, table.get(0));

        Set<List<String>> expectedRows = Utils.mkSet(
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

        KafkaFuture<TransactionDescription> describeFuture = KafkaFutureImpl.completedFuture(
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

        List<String> expectedHeaders = asList(TransactionsCommand.DescribeTransactionsCommand.HEADERS);
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
        KafkaFuture<PartitionProducerState> describeFuture = KafkaFutureImpl.completedFuture(
            new PartitionProducerState(singletonList(
                new ProducerState(producerId, producerEpoch, 1300, 1599509565L,
                    OptionalInt.of(coordinatorEpoch), OptionalLong.of(startOffset))
            )));

        AbortTransactionResult abortTransactionResult = Mockito.mock(AbortTransactionResult.class);
        KafkaFuture<Void> abortFuture = KafkaFutureImpl.completedFuture(null);
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
        KafkaFuture<Void> abortFuture = KafkaFutureImpl.completedFuture(null);

        final int expectedCoordinatorEpoch;
        if (coordinatorEpoch < 0) {
            expectedCoordinatorEpoch = 0;
        } else {
            expectedCoordinatorEpoch = coordinatorEpoch;
        }

        AbortTransactionSpec expectedAbortSpec = new AbortTransactionSpec(
            topicPartition, producerId, producerEpoch, expectedCoordinatorEpoch);

        Mockito.when(abortTransactionResult.all()).thenReturn(abortFuture);
        Mockito.when(admin.abortTransaction(expectedAbortSpec)).thenReturn(abortTransactionResult);

        execute(args);
        assertNormalExit();
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
        assertTrue(exitProcedure.hasExited);
        assertEquals(0, exitProcedure.statusCode);
    }

    private void assertCommandFailure(String[] args) throws Exception {
        execute(args);
        assertTrue(exitProcedure.hasExited);
        assertEquals(1, exitProcedure.statusCode);
    }

    private static class MockExitProcedure implements Exit.Procedure {
        private boolean hasExited = false;
        private int statusCode;

        @Override
        public void execute(int statusCode, String message) {
            if (!this.hasExited) {
                this.hasExited = true;
                this.statusCode = statusCode;
            }
        }
    }

}
