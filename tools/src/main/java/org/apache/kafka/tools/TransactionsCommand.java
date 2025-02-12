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

import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static net.sourceforge.argparse4j.impl.Arguments.store;

public abstract class TransactionsCommand {
    private static final Logger log = LoggerFactory.getLogger(TransactionsCommand.class);

    protected final Time time;

    protected TransactionsCommand(Time time) {
        this.time = time;
    }

    /**
     * Get the name of this command (e.g. `describe-producers`).
     */
    abstract String name();

    /**
     * Specify the arguments needed for this command.
     */
    abstract void addSubparser(Subparsers subparsers);

    /**
     * Execute the command logic.
     */
    abstract void execute(Admin admin, Namespace ns, PrintStream out) throws Exception;


    static class AbortTransactionCommand extends TransactionsCommand {

        AbortTransactionCommand(Time time) {
            super(time);
        }

        @Override
        String name() {
            return "abort";
        }

        @Override
        void addSubparser(Subparsers subparsers) {
            Subparser subparser = subparsers.addParser(name())
                .help("abort a hanging transaction (requires administrative privileges)");

            subparser.addArgument("--topic")
                .help("topic name")
                .action(store())
                .type(String.class)
                .required(true);

            subparser.addArgument("--partition")
                .help("partition number")
                .action(store())
                .type(Integer.class)
                .required(true);

            ArgumentGroup newBrokerArgumentGroup = subparser
                .addArgumentGroup("Brokers on versions 3.0 and above")
                .description("For newer brokers, only the start offset of the transaction " +
                    "to be aborted is required");

            newBrokerArgumentGroup.addArgument("--start-offset")
                .help("start offset of the transaction to abort")
                .action(store())
                .type(Long.class);

            ArgumentGroup olderBrokerArgumentGroup = subparser
                .addArgumentGroup("Brokers on versions older than 3.0")
                .description("For older brokers, you must provide all of these arguments");

            olderBrokerArgumentGroup.addArgument("--producer-id")
                .help("producer id")
                .action(store())
                .type(Long.class);

            olderBrokerArgumentGroup.addArgument("--producer-epoch")
                .help("producer epoch")
                .action(store())
                .type(Short.class);

            olderBrokerArgumentGroup.addArgument("--coordinator-epoch")
                .help("coordinator epoch")
                .action(store())
                .type(Integer.class);
        }

        private AbortTransactionSpec buildAbortSpec(
            Admin admin,
            TopicPartition topicPartition,
            long startOffset
        ) throws Exception {
            final DescribeProducersResult.PartitionProducerState result;
            try {
                result = admin.describeProducers(singleton(topicPartition))
                    .partitionResult(topicPartition)
                    .get();
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to validate producer state for partition "
                    + topicPartition, e.getCause());
                return null;
            }

            Optional<ProducerState> foundProducerState = result.activeProducers().stream()
                .filter(producerState -> {
                    OptionalLong txnStartOffsetOpt = producerState.currentTransactionStartOffset();
                    return txnStartOffsetOpt.isPresent() && txnStartOffsetOpt.getAsLong() == startOffset;
                })
                .findFirst();

            if (foundProducerState.isEmpty()) {
                printErrorAndExit("Could not find any open transactions starting at offset " +
                    startOffset + " on partition " + topicPartition);
                return null;
            }

            ProducerState producerState = foundProducerState.get();
            return new AbortTransactionSpec(
                topicPartition,
                producerState.producerId(),
                (short) producerState.producerEpoch(),
                producerState.coordinatorEpoch().orElse(0)
            );
        }

        private void abortTransaction(
            Admin admin,
            AbortTransactionSpec abortSpec
        ) throws Exception {
            try {
                admin.abortTransaction(abortSpec).all().get();
            } catch (ExecutionException e) {
                TransactionsCommand.printErrorAndExit("Failed to abort transaction " + abortSpec, e.getCause());
            }
        }

        @Override
        void execute(Admin admin, Namespace ns, PrintStream out) throws Exception {
            String topicName = ns.getString("topic");
            Integer partitionId = ns.getInt("partition");
            TopicPartition topicPartition = new TopicPartition(topicName, partitionId);

            Long startOffset = ns.getLong("start_offset");
            Long producerId = ns.getLong("producer_id");

            if (startOffset == null && producerId == null) {
                printErrorAndExit("The transaction to abort must be identified either with " +
                    "--start-offset (for brokers on 3.0 or above) or with " +
                    "--producer-id, --producer-epoch, and --coordinator-epoch (for older brokers)");
                return;
            }

            final AbortTransactionSpec abortSpec;
            if (startOffset == null) {
                Short producerEpoch = ns.getShort("producer_epoch");
                if (producerEpoch == null) {
                    printErrorAndExit("Missing required argument --producer-epoch");
                    return;
                }

                Integer coordinatorEpoch = ns.getInt("coordinator_epoch");
                if (coordinatorEpoch == null) {
                    printErrorAndExit("Missing required argument --coordinator-epoch");
                    return;
                }

                // If a transaction was started by a new producerId and became hanging
                // before the initial commit/abort, then the coordinator epoch will be -1
                // as seen in the `DescribeProducers` output. In this case, we conservatively
                // use a coordinator epoch of 0, which is less than or equal to any possible
                // leader epoch.
                if (coordinatorEpoch < 0) {
                    coordinatorEpoch = 0;
                }

                abortSpec = new AbortTransactionSpec(
                    topicPartition,
                    producerId,
                    producerEpoch,
                    coordinatorEpoch
                );
            } else {
                abortSpec = buildAbortSpec(admin, topicPartition, startOffset);
            }

            abortTransaction(admin, abortSpec);
        }
    }

    static class DescribeProducersCommand extends TransactionsCommand {
        static final List<String> HEADERS = asList(
            "ProducerId",
            "ProducerEpoch",
            "LatestCoordinatorEpoch",
            "LastSequence",
            "LastTimestamp",
            "CurrentTransactionStartOffset"
        );

        DescribeProducersCommand(Time time) {
            super(time);
        }

        @Override
        public String name() {
            return "describe-producers";
        }

        @Override
        public void addSubparser(Subparsers subparsers) {
            Subparser subparser = subparsers.addParser(name())
                .help("describe the states of active producers for a topic partition");

            subparser.addArgument("--broker-id")
                .help("optional broker id to describe the producer state on a specific replica")
                .action(store())
                .type(Integer.class)
                .required(false);

            subparser.addArgument("--topic")
                .help("topic name")
                .action(store())
                .type(String.class)
                .required(true);

            subparser.addArgument("--partition")
                .help("partition number")
                .action(store())
                .type(Integer.class)
                .required(true);
        }

        @Override
        public void execute(Admin admin, Namespace ns, PrintStream out) throws Exception {
            DescribeProducersOptions options = new DescribeProducersOptions();
            Optional.ofNullable(ns.getInt("broker_id")).ifPresent(options::brokerId);

            String topicName = ns.getString("topic");
            Integer partitionId = ns.getInt("partition");
            TopicPartition topicPartition = new TopicPartition(topicName, partitionId);

            final DescribeProducersResult.PartitionProducerState result;

            try {
                result = admin.describeProducers(singleton(topicPartition), options)
                    .partitionResult(topicPartition)
                    .get();
            } catch (ExecutionException e) {
                String brokerClause = options.brokerId().isPresent() ?
                    "broker " + options.brokerId().getAsInt() :
                    "leader";
                printErrorAndExit("Failed to describe producers for partition " +
                        topicPartition + " on " + brokerClause, e.getCause());
                return;
            }

            List<List<String>> rows = result.activeProducers().stream().map(producerState -> {
                String currentTransactionStartOffsetColumnValue =
                    producerState.currentTransactionStartOffset().isPresent() ?
                        String.valueOf(producerState.currentTransactionStartOffset().getAsLong()) :
                        "None";

                return asList(
                    String.valueOf(producerState.producerId()),
                    String.valueOf(producerState.producerEpoch()),
                    String.valueOf(producerState.coordinatorEpoch().orElse(-1)),
                    String.valueOf(producerState.lastSequence()),
                    String.valueOf(producerState.lastTimestamp()),
                    currentTransactionStartOffsetColumnValue
                );
            }).toList();

            ToolsUtils.prettyPrintTable(HEADERS, rows, out);
        }
    }

    static class DescribeTransactionsCommand extends TransactionsCommand {
        static final List<String> HEADERS = asList(
            "CoordinatorId",
            "TransactionalId",
            "ProducerId",
            "ProducerEpoch",
            "TransactionState",
            "TransactionTimeoutMs",
            "CurrentTransactionStartTimeMs",
            "TransactionDurationMs",
            "TopicPartitions"
        );

        DescribeTransactionsCommand(Time time) {
            super(time);
        }

        @Override
        public String name() {
            return "describe";
        }

        @Override
        public void addSubparser(Subparsers subparsers) {
            Subparser subparser = subparsers.addParser(name())
                .description("Describe the state of an active transactional-id.")
                .help("describe the state of an active transactional-id");

            subparser.addArgument("--transactional-id")
                .help("transactional id")
                .action(store())
                .type(String.class)
                .required(true);
        }

        @Override
        public void execute(Admin admin, Namespace ns, PrintStream out) throws Exception {
            String transactionalId = ns.getString("transactional_id");

            final TransactionDescription result;
            try {
                result = admin.describeTransactions(singleton(transactionalId))
                    .description(transactionalId)
                    .get();
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to describe transaction state of " +
                    "transactional-id `" + transactionalId + "`", e.getCause());
                return;
            }

            final String transactionDurationMsColumnValue;
            final String transactionStartTimeMsColumnValue;

            if (result.transactionStartTimeMs().isPresent()) {
                long transactionStartTimeMs = result.transactionStartTimeMs().getAsLong();
                transactionStartTimeMsColumnValue = String.valueOf(transactionStartTimeMs);
                transactionDurationMsColumnValue = String.valueOf(time.milliseconds() - transactionStartTimeMs);
            } else {
                transactionStartTimeMsColumnValue = "None";
                transactionDurationMsColumnValue = "None";
            }

            List<String> row = asList(
                String.valueOf(result.coordinatorId()),
                transactionalId,
                String.valueOf(result.producerId()),
                String.valueOf(result.producerEpoch()),
                result.state().toString(),
                String.valueOf(result.transactionTimeoutMs()),
                transactionStartTimeMsColumnValue,
                transactionDurationMsColumnValue,
                result.topicPartitions().stream().map(TopicPartition::toString).collect(Collectors.joining(","))
            );

            ToolsUtils.prettyPrintTable(HEADERS, singletonList(row), out);
        }
    }

    static class ListTransactionsCommand extends TransactionsCommand {
        static final List<String> HEADERS = asList(
            "TransactionalId",
            "Coordinator",
            "ProducerId",
            "TransactionState"
        );

        ListTransactionsCommand(Time time) {
            super(time);
        }

        @Override
        public String name() {
            return "list";
        }

        @Override
        public void addSubparser(Subparsers subparsers) {
            Subparser subparser = subparsers.addParser(name())
                .help("list transactions");

            subparser.addArgument("--duration-filter")
                    .help("Duration (in millis) to filter by: if < 0, all transactions will be returned; " +
                            "otherwise, only transactions running longer than this duration will be returned")
                    .action(store())
                    .type(Long.class)
                    .required(false);
        }

        @Override
        public void execute(Admin admin, Namespace ns, PrintStream out) throws Exception {
            ListTransactionsOptions options = new ListTransactionsOptions();
            Optional.ofNullable(ns.getLong("duration_filter")).ifPresent(options::filterOnDuration);

            final Map<Integer, Collection<TransactionListing>> result;

            try {
                result = admin.listTransactions(options)
                    .allByBrokerId()
                    .get();
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to list transactions", e.getCause());
                return;
            }

            List<List<String>> rows = new ArrayList<>();
            for (Map.Entry<Integer, Collection<TransactionListing>> brokerListingsEntry : result.entrySet()) {
                String coordinatorIdString = brokerListingsEntry.getKey().toString();
                Collection<TransactionListing> listings = brokerListingsEntry.getValue();

                for (TransactionListing listing : listings) {
                    rows.add(asList(
                        listing.transactionalId(),
                        coordinatorIdString,
                        String.valueOf(listing.producerId()),
                        listing.state().toString()
                    ));
                }
            }

            ToolsUtils.prettyPrintTable(HEADERS, rows, out);
        }
    }

    static class FindHangingTransactionsCommand extends TransactionsCommand {
        private static final int MAX_BATCH_SIZE = 500;

        static final List<String> HEADERS = asList(
            "Topic",
            "Partition",
            "ProducerId",
            "ProducerEpoch",
            "CoordinatorEpoch",
            "StartOffset",
            "LastTimestamp",
            "Duration(min)"
        );

        FindHangingTransactionsCommand(Time time) {
            super(time);
        }

        @Override
        String name() {
            return "find-hanging";
        }

        @Override
        void addSubparser(Subparsers subparsers) {
            Subparser subparser = subparsers.addParser(name())
                .help("find hanging transactions");

            subparser.addArgument("--broker-id")
                .help("broker id to search for hanging transactions")
                .action(store())
                .type(Integer.class)
                .required(false);

            subparser.addArgument("--max-transaction-timeout")
                .help("maximum transaction timeout in minutes to limit the scope of the search (15 minutes by default)")
                .action(store())
                .type(Integer.class)
                .setDefault(15)
                .required(false);

            subparser.addArgument("--topic")
                .help("topic name to limit search to (required if --partition is specified)")
                .action(store())
                .type(String.class)
                .required(false);

            subparser.addArgument("--partition")
                .help("partition number")
                .action(store())
                .type(Integer.class)
                .required(false);
        }

        @Override
        void execute(Admin admin, Namespace ns, PrintStream out) throws Exception {
            Optional<Integer> brokerId = Optional.ofNullable(ns.getInt("broker_id"));
            Optional<String> topic = Optional.ofNullable(ns.getString("topic"));

            if (topic.isEmpty() && brokerId.isEmpty()) {
                printErrorAndExit("The `find-hanging` command requires either --topic " +
                    "or --broker-id to limit the scope of the search");
                return;
            }

            Optional<Integer> partition = Optional.ofNullable(ns.getInt("partition"));
            if (partition.isPresent() && topic.isEmpty()) {
                printErrorAndExit("The --partition argument requires --topic to be provided");
                return;
            }

            long maxTransactionTimeoutMs = TimeUnit.MINUTES.toMillis(
                ns.getInt("max_transaction_timeout"));

            List<TopicPartition> topicPartitions = collectTopicPartitionsToSearch(
                admin,
                topic,
                partition,
                brokerId
            );

            List<OpenTransaction> candidates = collectCandidateOpenTransactions(
                admin,
                brokerId,
                maxTransactionTimeoutMs,
                topicPartitions
            );

            if (candidates.isEmpty()) {
                printHangingTransactions(Collections.emptyList(), out);
            } else {
                Map<Long, List<OpenTransaction>> openTransactionsByProducerId = groupByProducerId(candidates);

                Map<Long, String> transactionalIds = lookupTransactionalIds(
                    admin,
                    openTransactionsByProducerId.keySet()
                );

                Map<String, TransactionDescription> descriptions = describeTransactions(
                    admin,
                    transactionalIds.values()
                );

                List<OpenTransaction> hangingTransactions = filterHangingTransactions(
                    openTransactionsByProducerId,
                    transactionalIds,
                    descriptions
                );

                printHangingTransactions(hangingTransactions, out);
            }
        }

        private List<TopicPartition> collectTopicPartitionsToSearch(
            Admin admin,
            Optional<String> topic,
            Optional<Integer> partition,
            Optional<Integer> brokerId
        ) throws Exception {
            final List<String> topics;

            if (topic.isPresent()) {
                if (partition.isPresent()) {
                    return Collections.singletonList(new TopicPartition(topic.get(), partition.get()));
                } else {
                    topics = Collections.singletonList(topic.get());
                }
            } else {
                topics = listTopics(admin);
            }

            return findTopicPartitions(
                admin,
                brokerId,
                topics
            );
        }

        private List<OpenTransaction> filterHangingTransactions(
            Map<Long, List<OpenTransaction>> openTransactionsByProducerId,
            Map<Long, String> transactionalIds,
            Map<String, TransactionDescription> descriptions
        ) {
            List<OpenTransaction> hangingTransactions = new ArrayList<>();

            openTransactionsByProducerId.forEach((producerId, openTransactions) -> {
                String transactionalId = transactionalIds.get(producerId);
                if (transactionalId == null) {
                    // If we could not find the transactionalId corresponding to the
                    // producerId of an open transaction, then the transaction is hanging.
                    hangingTransactions.addAll(openTransactions);
                } else {
                    // Otherwise, we need to check the current transaction state.
                    TransactionDescription description = descriptions.get(transactionalId);
                    if (description == null) {
                        hangingTransactions.addAll(openTransactions);
                    } else {
                        for (OpenTransaction openTransaction : openTransactions) {
                            // The `DescribeTransactions` API returns all partitions being
                            // written to in an ongoing transaction and any partition which
                            // does not yet have markers written when in the `PendingAbort` or
                            // `PendingCommit` states. If the topic partition that we found is
                            // among these, then we can still expect the coordinator to write
                            // the marker. Otherwise, it is a hanging transaction.
                            if (!description.topicPartitions().contains(openTransaction.topicPartition)) {
                                hangingTransactions.add(openTransaction);
                            }
                        }
                    }
                }
            });

            return hangingTransactions;
        }

        private void printHangingTransactions(
            List<OpenTransaction> hangingTransactions,
            PrintStream out
        ) {
            long currentTimeMs = time.milliseconds();
            List<List<String>> rows = new ArrayList<>(hangingTransactions.size());

            for (OpenTransaction transaction : hangingTransactions) {
                long transactionDurationMinutes = TimeUnit.MILLISECONDS.toMinutes(
                    currentTimeMs - transaction.producerState.lastTimestamp());

                rows.add(asList(
                    transaction.topicPartition.topic(),
                    String.valueOf(transaction.topicPartition.partition()),
                    String.valueOf(transaction.producerState.producerId()),
                    String.valueOf(transaction.producerState.producerEpoch()),
                    String.valueOf(transaction.producerState.coordinatorEpoch().orElse(-1)),
                    String.valueOf(transaction.producerState.currentTransactionStartOffset().orElse(-1)),
                    String.valueOf(transaction.producerState.lastTimestamp()),
                    String.valueOf(transactionDurationMinutes)
                ));
            }

            ToolsUtils.prettyPrintTable(HEADERS, rows, out);
        }

        private Map<String, TransactionDescription> describeTransactions(
            Admin admin,
            Collection<String> transactionalIds
        ) throws Exception {
            try {
                DescribeTransactionsResult result = admin.describeTransactions(new HashSet<>(transactionalIds));
                Map<String, TransactionDescription> descriptions = new HashMap<>();

                for (String transactionalId : transactionalIds) {
                    try {
                        TransactionDescription description = result.description(transactionalId).get();
                        descriptions.put(transactionalId, description);
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof TransactionalIdNotFoundException) {
                            descriptions.put(transactionalId, null);
                        } else {
                            throw e;
                        }
                    }
                }

                return descriptions;
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to describe " + transactionalIds.size()
                    + " transactions", e.getCause());
                return Collections.emptyMap();
            }
        }

        private Map<Long, List<OpenTransaction>> groupByProducerId(
            List<OpenTransaction> openTransactions
        ) {
            Map<Long, List<OpenTransaction>> res = new HashMap<>();
            for (OpenTransaction transaction : openTransactions) {
                List<OpenTransaction> states = res.computeIfAbsent(
                    transaction.producerState.producerId(),
                    __ -> new ArrayList<>()
                );
                states.add(transaction);
            }
            return res;
        }

        private List<String> listTopics(
            Admin admin
        ) throws Exception {
            try {
                ListTopicsOptions listOptions = new ListTopicsOptions().listInternal(true);
                return new ArrayList<>(admin.listTopics(listOptions).names().get());
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to list topics", e.getCause());
                return Collections.emptyList();
            }
        }

        private List<TopicPartition> findTopicPartitions(
            Admin admin,
            Optional<Integer> brokerId,
            List<String> topics
        ) throws Exception {
            List<TopicPartition> topicPartitions = new ArrayList<>();
            consumeInBatches(topics, MAX_BATCH_SIZE, batch -> {
                findTopicPartitions(
                    admin,
                    brokerId,
                    batch,
                    topicPartitions
                );
            });
            return topicPartitions;
        }

        private void findTopicPartitions(
            Admin admin,
            Optional<Integer> brokerId,
            List<String> topics,
            List<TopicPartition> topicPartitions
        ) throws Exception {
            try {
                Map<String, TopicDescription> topicDescriptions = admin.describeTopics(topics).allTopicNames().get();
                topicDescriptions.forEach((topic, description) -> {
                    description.partitions().forEach(partitionInfo -> {
                        if (brokerId.isEmpty() || hasReplica(brokerId.get(), partitionInfo)) {
                            topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
                        }
                    });
                });
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to describe " + topics.size() + " topics", e.getCause());
            }
        }

        private boolean hasReplica(
            int brokerId,
            TopicPartitionInfo partitionInfo
        ) {
            return partitionInfo.replicas().stream().anyMatch(node -> node.id() == brokerId);
        }

        private List<OpenTransaction> collectCandidateOpenTransactions(
            Admin admin,
            Optional<Integer> brokerId,
            long maxTransactionTimeoutMs,
            List<TopicPartition> topicPartitions
        ) throws Exception {
            // We have to check all partitions on the broker. In order to avoid
            // overwhelming it with a giant request, we break the requests into
            // smaller batches.

            List<OpenTransaction> candidateTransactions = new ArrayList<>();

            consumeInBatches(topicPartitions, MAX_BATCH_SIZE, batch -> {
                collectCandidateOpenTransactions(
                    admin,
                    brokerId,
                    maxTransactionTimeoutMs,
                    batch,
                    candidateTransactions
                );
            });

            return candidateTransactions;
        }

        private static class OpenTransaction {
            private final TopicPartition topicPartition;
            private final ProducerState producerState;

            private OpenTransaction(
                TopicPartition topicPartition,
                ProducerState producerState
            ) {
                this.topicPartition = topicPartition;
                this.producerState = producerState;
            }
        }

        private void collectCandidateOpenTransactions(
            Admin admin,
            Optional<Integer> brokerId,
            long maxTransactionTimeoutMs,
            List<TopicPartition> topicPartitions,
            List<OpenTransaction> candidateTransactions
        ) throws Exception {
            try {
                DescribeProducersOptions describeOptions = new DescribeProducersOptions();
                brokerId.ifPresent(describeOptions::brokerId);

                Map<TopicPartition, DescribeProducersResult.PartitionProducerState> producersByPartition =
                    admin.describeProducers(topicPartitions, describeOptions).all().get();

                long currentTimeMs = time.milliseconds();

                producersByPartition.forEach((topicPartition, producersStates) -> {
                    producersStates.activeProducers().forEach(activeProducer -> {
                        if (activeProducer.currentTransactionStartOffset().isPresent()) {
                            long transactionDurationMs = currentTimeMs - activeProducer.lastTimestamp();
                            if (transactionDurationMs > maxTransactionTimeoutMs) {
                                candidateTransactions.add(new OpenTransaction(
                                    topicPartition,
                                    activeProducer
                                ));
                            }
                        }
                    });
                });
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to describe producers for " + topicPartitions.size() +
                    " partitions on broker " + brokerId, e.getCause());
            }
        }

        private Map<Long, String> lookupTransactionalIds(
            Admin admin,
            Set<Long> producerIds
        ) throws Exception {
            try {
                ListTransactionsOptions listTransactionsOptions = new ListTransactionsOptions()
                    .filterProducerIds(producerIds);

                Collection<TransactionListing> transactionListings =
                    admin.listTransactions(listTransactionsOptions).all().get();

                Map<Long, String> transactionalIdMap = new HashMap<>();

                transactionListings.forEach(listing -> {
                    if (!producerIds.contains(listing.producerId())) {
                        log.debug("Received transaction listing {} which has a producerId " +
                            "which was not requested", listing);
                    } else {
                        transactionalIdMap.put(
                            listing.producerId(),
                            listing.transactionalId()
                        );
                    }
                });

                return transactionalIdMap;
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to list transactions for " + producerIds.size() +
                    " producers", e.getCause());
                return Collections.emptyMap();
            }
        }

        @FunctionalInterface
        private interface ThrowableConsumer<T> {
            void accept(T t) throws Exception;
        }

        private <T> void consumeInBatches(
            List<T> list,
            int batchSize,
            ThrowableConsumer<List<T>> consumer
        ) throws Exception {
            int batchStartIndex = 0;
            int limitIndex = list.size();

            while (batchStartIndex < limitIndex) {
                int batchEndIndex = Math.min(
                    limitIndex,
                    batchStartIndex + batchSize
                );

                consumer.accept(list.subList(batchStartIndex, batchEndIndex));
                batchStartIndex = batchEndIndex;
            }
        }
    }

    private static void printErrorAndExit(String message, Throwable t) {
        log.debug(message, t);

        String exitMessage = message + ": " + t.getMessage() + "." +
            " Enable debug logging for additional detail.";

        printErrorAndExit(exitMessage);
    }

    private static void printErrorAndExit(String message) {
        System.err.println(message);
        Exit.exit(1, message);
    }

    private static Admin buildAdminClient(Namespace ns) {
        final Properties properties;

        String configFile = ns.getString("command_config");
        if (configFile == null) {
            properties = new Properties();
        } else {
            try {
                properties = Utils.loadProps(configFile);
            } catch (IOException e) {
                printErrorAndExit("Failed to load admin client properties", e);
                return null;
            }
        }

        String bootstrapServers = ns.getString("bootstrap_server");
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return Admin.create(properties);
    }

    static ArgumentParser buildBaseParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-transactions.sh");

        parser.description("This tool is used to analyze the transactional state of producers in the cluster. " +
            "It can be used to detect and recover from hanging transactions.");

        parser.addArgument("-v", "--version")
            .action(new PrintVersionAndExitAction())
            .help("show the version of this Kafka distribution and exit");

        parser.addArgument("--command-config")
            .help("property file containing configs to be passed to admin client")
            .action(store())
            .type(String.class)
            .metavar("FILE")
            .required(false);

        parser.addArgument("--bootstrap-server")
            .help("hostname and port for the broker to connect to, in the form `host:port`  " +
                "(multiple comma-separated entries can be given)")
            .action(store())
            .type(String.class)
            .metavar("host:port")
            .required(true);

        return parser;
    }

    static void execute(
        String[] args,
        Function<Namespace, Admin> adminSupplier,
        PrintStream out,
        Time time
    ) throws Exception {
        List<TransactionsCommand> commands = asList(
            new ListTransactionsCommand(time),
            new DescribeTransactionsCommand(time),
            new DescribeProducersCommand(time),
            new AbortTransactionCommand(time),
            new FindHangingTransactionsCommand(time)
        );

        ArgumentParser parser = buildBaseParser();
        Subparsers subparsers = parser.addSubparsers()
            .dest("command")
            .title("commands")
            .metavar("COMMAND");
        commands.forEach(command -> command.addSubparser(subparsers));

        final Namespace ns;

        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
            return;
        }

        Admin admin = adminSupplier.apply(ns);
        String commandName = ns.getString("command");

        Optional<TransactionsCommand> commandOpt = commands.stream()
            .filter(cmd -> cmd.name().equals(commandName))
            .findFirst();

        if (commandOpt.isEmpty()) {
            printErrorAndExit("Unexpected command " + commandName);
        }

        TransactionsCommand command = commandOpt.get();
        command.execute(admin, ns, out);
        Exit.exit(0);
    }

    public static void main(String[] args) throws Exception {
        execute(args, TransactionsCommand::buildAdminClient, System.out, Time.SYSTEM);
    }

}
