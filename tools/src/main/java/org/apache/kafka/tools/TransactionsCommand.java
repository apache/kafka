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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

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

            ArgumentGroup newBrokerArgumentGroup = subparser.addArgumentGroup("new brokers")
                .description("For newer brokers, you must provide the start offset of the transaction " +
                    "to be aborted");

            newBrokerArgumentGroup.addArgument("--start-offset")
                .help("start offset of the transaction to abort")
                .action(store())
                .type(Long.class);

            ArgumentGroup olderBrokerArgumentGroup = subparser.addArgumentGroup("older brokers")
                .description("For older brokers, you must provide all of these arguments");

            olderBrokerArgumentGroup.addArgument("--producer-id")
                .help("producer id")
                .action(store())
                .type(Long.class);

            olderBrokerArgumentGroup.addArgument("--producer-epoch")
                .help("producer epoch")
                .action(store())
                .type(Integer.class);

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

            if (!foundProducerState.isPresent()) {
                printErrorAndExit("Could not find any open transactions starting at offset " +
                    startOffset + " on partition " + topicPartition);
                return null;
            }

            ProducerState producerState = foundProducerState.get();
            return new AbortTransactionSpec(
                topicPartition,
                producerState.producerId(),
                producerState.producerEpoch(),
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
                    "--start-offset (for newer brokers) or with " +
                    "--producer-id, --producer-epoch, and --coordinator-epoch (for older brokers)");
                return;
            }

            final AbortTransactionSpec abortSpec;
            if (startOffset == null) {
                Integer producerEpoch = ns.getInt("producer_epoch");
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
            Optional.ofNullable(ns.getInt("broker_id")).ifPresent(options::setBrokerId);

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

            String[] headers = new String[]{
                "ProducerId",
                "ProducerEpoch",
                "LatestCoordinatorEpoch",
                "LastSequence",
                "LastTimestamp",
                "CurrentTransactionStartOffset"
            };

            List<String[]> rows = result.activeProducers().stream().map(producerState -> {
                String currentTransactionStartOffsetColumnValue =
                    producerState.currentTransactionStartOffset().isPresent() ?
                        String.valueOf(producerState.currentTransactionStartOffset().getAsLong()) :
                        "None";

                return new String[] {
                    String.valueOf(producerState.producerId()),
                    String.valueOf(producerState.producerEpoch()),
                    String.valueOf(producerState.coordinatorEpoch().orElse(-1)),
                    String.valueOf(producerState.lastSequence()),
                    String.valueOf(producerState.lastTimestamp()),
                    currentTransactionStartOffsetColumnValue
                };
            }).collect(Collectors.toList());

            prettyPrintTable(headers, rows, out);
        }
    }

    static class DescribeTransactionsCommand extends TransactionsCommand {

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
                    .transactionalIdResult(transactionalId)
                    .get();
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to describe transaction state of " +
                    "transactional-id `" + transactionalId + "`", e.getCause());
                return;
            }

            String[] headers = new String[]{
                "CoordinatorId",
                "TransactionalId",
                "ProducerId",
                "ProducerEpoch",
                "TransactionState",
                "TransactionTimeoutMs",
                "CurrentTransactionStartTimeMs",
                "TransactionDurationMs",
                "TopicPartitions"
            };

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

            String[] row = new String[]{
                String.valueOf(result.coordinatorId()),
                transactionalId,
                String.valueOf(result.producerId()),
                String.valueOf(result.producerEpoch()),
                result.state().toString(),
                String.valueOf(result.transactionTimeoutMs()),
                transactionStartTimeMsColumnValue,
                transactionDurationMsColumnValue,
                Utils.join(result.topicPartitions(), ",")
            };

            prettyPrintTable(headers, singletonList(row), out);
        }
    }

    static class ListTransactionsCommand extends TransactionsCommand {

        ListTransactionsCommand(Time time) {
            super(time);
        }

        @Override
        public String name() {
            return "list";
        }

        @Override
        public void addSubparser(Subparsers subparsers) {
            subparsers.addParser(name())
                .help("list transactions");
        }

        @Override
        public void execute(Admin admin, Namespace ns, PrintStream out) throws Exception {
            final Map<Integer, Collection<TransactionListing>> result;

            try {
                result = admin.listTransactions()
                    .allByBrokerId()
                    .get();
            } catch (ExecutionException e) {
                printErrorAndExit("Failed to list transactions", e.getCause());
                return;
            }

            String[] headers = new String[] {
                "TransactionalId",
                "Coordinator",
                "ProducerId",
                "TransactionState"
            };

            List<String[]> rows = new ArrayList<>();
            for (Map.Entry<Integer, Collection<TransactionListing>> brokerListingsEntry : result.entrySet()) {
                String coordinatorIdString = brokerListingsEntry.getKey().toString();
                Collection<TransactionListing> listings = brokerListingsEntry.getValue();

                for (TransactionListing listing : listings) {
                    rows.add(new String[] {
                        listing.transactionalId(),
                        coordinatorIdString,
                        String.valueOf(listing.producerId()),
                        listing.state().toString()
                    });
                }
            }

            prettyPrintTable(headers, rows, out);
        }
    }

    private static void appendColumnValue(
        StringBuilder rowBuilder,
        String value,
        int length
    ) {
        int padLength = length - value.length();
        rowBuilder.append(value);
        for (int i = 0; i < padLength; i++)
            rowBuilder.append(' ');
    }

    private static void printRow(
        List<Integer> columnLengths,
        String[] row,
        PrintStream out
    ) {
        StringBuilder rowBuilder = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            Integer columnLength = columnLengths.get(i);
            String columnValue = row[i];
            appendColumnValue(rowBuilder, columnValue, columnLength);
            rowBuilder.append('\t');
        }
        out.println(rowBuilder.toString());
    }

    private static void prettyPrintTable(
        String[] headers,
        List<String[]> rows,
        PrintStream out
    ) {
        List<Integer> columnLengths = Arrays.stream(headers)
            .map(String::length)
            .collect(Collectors.toList());

        for (String[] row : rows) {
            for (int i = 0; i < headers.length; i++) {
                columnLengths.set(i, Math.max(columnLengths.get(i), row[i].length()));
            }
        }

        printRow(columnLengths, headers, out);
        rows.forEach(row -> printRow(columnLengths, row, out));
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
                throw new RuntimeException(e);
            }
        }

        String bootstrapServers = ns.getString("bootstrap_server");
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return Admin.create(properties);
    }

    static ArgumentParser buildBaseParser() {
        ArgumentParser parser = ArgumentParsers.newFor("kafka-transactions.sh").build();

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
        List<TransactionsCommand> commands = Arrays.asList(
            new ListTransactionsCommand(time),
            new DescribeTransactionsCommand(time),
            new DescribeProducersCommand(time),
            new AbortTransactionCommand(time)
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

        if (!commandOpt.isPresent()) {
            throw new IllegalArgumentException("Unexpected command " + commandName);
        }

        TransactionsCommand command = commandOpt.get();
        command.execute(admin, ns, out);
        Exit.exit(0);
    }

    public static void main(String[] args) throws Exception {
        execute(args, TransactionsCommand::buildAdminClient, System.out, Time.SYSTEM);
    }

}
