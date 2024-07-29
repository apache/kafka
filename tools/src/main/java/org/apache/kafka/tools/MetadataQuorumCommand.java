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

import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;

/**
 * A tool for describing quorum status
 */
public class MetadataQuorumCommand {

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static final String DIRECTORY_ID_PROP = "directory.id";
    static final String META_PROPERTIES_NAME = "meta.properties";

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-metadata-quorum")
            .defaultHelp(true)
            .description("This tool describes kraft metadata quorum status.");
        MutuallyExclusiveGroup connectionOptions = parser.addMutuallyExclusiveGroup().required(true);
        connectionOptions.addArgument("--bootstrap-server")
            .help("A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.");
        connectionOptions.addArgument("--bootstrap-controller")
            .help("A comma-separated list of host:port pairs to use for establishing the connection to the Kafka controllers.");
        parser.addArgument("--command-config")
            .type(Arguments.fileType())
            .help("Property file containing configs to be passed to Admin Client.");
        addDescribeSubParser(parser);
        addAddControllerSubParser(parser);
        addRemoveControllerSubParser(parser);

        Admin admin = null;
        try {
            Namespace namespace = parser.parseArgsOrFail(args);
            String command = namespace.getString("command");

            File optionalCommandConfig = namespace.get("command_config");
            final Properties props = getProperties(optionalCommandConfig);
            CommandLineUtils.initializeBootstrapProperties(props,
                Optional.ofNullable(namespace.getString("bootstrap_server")),
                Optional.ofNullable(namespace.getString("bootstrap_controller")));
            admin = Admin.create(props);

            switch (command) {
                case "describe":
                    if (namespace.getBoolean("status") && namespace.getBoolean("replication")) {
                        throw new TerseException("Only one of --status or --replication should be specified with describe sub-command");
                    } else if (namespace.getBoolean("replication")) {
                        boolean humanReadable = Optional.of(namespace.getBoolean("human_readable")).orElse(false);
                        handleDescribeReplication(admin, humanReadable);
                    } else if (namespace.getBoolean("status")) {
                        if (namespace.getBoolean("human_readable")) {
                            throw new TerseException("The option --human-readable is only supported along with --replication");
                        }
                        handleDescribeStatus(admin);
                    } else {
                        throw new TerseException("One of --status or --replication must be specified with describe sub-command");
                    }
                    break;
                case "add-controller": {
                    String configPath = namespace.getString("config");
                    if (configPath != null) {
                        KafkaConfig kafkaConfig = new KafkaConfig(Utils.loadProps(configPath));
                        handleAddController(admin, kafkaConfig);
                    }
                    break;
                }
                case "remove-controller": {
                    String configPath = namespace.getString("config");
                    if (configPath != null) {
                        KafkaConfig kafkaConfig = new KafkaConfig(Utils.loadProps(configPath));
                        handleRemoveController(admin, kafkaConfig);
                    }
                    break;
                }
                default:
                    throw new IllegalStateException(format("Unknown command: %s, only 'describe' is supported", command));
            }
        } finally {
            if (admin != null)
                admin.close();
        }
    }

    private static Properties getProperties(File optionalCommandConfig) throws TerseException, IOException {
        if (optionalCommandConfig == null) {
            return new Properties();
        } else {
            if (!optionalCommandConfig.exists())
                throw new TerseException("Properties file " + optionalCommandConfig.getPath() + " does not exists!");
            return Utils.loadProps(optionalCommandConfig.getPath());
        }
    }

    private static void addDescribeSubParser(ArgumentParser parser) {
        Subparsers subparsers = parser.addSubparsers().dest("command");
        Subparser describeParser = subparsers
            .addParser("describe")
            .help("Describe the metadata quorum info");

        ArgumentGroup statusArgs = describeParser.addArgumentGroup("Status");
        statusArgs
            .addArgument("--status")
            .help("A short summary of the quorum status and the other provides detailed information about the status of replication.")
            .action(Arguments.storeTrue());

        ArgumentGroup replicationArgs = describeParser.addArgumentGroup("Replication");
        replicationArgs
            .addArgument("--replication")
            .help("Detailed information about the status of replication")
            .action(Arguments.storeTrue());
        replicationArgs
            .addArgument("--human-readable")
            .help("Human-readable output")
            .action(Arguments.storeTrue());
    }

    private static void handleDescribeReplication(Admin admin, boolean humanReadable) throws ExecutionException, InterruptedException {
        QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
        int leaderId = quorumInfo.leaderId();
        QuorumInfo.ReplicaState leader = quorumInfo.voters().stream().filter(voter -> voter.replicaId() == leaderId).findFirst().get();

        List<List<String>> rows = new ArrayList<>();
        rows.addAll(quorumInfoToRows(leader, Stream.of(leader), "Leader", humanReadable));
        rows.addAll(quorumInfoToRows(leader, quorumInfo.voters().stream().filter(v -> v.replicaId() != leaderId), "Follower", humanReadable));
        rows.addAll(quorumInfoToRows(leader, quorumInfo.observers().stream(), "Observer", humanReadable));

        ToolsUtils.prettyPrintTable(
            asList("NodeId", "LogEndOffset", "Lag", "LastFetchTimestamp", "LastCaughtUpTimestamp", "Status"),
            rows,
            System.out
        );
    }

    private static List<List<String>> quorumInfoToRows(QuorumInfo.ReplicaState leader,
                                                       Stream<QuorumInfo.ReplicaState> infos,
                                                       String status,
                                                       boolean humanReadable) {
        return infos.map(info -> {
            String lastFetchTimestamp = !info.lastFetchTimestamp().isPresent() ? "-1" :
                humanReadable ? format("%d ms ago", relativeTimeMs(info.lastFetchTimestamp().getAsLong(), "last fetch")) :
                    valueOf(info.lastFetchTimestamp().getAsLong());
            String lastCaughtUpTimestamp = !info.lastCaughtUpTimestamp().isPresent() ? "-1" :
                humanReadable ? format("%d ms ago", relativeTimeMs(info.lastCaughtUpTimestamp().getAsLong(), "last caught up")) :
                    valueOf(info.lastCaughtUpTimestamp().getAsLong());
            return Stream.of(
                info.replicaId(),
                info.logEndOffset(),
                leader.logEndOffset() - info.logEndOffset(),
                lastFetchTimestamp,
                lastCaughtUpTimestamp,
                status
            ).map(r -> r.toString()).collect(Collectors.toList());
        }).collect(Collectors.toList());
    }

    // visible for testing
    static long relativeTimeMs(long timestampMs, String desc) {
        Instant lastTimestamp = Instant.ofEpochMilli(timestampMs);
        Instant now = Instant.now();
        if (!(lastTimestamp.isAfter(Instant.EPOCH) && (lastTimestamp.isBefore(now) || lastTimestamp.equals(now)))) {
            throw new KafkaException(
                format("Error while computing relative time, possible drift in system clock.%n" +
                    "Current timestamp is %d, %s timestamp is %d", now.toEpochMilli(), desc, timestampMs)
            );
        }
        return Duration.between(lastTimestamp, now).toMillis();
    }

    private static void handleDescribeStatus(Admin admin) throws ExecutionException, InterruptedException {
        String clusterId = admin.describeCluster().clusterId().get();
        QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
        int leaderId = quorumInfo.leaderId();
        QuorumInfo.ReplicaState leader = quorumInfo.voters().stream().filter(voter -> voter.replicaId() == leaderId).findFirst().get();
        QuorumInfo.ReplicaState maxLagFollower = quorumInfo.voters().stream().min(Comparator.comparingLong(qi -> qi.logEndOffset())).get();
        long maxFollowerLag = leader.logEndOffset() - maxLagFollower.logEndOffset();

        long maxFollowerLagTimeMs;
        if (leader == maxLagFollower)
            maxFollowerLagTimeMs = 0;
        else if (leader.lastCaughtUpTimestamp().isPresent() && maxLagFollower.lastCaughtUpTimestamp().isPresent()) {
            maxFollowerLagTimeMs = leader.lastCaughtUpTimestamp().getAsLong() - maxLagFollower.lastCaughtUpTimestamp().getAsLong();
        } else {
            maxFollowerLagTimeMs = -1;
        }

        System.out.println(
            "ClusterId:              " + clusterId +
            "\nLeaderId:               " + quorumInfo.leaderId() +
            "\nLeaderEpoch:            " + quorumInfo.leaderEpoch() +
            "\nHighWatermark:          " + quorumInfo.highWatermark() +
            "\nMaxFollowerLag:         " + maxFollowerLag +
            "\nMaxFollowerLagTimeMs:   " + maxFollowerLagTimeMs +
            "\nCurrentVoters:          " + quorumInfo.voters().stream().map(QuorumInfo.ReplicaState::replicaId).map(Object::toString).collect(Collectors.joining(",", "[", "]")) +
            "\nCurrentObservers:       " + quorumInfo.observers().stream().map(QuorumInfo.ReplicaState::replicaId).map(Objects::toString).collect(Collectors.joining(",", "[", "]"))
        );
    }

    private static void addAddControllerSubParser(ArgumentParser parser) {
        Subparsers subparsers = parser.addSubparsers().dest("command");
        Subparser controllerParser = subparsers
            .addParser("add-controller")
            .help("Add controllers to the KRaft cluster metadata partition");

        ArgumentGroup statusArgs = controllerParser.addArgumentGroup("Config");
        statusArgs
            .addArgument("--config")
            .help("From the provided server.properties, read the replica id, the endpoints, and the meta.properties for the directory id.")
            .action(Arguments.store());
    }

    private static void addRemoveControllerSubParser(ArgumentParser parser) {
        Subparsers subparsers = parser.addSubparsers().dest("command");
        Subparser controllerParser = subparsers
            .addParser("remove-controller")
            .help("Remove controllers from KRaft cluster metadata partition");

        ArgumentGroup statusArgs = controllerParser.addArgumentGroup("Config");
        statusArgs
            .addArgument("--config")
            .help("From the provided server.properties, read the replica id and the meta.properties for the directory id.")
            .action(Arguments.store());
    }

    private static void handleAddController(Admin admin, KafkaConfig kafkaConfig) {
        int nodeId = kafkaConfig.nodeId();
        scala.collection.Seq<EndPoint> endPointSeq = kafkaConfig.effectiveAdvertisedControllerListeners();

        Set<RaftVoterEndpoint> raftVoterEndpoints  = new HashSet<>();
        for (int i = 0; i < endPointSeq.length(); i++) {
            EndPoint endPoint = endPointSeq.apply(i);
            String host = Optional.ofNullable(endPoint.host()).orElse("localhost");

            RaftVoterEndpoint raftVoterEndpoint = new RaftVoterEndpoint(endPoint.listenerName().value(), host, endPoint.port());
            raftVoterEndpoints.add(raftVoterEndpoint);
        }

        Optional<Uuid> directoryId = getDirectoryId(kafkaConfig.metadataLogDir());
        if (directoryId.isPresent()) {
            admin.addRaftVoter(nodeId, directoryId.get(), raftVoterEndpoints);
            System.out.println("Controller added." +
                    "\nVoterId:              " + nodeId +
                    "\nDirectoryId:     " + directoryId.get() +
                    "\nVoterEndpoints:  " + raftVoterEndpoints.stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"))
            );
        } else {
            throw new RuntimeException("Unable to fetch directory id from meta properties file.");
        }
    }

    private static void handleRemoveController(Admin admin, KafkaConfig kafkaConfig) {
        int nodeId = kafkaConfig.nodeId();
        Optional<Uuid> directoryId = getDirectoryId(kafkaConfig.metadataLogDir());
        if (directoryId.isPresent()) {
            admin.removeRaftVoter(nodeId, directoryId.get());
            System.out.println("Controller removed." +
                "\nVoterId:              " + nodeId +
                "\nDirectoryId:     " + directoryId.get()
            );
        } else {
            throw new RuntimeException("Unable to fetch directory id from meta properties file.");
        }
    }

    private static Optional<Uuid> getDirectoryId(String logDir) {
        Optional<Uuid> optionalDirectoryId;
        try {
            Properties metaProps = Utils.loadProps(logDir + "/" + META_PROPERTIES_NAME);
            if (metaProps.containsKey(DIRECTORY_ID_PROP)) {
                try {
                    optionalDirectoryId = Optional.of(Uuid.fromString(metaProps.getProperty(DIRECTORY_ID_PROP)));
                } catch (Exception e) {
                    throw new RuntimeException("Unable to read " + DIRECTORY_ID_PROP + " as a Uuid: " +
                        e.getMessage(), e);
                }
            } else {
                optionalDirectoryId = Optional.empty();
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to read meta properties file ", e);
        }
        return optionalDirectoryId;
    }

}
