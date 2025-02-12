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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.util.CommandLineUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (HelpScreenException e) {
            return 0;
        } catch (ArgumentParserException e) {
            e.getParser().handleError(e);
            return 1;
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
            .help("Property file containing configs to be passed to Admin Client. " +
                "For add-controller, the file is used to specify the controller properties as well.");
        Subparsers subparsers = parser.addSubparsers().dest("command");
        addDescribeSubParser(subparsers);
        addAddControllerSubParser(subparsers);
        addRemoveControllerSubParser(subparsers);

        Admin admin = null;
        try {
            Namespace namespace = parser.parseArgs(args);
            String command = namespace.getString("command");

            File optionalCommandConfig = namespace.get("command_config");
            final Properties props = getProperties(optionalCommandConfig);
            CommandLineUtils.initializeBootstrapProperties(props,
                Optional.ofNullable(namespace.getString("bootstrap_server")),
                Optional.ofNullable(namespace.getString("bootstrap_controller")));
            admin = Admin.create(props);

            if (command.equals("describe")) {
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
            } else if (command.equals("add-controller")) {
                if (optionalCommandConfig == null) {
                    throw new TerseException("You must supply the configuration file of the controller you are " +
                        "adding when using add-controller.");
                }
                handleAddController(admin,
                    namespace.getBoolean("dry_run"),
                    props);
            } else if (command.equals("remove-controller")) {
                handleRemoveController(admin,
                    namespace.getInt("controller_id"),
                    namespace.getString("controller_directory_id"),
                    namespace.getBoolean("dry_run"));
            } else {
                throw new IllegalStateException(format("Unknown command: %s", command));
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

    private static void addDescribeSubParser(Subparsers subparsers) {
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
            asList("NodeId", "DirectoryId", "LogEndOffset", "Lag", "LastFetchTimestamp", "LastCaughtUpTimestamp", "Status"),
            rows,
            System.out
        );
    }

    private static List<List<String>> quorumInfoToRows(QuorumInfo.ReplicaState leader,
                                                       Stream<QuorumInfo.ReplicaState> infos,
                                                       String status,
                                                       boolean humanReadable) {
        return infos.map(info -> {
            String lastFetchTimestamp = info.lastFetchTimestamp().isEmpty() ? "-1" :
                humanReadable ? format("%d ms ago", relativeTimeMs(info.lastFetchTimestamp().getAsLong(), "last fetch")) :
                    valueOf(info.lastFetchTimestamp().getAsLong());
            String lastCaughtUpTimestamp = info.lastCaughtUpTimestamp().isEmpty() ? "-1" :
                humanReadable ? format("%d ms ago", relativeTimeMs(info.lastCaughtUpTimestamp().getAsLong(), "last caught up")) :
                    valueOf(info.lastCaughtUpTimestamp().getAsLong());
            return Stream.of(
                info.replicaId(),
                info.replicaDirectoryId(),
                info.logEndOffset(),
                leader.logEndOffset() - info.logEndOffset(),
                lastFetchTimestamp,
                lastCaughtUpTimestamp,
                status
            ).map(r -> r.toString()).toList();
        }).toList();
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
            "\nCurrentVoters:          " + printVoterState(quorumInfo) +
            "\nCurrentObservers:       " + printObserverState(quorumInfo)
        );
    }

    // Constructs the CurrentVoters string
    // CurrentVoters: [{"id": 0, "directoryId": "UUID1", "endpoints": ["C://controller-0:1234"]}]
    private static String printVoterState(QuorumInfo quorumInfo) {
        return printReplicaState(quorumInfo, quorumInfo.voters());
    }

    // Constructs the CurrentObservers string
    private static String printObserverState(QuorumInfo quorumInfo) {
        return printReplicaState(quorumInfo, quorumInfo.observers());
    }

    private static String printReplicaState(QuorumInfo quorumInfo, List<QuorumInfo.ReplicaState> replicas) {
        List<Node> currentVoterList = replicas.stream().map(voter -> new Node(
            voter.replicaId(),
            voter.replicaDirectoryId(),
            getEndpoints(quorumInfo.nodes().get(voter.replicaId())))).toList();
        return currentVoterList.stream().map(Objects::toString).collect(Collectors.joining(", ", "[", "]"));
    }

    private static List<RaftVoterEndpoint> getEndpoints(QuorumInfo.Node node) {
        return node == null ? new ArrayList<>() : node.endpoints();
    }

    private static class Node {
        private final int id;
        private final Uuid directoryId;
        private final List<RaftVoterEndpoint> endpoints;

        private Node(int id, Uuid directoryId, List<RaftVoterEndpoint> endpoints) {
            this.id = id;
            this.directoryId = Objects.requireNonNull(directoryId);
            this.endpoints = Objects.requireNonNull(endpoints);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"id\": ").append(id).append(", ");
            sb.append("\"directoryId\": ").append(directoryId.equals(Uuid.ZERO_UUID) ? "null" : "\"" + directoryId + "\"");
            if (!endpoints.isEmpty()) {
                sb.append(", \"endpoints\": [");
                for (RaftVoterEndpoint endpoint : endpoints) {
                    sb.append("\"");
                    sb.append(endpoint.toString()).append("\", ");
                }
                sb.setLength(sb.length() - 2);  // remove the last comma and space
                sb.append("]");
            }
            sb.append("}");
            return sb.toString();
        }
    }

    private static void addAddControllerSubParser(Subparsers subparsers) {
        Subparser addControllerParser = subparsers
            .addParser("add-controller")
            .help("Add a controller to the KRaft controller cluster");

        addControllerParser
            .addArgument("--dry-run")
            .help("True if we should print what would be done, but not do it.")
            .action(Arguments.storeTrue());
    }

    static int getControllerId(Properties props) throws TerseException {
        if (!props.containsKey(KRaftConfigs.NODE_ID_CONFIG)) {
            throw new TerseException(KRaftConfigs.NODE_ID_CONFIG + " not found in configuration " +
                "file. Is this a valid controller configuration file?");
        }
        int nodeId = Integer.parseInt(props.getProperty(KRaftConfigs.NODE_ID_CONFIG));
        if (nodeId < 0) {
            throw new TerseException(KRaftConfigs.NODE_ID_CONFIG + " was negative in configuration " +
                "file. Is this a valid controller configuration file?");
        }
        if (!props.getOrDefault(KRaftConfigs.PROCESS_ROLES_CONFIG, "").toString().contains("controller")) {
            throw new TerseException(KRaftConfigs.PROCESS_ROLES_CONFIG + " did not contain 'controller' in " +
                "configuration file. Is this a valid controller configuration file?");
        }
        return nodeId;
    }

    static String getMetadataDirectory(Properties props) throws TerseException {
        if (props.containsKey(KRaftConfigs.METADATA_LOG_DIR_CONFIG)) {
            return props.getProperty(KRaftConfigs.METADATA_LOG_DIR_CONFIG);
        }
        if (props.containsKey(ServerLogConfigs.LOG_DIRS_CONFIG)) {
            String[] logDirs = props.getProperty(ServerLogConfigs.LOG_DIRS_CONFIG).trim().split(",");
            if (logDirs.length > 0) {
                return logDirs[0];
            }
        }
        throw new TerseException("Neither " + KRaftConfigs.METADATA_LOG_DIR_CONFIG + " nor " +
            ServerLogConfigs.LOG_DIRS_CONFIG + " were found. Is this a valid controller " +
            "configuration file?");
    }

    static Uuid getMetadataDirectoryId(String metadataDirectory) throws Exception {
        MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble.Loader().
            addLogDirs(Collections.singletonList(metadataDirectory)).
            addMetadataLogDir(metadataDirectory).
            load();
        MetaProperties metaProperties = ensemble.logDirProps().get(metadataDirectory);
        if (metaProperties == null) {
            throw new TerseException("Unable to read meta.properties from " + metadataDirectory);
        }
        if (metaProperties.directoryId().isEmpty()) {
            throw new TerseException("No directory id found in " + metadataDirectory);
        }
        return metaProperties.directoryId().get();
    }

    static Set<RaftVoterEndpoint> getControllerAdvertisedListeners(
        Properties props
    ) throws Exception {
        Map<String, Endpoint> listeners = new HashMap<>();
        SocketServerConfigs.listenerListToEndPoints(
            props.getOrDefault(SocketServerConfigs.LISTENERS_CONFIG, "").toString(),
            __ -> SecurityProtocol.PLAINTEXT).forEach(e -> listeners.put(e.listenerName().get(), e));
        SocketServerConfigs.listenerListToEndPoints(
            props.getOrDefault(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "").toString(),
            __ -> SecurityProtocol.PLAINTEXT).forEach(e -> listeners.put(e.listenerName().get(), e));
        if (!props.containsKey(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG)) {
            throw new TerseException(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG +
                " was not found. Is this a valid controller configuration file?");
        }
        LinkedHashSet<RaftVoterEndpoint> results = new LinkedHashSet<>();
        for (String listenerName : props.getProperty(
                KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG).split(",")) {
            listenerName = ListenerName.normalised(listenerName).value();
            Endpoint endpoint = listeners.get(listenerName);
            if (endpoint == null) {
                throw new TerseException("Cannot find information about controller listener name: " +
                    listenerName);
            }
            results.add(new RaftVoterEndpoint(endpoint.listenerName().get(),
                    endpoint.host() == null ? "localhost" : endpoint.host(),
                    endpoint.port()));
        }
        return results;
    }

    static void handleAddController(
        Admin admin,
        boolean dryRun,
        Properties props
    ) throws Exception {
        int controllerId = getControllerId(props);
        String metadataDirectory = getMetadataDirectory(props);
        Uuid directoryId = getMetadataDirectoryId(metadataDirectory);
        Set<RaftVoterEndpoint> endpoints = getControllerAdvertisedListeners(props);
        if (!dryRun) {
            admin.addRaftVoter(controllerId, directoryId, endpoints).
                all().get();
        }
        StringBuilder output = new StringBuilder();
        if (dryRun) {
            output.append("DRY RUN of adding");
        } else {
            output.append("Added");
        }
        output.append(" controller ").append(controllerId);
        output.append(" with directory id ").append(directoryId);
        output.append(" and endpoints: ");
        String prefix = "";
        for (RaftVoterEndpoint endpoint : endpoints) {
            output.append(prefix).append(endpoint.name()).append("://");
            if (endpoint.host().contains(":")) {
                output.append("[");
            }
            output.append(endpoint.host());
            if (endpoint.host().contains(":")) {
                output.append("]");
            }
            output.append(":").append(endpoint.port());
            prefix = ", ";
        }
        System.out.println(output);
    }

    private static void addRemoveControllerSubParser(Subparsers subparsers) {
        Subparser removeControllerParser = subparsers
            .addParser("remove-controller")
            .help("Remove a controller from the KRaft controller cluster");

        removeControllerParser
            .addArgument("--controller-id", "-i")
            .help("The id of the controller to remove.")
            .type(Integer.class)
            .required(true)
            .action(Arguments.store());

        removeControllerParser
            .addArgument("--controller-directory-id", "-d")
            .help("The directory ID of the controller to remove.")
            .required(true)
            .action(Arguments.store());

        removeControllerParser
            .addArgument("--dry-run")
            .help("True if we should print what would be done, but not do it.")
            .action(Arguments.storeTrue());
    }

    static void handleRemoveController(
        Admin admin,
        int controllerId,
        String controllerDirectoryIdString,
        boolean dryRun
    ) throws TerseException, ExecutionException, InterruptedException {
        if (controllerId < 0) {
            throw new TerseException("Invalid negative --controller-id: " + controllerId);
        }
        Uuid directoryId;
        try {
            directoryId = Uuid.fromString(controllerDirectoryIdString);
        } catch (IllegalArgumentException e) {
            throw new TerseException("Failed to parse --controller-directory-id: " + e.getMessage());
        }
        if (!dryRun) {
            admin.removeRaftVoter(controllerId, directoryId).
                all().get();
        }
        System.out.printf("%s KRaft controller %d with directory id %s%n",
            dryRun ? "DRY RUN of removing " : "Removed ",
            controllerId,
            directoryId);
    }
}
