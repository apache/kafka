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
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.ToolsUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        parser
            .addArgument("--bootstrap-server")
            .help("A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.")
            .required(true);

        parser
            .addArgument("--command-config")
            .type(Arguments.fileType())
            .help("Property file containing configs to be passed to Admin Client.");
        Subparsers subparsers = parser.addSubparsers().dest("command");
        addDescribeParser(subparsers);

        Admin admin = null;
        try {
            Namespace namespace = parser.parseArgsOrFail(args);
            String command = namespace.getString("command");

            File commandConfig = namespace.get("command_config");
            Properties props = new Properties();
            if (commandConfig != null) {
                if (!commandConfig.exists())
                    throw new TerseException("Properties file " + commandConfig.getPath() + " does not exists!");

                Utils.loadProps(commandConfig.getPath());
            }
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, namespace.getString("bootstrap_server"));
            admin = Admin.create(props);

            if (command.equals("describe")) {
                if (namespace.getBoolean("status") && namespace.getBoolean("replication")) {
                    throw new TerseException("Only one of --status or --replication should be specified with describe sub-command");
                } else if (namespace.getBoolean("replication")) {
                    handleDescribeReplication(admin);
                } else if (namespace.getBoolean("status")) {
                    handleDescribeStatus(admin);
                } else {
                    throw new TerseException("One of --status or --replication must be specified with describe sub-command");
                }
            } else {
                throw new IllegalStateException("Unknown command: " + command + ", only 'describe' is supported");
            }
        } finally {
            if (admin != null)
                admin.close();
        }
    }

    private static void addDescribeParser(Subparsers subparsers) {
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
    }

    private static void handleDescribeReplication(Admin admin) throws ExecutionException, InterruptedException {
        QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
        int leaderId = quorumInfo.leaderId();
        QuorumInfo.ReplicaState leader = quorumInfo.voters().stream().filter(voter -> voter.replicaId() == leaderId).findFirst().get();

        List<List<String>> rows = new ArrayList<>();
        rows.addAll(quorumInfoToRows(leader, Stream.of(leader), "Leader"));
        rows.addAll(quorumInfoToRows(leader, quorumInfo.voters().stream().filter(v -> v.replicaId() != leaderId), "Follower"));
        rows.addAll(quorumInfoToRows(leader, quorumInfo.observers().stream(), "Observer"));

        ToolsUtils.prettyPrintTable(
            asList("NodeId", "LogEndOffset", "Lag", "LastFetchTimestamp", "LastCaughtUpTimestamp", "Status"),
            rows,
            System.out
        );
    }

    private static List<List<String>> quorumInfoToRows(QuorumInfo.ReplicaState leader, Stream<QuorumInfo.ReplicaState> infos, String status) {
        return infos.map(info ->
            Stream.of(
                info.replicaId(),
                info.logEndOffset(),
                leader.logEndOffset() - info.logEndOffset(),
                info.lastFetchTimestamp().orElse(-1),
                info.lastCaughtUpTimestamp().orElse(-1),
                status
            ).map(r -> r.toString()).collect(Collectors.toList())
        ).collect(Collectors.toList());
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
            "\nCurrentVoters:          " + Utils.mkString(quorumInfo.voters().stream().map(v -> v.replicaId()), "[", "]", ",") +
            "\nCurrentObservers:       " + Utils.mkString(quorumInfo.observers().stream().map(v -> v.replicaId()), "[", "]", ",")
        );
    }

}
