/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools.reassign;

import joptsimple.OptionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.json.JsonValue;
import org.apache.kafka.tools.ToolsUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 *
 */
public class ReassignPartitionCommand {
    private static final String AnyLogDir = "any";

    private static final String helpText = "This tool helps to move topic partitions between replicas.";

    /**
     * The earliest version of the partition reassignment JSON.  We will default to this
     * version if no other version number is given.
     */
    public static final int EarliestVersion = 1;

    /**
     * The earliest version of the JSON for each partition reassignment topic.  We will
     * default to this version if no other version number is given.
     */
    public static final int EarliestTopicsJsonVersion = 1;

    // Throttles that are set at the level of an individual broker.
    //DynamicConfig.Broker.LeaderReplicationThrottledRateProp
    private static final String brokerLevelLeaderThrottle = "leader.replication.throttled.rate";
    //DynamicConfig.Broker.FollowerReplicationThrottledRateProp
    private static final String brokerLevelFollowerThrottle = "follower.replication.throttled.rate";
    //DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp
    private static final String brokerLevelLogDirThrottle = "replica.alter.log.dirs.io.max.bytes.per.second";
    private static final List<String> brokerLevelThrottles = Arrays.asList(
        brokerLevelLeaderThrottle,
        brokerLevelFollowerThrottle,
        brokerLevelLogDirThrottle
    );

    // Throttles that are set at the level of an individual topic.
    //LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG
    private static final String topicLevelLeaderThrottle = "leader.replication.throttled.replicas";
    //LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG
    private static final String topicLevelFollowerThrottle = "follower.replication.throttled.replicas";
    private static final List<String> topicLevelThrottles = Arrays.asList(
        topicLevelLeaderThrottle,
        topicLevelFollowerThrottle
    );

    private static final String cannotExecuteBecauseOfExistingMessage = "Cannot execute because " +
        "there is an existing partition assignment.  Use --additional to override this and " +
        "create a new partition assignment in addition to the existing one. The --additional " +
        "flag can also be used to change the throttle by resubmitting the current reassignment.";

    private static final String youMustRunVerifyPeriodicallyMessage = "Warning: You must run " +
        "--verify periodically, until the reassignment completes, to ensure the throttle " +
        "is removed.";

    public static void main(String[] args) {
        ReassignPartitionsCommandOptions opts = validateAndParseArgs(args);
        boolean failed = true;
        Admin adminClient = null;

        try {
            Properties props = opts.options.has(opts.commandConfigOpt)
                ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
                : new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
            props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "reassign-partitions-tool");
            adminClient = Admin.create(props);
            handleAction(adminClient, opts);
            failed = false;
        } catch (TerseReassignmentFailureException e) {
            System.out.println(e.getMessage());
        } catch (Throwable e) {
            System.out.println("Error: " + e.getMessage());
            System.out.println(Utils.stackTrace(e));
        } finally {
            // It's good to do this after printing any error stack trace.
            if (adminClient != null) {
                adminClient.close();
            }
        }
        // If the command failed, exit with a non-zero exit code.
        if (failed) {
            Exit.exit(1);
        }
    }

    private static void handleAction(Admin adminClient, ReassignPartitionsCommandOptions opts) throws IOException, ExecutionException, InterruptedException {
        if (opts.options.has(opts.verifyOpt)) {
            verifyAssignment(adminClient,
                Utils.readFileAsString(opts.options.valueOf(opts.reassignmentJsonFileOpt)),
                opts.options.has(opts.preserveThrottlesOpt));
        } else if (opts.options.has(opts.generateOpt)) {
            generateAssignment(adminClient,
                Utils.readFileAsString(opts.options.valueOf(opts.topicsToMoveJsonFileOpt)),
                opts.options.valueOf(opts.brokerListOpt),
                !opts.options.has(opts.disableRackAware));
        } else if (opts.options.has(opts.executeOpt)) {
            executeAssignment(adminClient,
                opts.options.has(opts.additionalOpt),
                Utils.readFileAsString(opts.options.valueOf(opts.reassignmentJsonFileOpt)),
                opts.options.valueOf(opts.interBrokerThrottleOpt),
                opts.options.valueOf(opts.replicaAlterLogDirsThrottleOpt),
                opts.options.valueOf(opts.timeoutOpt),
                Time.SYSTEM);
        } else if (opts.options.has(opts.cancelOpt)) {
            cancelAssignment(adminClient,
                Utils.readFileAsString(opts.options.valueOf(opts.reassignmentJsonFileOpt)),
                opts.options.has(opts.preserveThrottlesOpt),
                opts.options.valueOf(opts.timeoutOpt),
                Time.SYSTEM);
        } else if (opts.options.has(opts.listOpt)) {
            listReassignments(adminClient);
        } else {
            throw new RuntimeException("Unsupported action.");
        }
    }

    /**
     * The entry point for the --verify command.
     *
     * @param adminClient           The AdminClient to use.
     * @param jsonString            The JSON string to use for the topics and partitions to verify.
     * @param preserveThrottles     True if we should avoid changing topic or broker throttles.
     *
     * @return                      A result that is useful for testing.
     */
    private static VerifyAssignmentResult verifyAssignment(Admin adminClient, String jsonString, Boolean preserveThrottles) throws ExecutionException, InterruptedException {
        Tuple<List<Tuple<TopicPartition, List<Integer>>>, Map<TopicPartitionReplica, String>> t0 = parsePartitionReassignmentData(jsonString);

        List<Tuple<TopicPartition, List<Integer>>> targetParts = t0.v1;
        Map<TopicPartitionReplica, String> targetLogDirs = t0.v2;

        Tuple<Map<TopicPartition, PartitionReassignmentState>, Boolean> t1 = verifyPartitionAssignments(adminClient, targetParts);

        Map<TopicPartition, PartitionReassignmentState> partStates = t1.v1;
        Boolean partsOngoing = t1.v2;

        Tuple<Map<TopicPartitionReplica, LogDirMoveState>, Boolean> t2 = verifyReplicaMoves(adminClient, targetLogDirs);

        Map<TopicPartitionReplica, LogDirMoveState> moveStates = t2.v1;
        Boolean movesOngoing = t2.v2;

        if (!partsOngoing && !movesOngoing && !preserveThrottles) {
            // If the partition assignments and replica assignments are done, clear any throttles
            // that were set.  We have to clear all throttles, because we don't have enough
            // information to know all of the source brokers that might have been involved in the
            // previous reassignments.
            clearAllThrottles(adminClient, targetParts);
        }

        return new VerifyAssignmentResult(partStates, partsOngoing, moveStates, movesOngoing);
    }

    /**
     * Verify the partition reassignments specified by the user.
     *
     * @param adminClient           The AdminClient to use.
     * @param targets               The partition reassignments specified by the user.
     *
     * @return                      A tuple of the partition reassignment states, and a
     *                              boolean which is true if there are no ongoing
     *                              reassignments (including reassignments not described
     *                              in the JSON file.)
     */
    private static Tuple<Map<TopicPartition, PartitionReassignmentState>, Boolean> verifyPartitionAssignments(Admin adminClient,
                                                                                                              List<Tuple<TopicPartition, List<Integer>>> targets) throws ExecutionException, InterruptedException {
        Tuple<Map<TopicPartition, PartitionReassignmentState>, Boolean> t0 = findPartitionReassignmentStates(adminClient, targets);

        Map<TopicPartition, PartitionReassignmentState> partStates = t0.v1;
        Boolean partsOngoing = t0.v2;

        System.out.print(partitionReassignmentStatesToString(partStates));
        return new Tuple<>(partStates, partsOngoing);
    }

    private static int compareTopicPartitions(TopicPartition a, TopicPartition b) {
        int topicOrder = Objects.compare(a.topic(), b.topic(), String::compareTo);
        return topicOrder == 0 ? Integer.compare(a.partition(), b.partition()) : topicOrder;
    }

    private static int compareTopicPartitionReplicas(TopicPartitionReplica a, TopicPartitionReplica b) {
        int brokerOrder =  Integer.compare(a.brokerId(), b.brokerId());

        if (brokerOrder != 0)
            return brokerOrder;

        int topicOrder = Objects.compare(a.topic(), b.topic(), String::compareTo);
        return topicOrder == 0 ? Integer.compare(a.partition(), b.partition()) : topicOrder;
    }

    /**
     * Convert partition reassignment states to a human-readable string.
     *
     * @param states      A map from topic partitions to states.
     * @return            A string summarizing the partition reassignment states.
     */
    private static String partitionReassignmentStatesToString(Map<TopicPartition, PartitionReassignmentState> states) {
        List<String> bld = new ArrayList<>();
        bld.add("Status of partition reassignment:");
        states.keySet().stream().sorted(ReassignPartitionCommand::compareTopicPartitions).forEach(topicPartition -> {
            PartitionReassignmentState state = states.get(topicPartition);
            if (state.done) {
                if (state.currentReplicas.equals(state.targetReplicas)) {
                    bld.add(String.format("Reassignment of partition %s is completed.", topicPartition));
                } else {
                    String currentReplicaStr = state.currentReplicas.stream().map(String::valueOf).collect(Collectors.joining(","));
                    String targetReplicaStr = state.targetReplicas.stream().map(String::valueOf).collect(Collectors.joining(","));

                    bld.add("There is no active reassignment of partition " + topicPartition + ", " +
                        "but replica set is " + currentReplicaStr + " rather than " +
                        targetReplicaStr + ".");
                }
            } else {
                bld.add(String.format("Reassignment of partition %s is still in progress.", topicPartition));
            }
        });
        return bld.stream().collect(Collectors.joining(System.lineSeparator()));
    }

    /**
     * Find the state of the specified partition reassignments.
     *
     * @param adminClient          The Admin client to use.
     * @param targetReassignments  The reassignments we want to learn about.
     *
     * @return                     A tuple containing the reassignment states for each topic
     *                             partition, plus whether there are any ongoing reassignments.
     */
    private static Tuple<Map<TopicPartition, PartitionReassignmentState>, Boolean> findPartitionReassignmentStates(Admin adminClient,
                                                                                                                   List<Tuple<TopicPartition, List<Integer>>> targetReassignments) throws ExecutionException, InterruptedException {
        Map<TopicPartition, PartitionReassignment> currentReassignments = adminClient.
            listPartitionReassignments().reassignments().get();

        List<Tuple<TopicPartition, List<Integer>>> foundReassignments = new ArrayList<>();
        List<Tuple<TopicPartition, List<Integer>>> notFoundReassignments = new ArrayList<>();

        targetReassignments.forEach(reassignment -> {
            if (targetReassignments.contains(reassignment))
                foundReassignments.add(reassignment);
            else
                notFoundReassignments.add(reassignment);
        });

        List<Tuple<TopicPartition, PartitionReassignmentState>> foundResults = foundReassignments.stream().map(e -> {
            TopicPartition part = e.v1;
            List<Integer> targetReplicas = e.v2;
            return new Tuple<>(part,
                new PartitionReassignmentState(
                    currentReassignments.get(part).replicas(),
                    targetReplicas,
                    false));
        }).collect(Collectors.toList());

        Set<String> topicNamesToLookUp = new HashSet<>();
        notFoundReassignments.forEach(e -> {
            TopicPartition part = e.v1;
            if (!currentReassignments.containsKey(part))
                topicNamesToLookUp.add(part.topic());
        });

        Map<String, KafkaFuture<TopicDescription>> topicDescriptions = adminClient.
            describeTopics(topicNamesToLookUp).topicNameValues();

        List<Tuple<TopicPartition, PartitionReassignmentState>> notFoundResults = new ArrayList<>();
        for (Tuple<TopicPartition, List<Integer>> e : notFoundReassignments) {
            TopicPartition part = e.v1;
            List<Integer> targetReplicas = e.v2;

            if (currentReassignments.containsKey(part)) {
                PartitionReassignment reassignment = currentReassignments.get(part);
                notFoundResults.add(new Tuple<>(part, new PartitionReassignmentState(
                    reassignment.replicas(),
                    targetReplicas,
                    false)));
            } else {
                notFoundResults.add(new Tuple<>(part, topicDescriptionFutureToState(part.partition(),
                    topicDescriptions.get(part.topic()), targetReplicas)));
            }
        }

        Map<TopicPartition, PartitionReassignmentState> allResults = new HashMap<>();
        foundResults.forEach(e -> allResults.put(e.v1, e.v2));
        notFoundResults.forEach(e -> allResults.put(e.v1, e.v2));

        return new Tuple<>(allResults, currentReassignments.size() > 0);
    }

    private static PartitionReassignmentState topicDescriptionFutureToState(int partition,
                                                                            KafkaFuture<TopicDescription> future,
                                                                            List<Integer> targetReplicas) throws InterruptedException, ExecutionException {
        try {
            TopicDescription topicDescription = future.get();
            if (topicDescription.partitions().size() < partition) {
                throw new ExecutionException("Too few partitions found", new UnknownTopicOrPartitionException());
            }
            return new PartitionReassignmentState(
                topicDescription.partitions().get(partition).replicas().stream().map(Node::id).collect(Collectors.toList()),
                targetReplicas,
                true);
        } catch (ExecutionException t) {
            if (t.getCause() instanceof UnknownTopicOrPartitionException)
                return new PartitionReassignmentState(Collections.emptyList(), targetReplicas, true);

            throw t;
        }
    }

    /**
     * Verify the replica reassignments specified by the user.
     *
     * @param adminClient           The AdminClient to use.
     * @param targetReassignments   The replica reassignments specified by the user.
     *
     * @return                      A tuple of the replica states, and a boolean which is true
     *                              if there are any ongoing replica moves.
     *
     *                              Note: Unlike in verifyPartitionAssignments, we will
     *                              return false here even if there are unrelated ongoing
     *                              reassignments. (We don't have an efficient API that
     *                              returns all ongoing replica reassignments.)
     */
    private static Tuple<Map<TopicPartitionReplica, LogDirMoveState>, Boolean> verifyReplicaMoves(Admin adminClient,
                                                                                                  Map<TopicPartitionReplica, String> targetReassignments) throws ExecutionException, InterruptedException {
        Map<TopicPartitionReplica, LogDirMoveState> moveStates = findLogDirMoveStates(adminClient, targetReassignments);
        System.out.println(replicaMoveStatesToString(moveStates));
        return new Tuple<>(moveStates, moveStates.values().stream().noneMatch(LogDirMoveState::done));
    }

    /**
     * Find the state of the specified partition reassignments.
     *
     * @param adminClient           The AdminClient to use.
     * @param targetMoves           The movements we want to learn about.  The map is keyed
     *                              by TopicPartitionReplica, and its values are target log
     *                              directories.
     *
     * @return                      The states for each replica movement.
     */
    private static Map<TopicPartitionReplica, LogDirMoveState> findLogDirMoveStates(Admin adminClient,
                                                                                   Map<TopicPartitionReplica, String> targetMoves) throws ExecutionException, InterruptedException {
        Map<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> replicaLogDirInfos = adminClient.
            describeReplicaLogDirs(targetMoves.keySet()).all().get();

        Map<TopicPartitionReplica, LogDirMoveState> res = new HashMap<>();

        targetMoves.entrySet().forEach(e -> {
            TopicPartitionReplica replica = e.getKey();
            String targetLogDir = e.getValue();

            LogDirMoveState moveState;

            if (!replicaLogDirInfos.containsKey(replica)) {
                moveState = new MissingReplicaMoveState(targetLogDir);
            } else {
                DescribeReplicaLogDirsResult.ReplicaLogDirInfo info = replicaLogDirInfos.get(replica);
                if (info.getCurrentReplicaLogDir() == null){
                    moveState = new MissingLogDirMoveState(targetLogDir);
                } else if (info.getFutureReplicaLogDir() == null) {
                    if (info.getCurrentReplicaLogDir().equals(targetLogDir)) {
                        moveState = new CompletedMoveState(targetLogDir);
                    } else {
                        moveState = new CancelledMoveState(info.getCurrentReplicaLogDir(), targetLogDir);
                    }
                } else {
                    moveState = new ActiveMoveState(info.getCurrentReplicaLogDir(),
                        targetLogDir,
                        info.getFutureReplicaLogDir());
                }
            }
            res.put(replica, moveState);
        });

        return res;
    }

    /**
     * Convert replica move states to a human-readable string.
     *
     * @param states          A map from topic partition replicas to states.
     * @return                A tuple of a summary string, and a boolean describing
     *                        whether there are any active replica moves.
     */
    private static String replicaMoveStatesToString(Map<TopicPartitionReplica, LogDirMoveState> states) {
        List<String> bld = new ArrayList<>();
        states.keySet().stream().sorted(ReassignPartitionCommand::compareTopicPartitionReplicas).forEach(replica -> {
            LogDirMoveState state = states.get(replica);
            if (state instanceof MissingLogDirMoveState) {
                bld.add("Partition " + replica.topic() + "-" + replica.partition() + " is not found " +
                    "in any live log dir on broker " + replica.brokerId() + ". There is likely an " +
                    "offline log directory on the broker.");
            } else if (state instanceof MissingReplicaMoveState) {
                bld.add("Partition " + replica.topic() + "-" + replica.partition() + " cannot be found " +
                    "in any live log directory on broker " + replica.brokerId() + ".");
            } else if (state instanceof ActiveMoveState) {
                String targetLogDir = ((ActiveMoveState) state).targetLogDir;
                String futureLogDir = ((ActiveMoveState) state).futureLogDir;
                if (targetLogDir.equals(futureLogDir)) {
                    bld.add("Reassignment of replica " + replica + " is still in progress.");
                } else {
                    bld.add("Partition " + replica.topic() + "-" + replica.partition() + " on broker " +
                        replica.brokerId() + " is being moved to log dir " + futureLogDir + " " +
                        "instead of " + targetLogDir + ".");
                }
            } else if (state instanceof CancelledMoveState) {
                String targetLogDir = ((CancelledMoveState) state).targetLogDir;
                String currentLogDir = ((CancelledMoveState) state).currentLogDir;
                bld.add("Partition " + replica.topic() + "-" + replica.partition() + " on broker " +
                    replica.brokerId() + " is not being moved from log dir " + currentLogDir + " to " +
                    targetLogDir + ".");
            } else if (state instanceof CompletedMoveState) {
                bld.add("Reassignment of replica " + replica + " completed successfully.");
            }
        });

        return bld.stream().collect(Collectors.joining(System.lineSeparator()));
    }

    /**
     * Clear all topic-level and broker-level throttles.
     *
     * @param adminClient     The AdminClient to use.
     * @param targetParts     The target partitions loaded from the JSON file.
     */
    private static void clearAllThrottles(Admin adminClient, List<Tuple<TopicPartition, List<Integer>>> targetParts) throws ExecutionException, InterruptedException {
        Set<Integer> activeBrokers = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toSet());
        Set<Integer> brokers = new HashSet<>(activeBrokers);
        targetParts.forEach(t -> brokers.addAll(t.v2));

        System.out.printf("Clearing broker-level throttles on broker%s %s%n",
            brokers.size() == 1 ? "" : "s", brokers.stream().map(Object::toString).collect(Collectors.joining(",")));
        clearBrokerLevelThrottles(adminClient, brokers);

        Set<String> topics = targetParts.stream().map(t -> t.v1.topic()).collect(Collectors.toSet());
        System.out.printf("Clearing topic-level throttles on topic%s %s%n",
            topics.size() == 1 ? "" : "s", topics.stream().map(Object::toString).collect(Collectors.joining(",")));
        clearTopicLevelThrottles(adminClient, topics);
    }

    /**
     * Clear all throttles which have been set at the broker level.
     *
     * @param adminClient       The AdminClient to use.
     * @param brokers           The brokers to clear the throttles for.
     */
    private static void clearBrokerLevelThrottles(Admin adminClient, Set<Integer> brokers) throws ExecutionException, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = new HashMap<>();
        brokers.forEach(brokerId -> configOps.put(
            new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString()),
            brokerLevelThrottles.stream().map(throttle -> new AlterConfigOp(
                new ConfigEntry(throttle, null), AlterConfigOp.OpType.DELETE)).collect(Collectors.toList())
        ));
        adminClient.incrementalAlterConfigs(configOps).all().get();
    }

    /**
     * Clear the reassignment throttles for the specified topics.
     *
     * @param adminClient           The AdminClient to use.
     * @param topics                The topics to clear the throttles for.
     */
    private static void clearTopicLevelThrottles(Admin adminClient, Set<String> topics) throws ExecutionException, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = new HashMap<>();
        topics.forEach(topicName -> configOps.put(
            new ConfigResource(ConfigResource.Type.TOPIC, topicName),
            topicLevelThrottles.stream().map(throttle -> new AlterConfigOp(new ConfigEntry(throttle, null),
                AlterConfigOp.OpType.DELETE)).collect(Collectors.toList())

        ));
        adminClient.incrementalAlterConfigs(configOps).all().get();
    }

    /**
     * The entry point for the --generate command.
     *
     * @param adminClient           The AdminClient to use.
     * @param reassignmentJson      The JSON string to use for the topics to reassign.
     * @param brokerListString      The comma-separated string of broker IDs to use.
     * @param enableRackAwareness   True if rack-awareness should be enabled.
     *
     * @return                      A tuple containing the proposed assignment and the
     *                              current assignment.
     */
    private static Tuple<Map<TopicPartition, List<Integer>>, Map<TopicPartition, List<Integer>>> generateAssignment(Admin adminClient,
                                                                                                                    String reassignmentJson,
                                                                                                                    String brokerListString,
                                                                                                                    Boolean enableRackAwareness) throws ExecutionException, InterruptedException {
        Tuple<List<Integer>, List<String>> t0 = parseGenerateAssignmentArgs(reassignmentJson, brokerListString);

        List<Integer> brokersToReassign = t0.v1;
        List<String> topicsToReassign = t0.v2;

        Map<TopicPartition, List<Integer>> currentAssignments = getReplicaAssignmentForTopics(adminClient, topicsToReassign);
        List<BrokerMetadata> brokerMetadatas = getBrokerMetadata(adminClient, brokersToReassign, enableRackAwareness);
        Map<TopicPartition, List<Integer>> proposedAssignments = calculateAssignment(currentAssignments, brokerMetadatas);
        System.out.printf("Current partition replica assignment\n%s\n%n",
            formatAsReassignmentJson(currentAssignments, Collections.emptyMap()));
        System.out.printf("Proposed partition reassignment configuration\n%s",
            formatAsReassignmentJson(proposedAssignments, Collections.emptyMap()));
        return new Tuple<>(proposedAssignments, currentAssignments);
    }

    /**
     * Calculate the new partition assignments to suggest in --generate.
     *
     * @param currentAssignment  The current partition assignments.
     * @param brokerMetadatas    The rack information for each broker.
     *
     * @return                   A map from partitions to the proposed assignments for each.
     */
    private static Map<TopicPartition, List<Integer>> calculateAssignment(Map<TopicPartition, List<Integer>> currentAssignment,
                                                                          List<BrokerMetadata> brokerMetadatas) {
        Map<String, List<Map.Entry<TopicPartition, List<Integer>>>> groupedByTopic = new HashMap<>();
        for (Map.Entry<TopicPartition, List<Integer>> e : currentAssignment.entrySet())
            groupedByTopic.computeIfAbsent(e.getKey().topic(), k -> new ArrayList<>()).add(e);
        HashMap<TopicPartition, List<Integer>> proposedAssignments = new HashMap<>();
        groupedByTopic.forEach((topic, assignment) -> {
            List<Integer> replicas = assignment.get(0).getValue();
            Map<Integer, List<Integer>> assignedReplicas = null/*AdminUtils.
                assignReplicasToBrokers(brokerMetadatas, assignment.size(), replicas.size())*/;
            assignedReplicas.forEach((partition, replicas0) ->
                proposedAssignments.put(new TopicPartition(topic, partition), replicas0));
        });
        return proposedAssignments;
    }

    private static Map<String, TopicDescription> describeTopics(Admin adminClient,
                                       Set<String> topics) throws ExecutionException, InterruptedException {
        Map<String, KafkaFuture<TopicDescription>> futures = adminClient.describeTopics(topics).topicNameValues();
        Map<String, TopicDescription> res = new HashMap<>();
        for (Map.Entry<String, KafkaFuture<TopicDescription>> e : futures.entrySet()) {
            String topicName = e.getKey();
            KafkaFuture<TopicDescription> topicDescriptionFuture = e.getValue();
            try {
                res.put(topicName, topicDescriptionFuture.get());
            } catch (ExecutionException t) {
                if (t.getCause() instanceof UnknownTopicOrPartitionException)
                    throw new ExecutionException(
                        new UnknownTopicOrPartitionException("Topic " + topicName + " not found."));
                throw t;
            }
        }
        return res;
    }

    /**
     * Get the current replica assignments for some topics.
     *
     * @param adminClient     The AdminClient to use.
     * @param topics          The topics to get information about.
     * @return                A map from partitions to broker assignments.
     *                        If any topic can't be found, an exception will be thrown.
     */
    private static Map<TopicPartition, List<Integer>> getReplicaAssignmentForTopics(Admin adminClient,
                                                                                    List<String> topics) throws ExecutionException, InterruptedException {

        Map<TopicPartition, List<Integer>> res = new HashMap<>();
        describeTopics(adminClient, new HashSet<>(topics)).forEach((topicName, topicDescription) ->
            topicDescription.partitions().forEach(info -> res.put(
                new TopicPartition(topicName, info.partition()),
                info.replicas().stream().map(Node::id).collect(Collectors.toList())
            )
        ));
        return res;
    }

    /**
     * Get the current replica assignments for some partitions.
     *
     * @param adminClient     The AdminClient to use.
     * @param partitions      The partitions to get information about.
     * @return                A map from partitions to broker assignments.
     *                        If any topic can't be found, an exception will be thrown.
     */
    private static Map<TopicPartition, List<Integer>> getReplicaAssignmentForPartitions(Admin adminClient,
                                                                                        Set<TopicPartition> partitions) throws ExecutionException, InterruptedException {
        Map<TopicPartition, List<Integer>> res = new HashMap<>();
        describeTopics(adminClient, partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet())).forEach((topicName, topicDescription) -> {
            topicDescription.partitions().forEach(info -> {
                TopicPartition tp = new TopicPartition(topicName, info.partition());
                if (partitions.contains(tp))
                    res.put(tp, info.replicas().stream().map(Node::id).collect(Collectors.toList()));
            });
        });
        return res;
    }

    /**
     * Find the rack information for some brokers.
     *
     * @param adminClient         The AdminClient object.
     * @param brokers             The brokers to gather metadata about.
     * @param enableRackAwareness True if we should return rack information, and throw an
     *                            exception if it is inconsistent.
     *
     * @return                    The metadata for each broker that was found.
     *                            Brokers that were not found will be omitted.
     */
    private static List<BrokerMetadata> getBrokerMetadata(Admin adminClient, List<Integer> brokers, boolean enableRackAwareness) throws ExecutionException, InterruptedException {
        Set<Integer> brokerSet = new HashSet<>(brokers);
        List<BrokerMetadata> results = adminClient.describeCluster().nodes().get().stream().
            filter(node -> brokerSet.contains(node.id())).
            map(node -> (enableRackAwareness && node.rack() != null)
                ? new BrokerMetadata(node.id(), Optional.of(node.rack()))
                : new BrokerMetadata(node.id(), Optional.empty())).collect(Collectors.toList());

        long numRackless = results.stream().filter(m -> !m.rack.isPresent()).count();
        if (enableRackAwareness && numRackless != 0 && numRackless != results.size()) {
            throw new AdminOperationException("Not all brokers have rack information. Add " +
                "--disable-rack-aware in command line to make replica assignment without rack " +
                "information.");
        }
        return results;
    }

    /**
     * Parse and validate data gathered from the command-line for --generate
     * In particular, we parse the JSON and validate that duplicate brokers and
     * topics don't appear.
     *
     * @param reassignmentJson       The JSON passed to --generate .
     * @param brokerList             A list of brokers passed to --generate.
     *
     * @return                       A tuple of brokers to reassign, topics to reassign
     */
    private static Tuple<List<Integer>, List<String>> parseGenerateAssignmentArgs(String reassignmentJson,
                                                                                  String brokerList) {
        List<Integer> brokerListToReassign = Arrays.stream(brokerList.split(",")).map(Integer::parseInt).collect(Collectors.toList());
        Set<Integer> duplicateReassignments = ToolsUtils.duplicates(brokerListToReassign);
        if (!duplicateReassignments.isEmpty())
            throw new AdminCommandFailedException(String.format("Broker list contains duplicate entries: %s", duplicateReassignments));
        List<String> topicsToReassign = parseTopicsData(reassignmentJson);
        Set<String> duplicateTopicsToReassign = ToolsUtils.duplicates(topicsToReassign);
        if (!duplicateTopicsToReassign.isEmpty())
            throw new AdminCommandFailedException(String.format("List of topics to reassign contains duplicate entries: %s",
                duplicateTopicsToReassign));
        return new Tuple<>(brokerListToReassign, topicsToReassign);
    }

    /**
     * The entry point for the --execute and --execute-additional commands.
     *
     * @param adminClient                 The AdminClient to use.
     * @param additional                  Whether --additional was passed.
     * @param reassignmentJson            The JSON string to use for the topics to reassign.
     * @param interBrokerThrottle         The inter-broker throttle to use, or a negative
     *                                    number to skip using a throttle.
     * @param logDirThrottle              The replica log directory throttle to use, or a
     *                                    negative number to skip using a throttle.
     * @param timeoutMs                   The maximum time in ms to wait for log directory
     *                                    replica assignment to begin.
     * @param time                        The Time object to use.
     */
    private static void executeAssignment(Admin adminClient,
                                          Boolean additional,
                                          String reassignmentJson,
                                          Long interBrokerThrottle,
                                          Long logDirThrottle,
                                          Long timeoutMs,
                                          Time time) {

    }

    /**
     * Execute some partition log directory movements.
     *
     * @param adminClient                 The AdminClient to use.
     * @param proposedReplicas            A map from TopicPartitionReplicas to the
     *                                    directories to move them to.
     * @param timeoutMs                   The maximum time in ms to wait for log directory
     *                                    replica assignment to begin.
     * @param time                        The Time object to use.
     */
    private static void executeMoves(Admin adminClient,
                                     Map<TopicPartitionReplica, String> proposedReplicas,
                                     Long timeoutMs,
                                     Time time) {

    }

    /**
     * Entry point for the --list command.
     *
     * @param adminClient   The AdminClient to use.
     */
    private static void listReassignments(Admin adminClient) {

    }

    /**
     * Convert the current partition reassignments to text.
     *
     * @param adminClient   The AdminClient to use.
     * @return              A string describing the current partition reassignments.
     */
    private static String curReassignmentsToString(Admin adminClient) {
        return null;
    }

    /**
     * Verify that all the brokers in an assignment exist.
     *
     * @param adminClient                 The AdminClient to use.
     * @param brokers                     The broker IDs to verify.
     */
    private static void verifyBrokerIds(Admin adminClient, Set<Integer> brokers) {

    }

    /**
     * Return the string which we want to print to describe the current partition assignment.
     *
     * @param proposedParts               The proposed partition assignment.
     * @param currentParts                The current partition assignment.
     *
     * @return                            The string to print.  We will only print information about
     *                                    partitions that appear in the proposed partition assignment.
     */
    private static String currentPartitionReplicaAssignmentToString(Map<TopicPartition, List<Integer>> proposedParts,
                                                                    Map<TopicPartition, List<Integer>> currentParts) {
        return null;
    }

    /**
     * Execute the given partition reassignments.
     *
     * @param adminClient       The admin client object to use.
     * @param reassignments     A map from topic names to target replica assignments.
     * @return                  A map from partition objects to error strings.
     */
    private static Map<TopicPartition, Throwable> alterPartitionReassignments(Admin adminClient,
                                                                              Map<TopicPartition, List<Integer>> reassignments) {
        return null;
    }

    /**
     * Cancel the given partition reassignments.
     *
     * @param adminClient       The admin client object to use.
     * @param reassignments     The partition reassignments to cancel.
     * @return                  A map from partition objects to error strings.
     */
    private static Map<TopicPartition, Throwable> cancelPartitionReassignments(Admin adminClient,
                                                                               Set<TopicPartition> reassignments) {
        return null;
    }

    /**
     * Compute the in progress partition move from the current reassignments.
     * @param currentReassignments All replicas, adding replicas and removing replicas of target partitions
     */
    private static Map<String, Map<Integer, PartitionMove>> calculateCurrentMoveMap(Map<TopicPartition, PartitionReassignment> currentReassignments) {
        return null;
    }

    /**
     * Calculate the global map of all partitions that are moving.
     *
     * @param currentReassignments    The currently active reassignments.
     * @param proposedParts           The proposed location of the partitions (destinations replicas only).
     * @param currentParts            The current location of the partitions that we are
     *                                proposing to move.
     * @return                        A map from topic name to partition map.
     *                                The partition map is keyed on partition index and contains
     *                                the movements for that partition.
     */
    private static Map<String, Map<Integer, PartitionMove>> calculateProposedMoveMap(Map<TopicPartition, PartitionReassignment> currentReassignments,
                                 Map<TopicPartition, List<Integer>> proposedParts,
                                 Map<TopicPartition, List<Integer>> currentParts) {
        return null;
    }

    /**
     * Calculate the leader throttle configurations to use.
     *
     * @param moveMap   The movements.
     * @return          A map from topic names to leader throttle configurations.
     */
    private static Map<String, String> calculateLeaderThrottles(Map<String, Map<Integer, PartitionMove>> moveMap) {
        return null;
    }

    /**
     * Calculate the follower throttle configurations to use.
     *
     * @param moveMap   The movements.
     * @return          A map from topic names to follower throttle configurations.
     */
    private static Map<String, String> calculateFollowerThrottles(Map<String, Map<Integer, PartitionMove>> moveMap) {
        return null;
    }

    /**
     * Calculate all the brokers which are involved in the given partition reassignments.
     *
     * @param moveMap       The partition movements.
     * @return              A set of all the brokers involved.
     */
    private static List<Integer> calculateReassigningBrokers(Map<String, Map<Integer, PartitionMove>> moveMap) {
        return null;
    }

    /**
     * Calculate all the brokers which are involved in the given directory movements.
     *
     * @param replicaMoves  The replica movements.
     * @return              A set of all the brokers involved.
     */
    private static Set<Integer> calculateMovingBrokers(Set<TopicPartitionReplica> replicaMoves) {
        return null;
    }

    /**
     * Modify the topic configurations that control inter-broker throttling.
     *
     * @param adminClient         The adminClient object to use.
     * @param leaderThrottles     A map from topic names to leader throttle configurations.
     * @param followerThrottles   A map from topic names to follower throttle configurations.
     */
    private static void modifyTopicThrottles(Admin adminClient,
                                             Map<String, String> leaderThrottles,
                                             Map<String, String> followerThrottles) {
    }

    private static void modifyReassignmentThrottle(Admin admin, Map<String, Map<Integer, PartitionMove>> moveMap, Long interBrokerThrottle) {

    }

    /**
     * Modify the leader/follower replication throttles for a set of brokers.
     *
     * @param adminClient The Admin instance to use
     * @param reassigningBrokers The set of brokers involved in the reassignment
     * @param interBrokerThrottle The new throttle (ignored if less than 0)
     */
    private static void modifyInterBrokerThrottle(Admin adminClient,
                                                  Set<Integer> reassigningBrokers,
                                                  Long interBrokerThrottle) {

    }

    /**
     * Modify the log dir reassignment throttle for a set of brokers.
     *
     * @param admin The Admin instance to use
     * @param movingBrokers The set of broker to alter the throttle of
     * @param logDirThrottle The new throttle (ignored if less than 0)
     */
    private static void modifyLogDirThrottle(Admin admin,
                                             Set<Integer> movingBrokers,
                                             Long logDirThrottle) {

    }

    /**
     * Parse the reassignment JSON string passed to the --execute command.
     *
     * @param reassignmentJson  The JSON string.
     * @return                  A tuple of the partitions to be reassigned and the replicas
     *                          to be reassigned.
     */
    private static Tuple<Map<TopicPartition, List<Integer>>, Map<TopicPartitionReplica, String>> parseExecuteAssignmentArgs(String reassignmentJson) {
        return null;
    }

    /**
     * The entry point for the --cancel command.
     *
     * @param adminClient           The AdminClient to use.
     * @param jsonString            The JSON string to use for the topics and partitions to cancel.
     * @param preserveThrottles     True if we should avoid changing topic or broker throttles.
     * @param timeoutMs             The maximum time in ms to wait for log directory
     *                              replica assignment to begin.
     * @param time                  The Time object to use.
     *
     * @return                      A tuple of the partition reassignments that were cancelled,
     *                              and the replica movements that were cancelled.
     */
    private static Tuple<Set<TopicPartition>, Set<TopicPartitionReplica>> cancelAssignment(Admin adminClient,
                                                                                           String jsonString,
                                                                                           Boolean preserveThrottles,
                                                                                           Long timeoutMs,
                                                                                           Time time) {
        return null;
    }

    private static String formatAsReassignmentJson(Map<TopicPartition, List<Integer>> partitionsToBeReassigned,
                                                   Map<TopicPartitionReplica, String> replicaLogDirAssignment) {
        return null;
    }

    private static List<String> parseTopicsData(String jsonData) {
        return null;
    }

    private static List<String> parseTopicsData(int version, JsonValue js) {
        return null;
    }

    private static Tuple<List<Tuple<TopicPartition, List<Integer>>>, Map<TopicPartitionReplica, String>> parsePartitionReassignmentData(String jsonData) {
        return null;
    }

    // Parses without deduplicating keys so the data can be checked before allowing reassignment to proceed
    private static Tuple<List<Tuple<TopicPartition, List<Integer>>>, Map<TopicPartitionReplica, String>> parsePartitionReassignmentData(int version, JsonValue jsonData) {
        return null;
    }


    private static ReassignPartitionsCommandOptions validateAndParseArgs(String[] args) {
        ReassignPartitionsCommandOptions opts = new ReassignPartitionsCommandOptions(args);

        CommandLineUtils.maybePrintHelpOrVersion(opts, helpText);

        // Determine which action we should perform.
        List<OptionSpec<?>> validActions = Arrays.asList(opts.generateOpt, opts.executeOpt, opts.verifyOpt,
            opts.cancelOpt, opts.listOpt);

        List<OptionSpec<?>> allActions = validActions.stream()
            .filter(a -> opts.options.has(a))
            .collect(Collectors.toList());

        if (allActions.size() != 1) {
            CommandLineUtils.printUsageAndExit(opts.parser, String.format("Command must include exactly one action: %s",
                validActions.stream().map(a -> "--" + a.options().get(0)).collect(Collectors.joining(", "))));
        }

        OptionSpec<?> action = allActions.get(0);

        if (!opts.options.has(opts.bootstrapServerOpt))
            CommandLineUtils.printUsageAndExit(opts.parser, "Please specify --bootstrap-server");

        // Make sure that we have all the required arguments for our action.
        Map<OptionSpec<?>, List<OptionSpec<?>>> requiredArgs = new HashMap<>();

        requiredArgs.put(opts.verifyOpt, Collections.singletonList(
            opts.reassignmentJsonFileOpt
        ));
        requiredArgs.put(opts.generateOpt, Arrays.asList(
            opts.topicsToMoveJsonFileOpt,
            opts.brokerListOpt
        ));
        requiredArgs.put(opts.executeOpt, Collections.singletonList(
            opts.reassignmentJsonFileOpt
        ));
        requiredArgs.put(opts.cancelOpt, Collections.singletonList(
            opts.reassignmentJsonFileOpt
        ));
        requiredArgs.put(opts.listOpt, Collections.emptyList());

        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, requiredArgs.get(action).toArray(new OptionSpec[0]));

        Map<OptionSpec<?>, List<OptionSpec<?>>> permittedArgs = new HashMap<>();

        permittedArgs.put(opts.verifyOpt, Arrays.asList(
            opts.bootstrapServerOpt,
            opts.commandConfigOpt,
            opts.preserveThrottlesOpt
        ));
        permittedArgs.put(opts.generateOpt, Arrays.asList(
            opts.bootstrapServerOpt,
            opts.brokerListOpt,
            opts.commandConfigOpt,
            opts.disableRackAware
        ));
        permittedArgs.put(opts.executeOpt, Arrays.asList(
            opts.additionalOpt,
            opts.bootstrapServerOpt,
            opts.commandConfigOpt,
            opts.interBrokerThrottleOpt,
            opts.replicaAlterLogDirsThrottleOpt,
            opts.timeoutOpt
        ));
        permittedArgs.put(opts.cancelOpt, Arrays.asList(
            opts.bootstrapServerOpt,
            opts.commandConfigOpt,
            opts.preserveThrottlesOpt,
            opts.timeoutOpt
        ));
        permittedArgs.put(opts.listOpt, Arrays.asList(
            opts.bootstrapServerOpt,
            opts.commandConfigOpt
        ));

        opts.options.specs().forEach(opt -> {
            if (!opt.equals(action) &&
                !requiredArgs.getOrDefault(action, Collections.emptyList()).contains(opt) &&
                !permittedArgs.getOrDefault(action, Collections.emptyList()).contains(opt)) {
                CommandLineUtils.printUsageAndExit(opts.parser,
                    String.format("Option \"%s\" can't be used with action \"%s\"", opt, action));
            }
        });

        return opts;
    }

    private static Set<TopicPartitionReplica> alterReplicaLogDirs(Admin adminClient, Map<TopicPartitionReplica, String> assignment) {
        return null;
    }
}
