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
package org.apache.kafka.tools.reassign;

import org.apache.kafka.admin.AdminUtils;
import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.Json;
import org.apache.kafka.server.util.json.DecodeJson;
import org.apache.kafka.server.util.json.JsonObject;
import org.apache.kafka.server.util.json.JsonValue;
import org.apache.kafka.tools.TerseException;
import org.apache.kafka.tools.ToolsUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import joptsimple.OptionSpec;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class ReassignPartitionsCommand {
    private static final String ANY_LOG_DIR = "any";

    static final String HELP_TEXT = "This tool helps to move topic partitions between replicas.";

    private static final DecodeJson.DecodeInteger INT = new DecodeJson.DecodeInteger();

    private static final DecodeJson.DecodeString STRING = new DecodeJson.DecodeString();

    private static final DecodeJson<List<Integer>> INT_LIST = DecodeJson.decodeList(INT);

    private static final DecodeJson<List<String>> STRING_LIST = DecodeJson.decodeList(STRING);

    /**
     * The earliest version of the partition reassignment JSON.  We will default to this
     * version if no other version number is given.
     */
    static final int EARLIEST_VERSION = 1;

    /**
     * The earliest version of the JSON for each partition reassignment topic.  We will
     * default to this version if no other version number is given.
     */
    static final int EARLIEST_TOPICS_JSON_VERSION = 1;

    static final List<String> BROKER_LEVEL_THROTTLES = Arrays.asList(
            QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
            QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
            QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG
    );

    private static final List<String> TOPIC_LEVEL_THROTTLES = Arrays.asList(
            QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
            QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG
    );

    private static final String CANNOT_EXECUTE_BECAUSE_OF_EXISTING_MESSAGE = "Cannot execute because " +
        "there is an existing partition assignment.  Use --additional to override this and " +
        "create a new partition assignment in addition to the existing one. The --additional " +
        "flag can also be used to change the throttle by resubmitting the current reassignment.";

    private static final String YOU_MUST_RUN_VERIFY_PERIODICALLY_MESSAGE = "Warning: You must run " +
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
            if (opts.options.has(opts.bootstrapControllerOpt)) {
                props.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, opts.options.valueOf(opts.bootstrapControllerOpt));
            } else {
                props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
            }
            props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "reassign-partitions-tool");
            adminClient = Admin.create(props);
            handleAction(adminClient, opts);
            failed = false;
        } catch (TerseException e) {
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

    private static void handleAction(Admin adminClient, ReassignPartitionsCommandOptions opts) throws IOException, ExecutionException, InterruptedException, TerseException {
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
    static VerifyAssignmentResult verifyAssignment(Admin adminClient,
                                                   String jsonString,
                                                   Boolean preserveThrottles
    ) throws ExecutionException, InterruptedException, JsonProcessingException {
        Entry<List<Entry<TopicPartition, List<Integer>>>, Map<TopicPartitionReplica, String>> t0 = parsePartitionReassignmentData(jsonString);

        List<Entry<TopicPartition, List<Integer>>> targetParts = t0.getKey();
        Map<TopicPartitionReplica, String> targetLogDirs = t0.getValue();

        Entry<Map<TopicPartition, PartitionReassignmentState>, Boolean> t1 = verifyPartitionAssignments(adminClient, targetParts);

        Map<TopicPartition, PartitionReassignmentState> partStates = t1.getKey();
        Boolean partsOngoing = t1.getValue();

        Entry<Map<TopicPartitionReplica, LogDirMoveState>, Boolean> t2 = verifyReplicaMoves(adminClient, targetLogDirs);

        Map<TopicPartitionReplica, LogDirMoveState> moveStates = t2.getKey();
        Boolean movesOngoing = t2.getValue();

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
    private static Entry<Map<TopicPartition, PartitionReassignmentState>, Boolean> verifyPartitionAssignments(Admin adminClient,
                                                                                                               List<Entry<TopicPartition, List<Integer>>> targets
    ) throws ExecutionException, InterruptedException {
        Entry<Map<TopicPartition, PartitionReassignmentState>, Boolean> t0 = findPartitionReassignmentStates(adminClient, targets);
        System.out.println(partitionReassignmentStatesToString(t0.getKey()));
        return t0;
    }

    static int compareTopicPartitions(TopicPartition a, TopicPartition b) {
        int topicOrder = Objects.compare(a.topic(), b.topic(), String::compareTo);
        return topicOrder == 0 ? Integer.compare(a.partition(), b.partition()) : topicOrder;
    }

    static int compareTopicPartitionReplicas(TopicPartitionReplica a, TopicPartitionReplica b) {
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
    static String partitionReassignmentStatesToString(Map<TopicPartition, PartitionReassignmentState> states) {
        List<String> bld = new ArrayList<>();
        bld.add("Status of partition reassignment:");
        states.keySet().stream().sorted(ReassignPartitionsCommand::compareTopicPartitions).forEach(topicPartition -> {
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
    static Entry<Map<TopicPartition, PartitionReassignmentState>, Boolean> findPartitionReassignmentStates(Admin adminClient,
                                                                                                            List<Entry<TopicPartition,
                                                                                                            List<Integer>>> targetReassignments
    ) throws ExecutionException, InterruptedException {
        Map<TopicPartition, PartitionReassignment> currentReassignments = adminClient.
            listPartitionReassignments().reassignments().get();

        List<Entry<TopicPartition, List<Integer>>> foundReassignments = new ArrayList<>();
        List<Entry<TopicPartition, List<Integer>>> notFoundReassignments = new ArrayList<>();

        targetReassignments.forEach(reassignment -> {
            if (currentReassignments.containsKey(reassignment.getKey()))
                foundReassignments.add(reassignment);
            else
                notFoundReassignments.add(reassignment);
        });

        List<Entry<TopicPartition, PartitionReassignmentState>> foundResults = foundReassignments.stream().map(e -> {
            TopicPartition part = e.getKey();
            List<Integer> targetReplicas = e.getValue();
            return new SimpleImmutableEntry<>(part,
                new PartitionReassignmentState(
                    currentReassignments.get(part).replicas(),
                    targetReplicas,
                    false));
        }).collect(Collectors.toList());

        Set<String> topicNamesToLookUp = notFoundReassignments.stream()
            .map(e -> e.getKey())
            .filter(part -> !currentReassignments.containsKey(part))
            .map(TopicPartition::topic)
            .collect(Collectors.toSet());

        Map<String, KafkaFuture<TopicDescription>> topicDescriptions = adminClient.
            describeTopics(topicNamesToLookUp).topicNameValues();

        List<Entry<TopicPartition, PartitionReassignmentState>> notFoundResults = new ArrayList<>();
        for (Entry<TopicPartition, List<Integer>> e : notFoundReassignments) {
            TopicPartition part = e.getKey();
            List<Integer> targetReplicas = e.getValue();

            if (currentReassignments.containsKey(part)) {
                PartitionReassignment reassignment = currentReassignments.get(part);
                notFoundResults.add(new SimpleImmutableEntry<>(part, new PartitionReassignmentState(
                    reassignment.replicas(),
                    targetReplicas,
                    false)));
            } else {
                notFoundResults.add(new SimpleImmutableEntry<>(part, topicDescriptionFutureToState(part.partition(),
                    topicDescriptions.get(part.topic()), targetReplicas)));
            }
        }

        Map<TopicPartition, PartitionReassignmentState> allResults = new HashMap<>();
        foundResults.forEach(e -> allResults.put(e.getKey(), e.getValue()));
        notFoundResults.forEach(e -> allResults.put(e.getKey(), e.getValue()));

        return new SimpleImmutableEntry<>(allResults, !currentReassignments.isEmpty());
    }

    private static PartitionReassignmentState topicDescriptionFutureToState(int partition,
                                                                            KafkaFuture<TopicDescription> future,
                                                                            List<Integer> targetReplicas
    ) throws InterruptedException, ExecutionException {
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
    private static Entry<Map<TopicPartitionReplica, LogDirMoveState>, Boolean> verifyReplicaMoves(Admin adminClient,
                                                                                                   Map<TopicPartitionReplica, String> targetReassignments
    ) throws ExecutionException, InterruptedException {
        Map<TopicPartitionReplica, LogDirMoveState> moveStates = findLogDirMoveStates(adminClient, targetReassignments);
        System.out.println(replicaMoveStatesToString(moveStates));
        return new SimpleImmutableEntry<>(moveStates, !moveStates.values().stream().allMatch(LogDirMoveState::done));
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
    static Map<TopicPartitionReplica, LogDirMoveState> findLogDirMoveStates(Admin adminClient,
                                                                            Map<TopicPartitionReplica, String> targetMoves
    ) throws ExecutionException, InterruptedException {
        Map<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> replicaLogDirInfos = adminClient
            .describeReplicaLogDirs(targetMoves.keySet()).all().get();

        return targetMoves.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> {
            TopicPartitionReplica replica = e.getKey();
            String targetLogDir = e.getValue();

            if (!replicaLogDirInfos.containsKey(replica))
                return new MissingReplicaMoveState(targetLogDir);

            DescribeReplicaLogDirsResult.ReplicaLogDirInfo info = replicaLogDirInfos.get(replica);

            if (info.getCurrentReplicaLogDir() == null)
                return new MissingLogDirMoveState(targetLogDir);

            if (info.getFutureReplicaLogDir() == null) {
                if (info.getCurrentReplicaLogDir().equals(targetLogDir))
                    return new CompletedMoveState(targetLogDir);

                return new CancelledMoveState(info.getCurrentReplicaLogDir(), targetLogDir);
            }

            return new ActiveMoveState(info.getCurrentReplicaLogDir(), targetLogDir, info.getFutureReplicaLogDir());
        }));
    }

    /**
     * Convert replica move states to a human-readable string.
     *
     * @param states          A map from topic partition replicas to states.
     * @return                A tuple of a summary string, and a boolean describing
     *                        whether there are any active replica moves.
     */
    static String replicaMoveStatesToString(Map<TopicPartitionReplica, LogDirMoveState> states) {
        List<String> bld = new ArrayList<>();
        states.keySet().stream().sorted(ReassignPartitionsCommand::compareTopicPartitionReplicas).forEach(replica -> {
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
    private static void clearAllThrottles(Admin adminClient,
                                          List<Entry<TopicPartition, List<Integer>>> targetParts
    ) throws ExecutionException, InterruptedException {
        Set<Integer> brokers = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toSet());
        targetParts.forEach(t -> brokers.addAll(t.getValue()));

        System.out.printf("Clearing broker-level throttles on broker%s %s%n",
            brokers.size() == 1 ? "" : "s", brokers.stream().map(Object::toString).collect(Collectors.joining(",")));
        clearBrokerLevelThrottles(adminClient, brokers);

        Set<String> topics = targetParts.stream().map(t -> t.getKey().topic()).collect(Collectors.toSet());
        System.out.printf("Clearing topic-level throttles on topic%s %s%n",
            topics.size() == 1 ? "" : "s", String.join(",", topics));
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
            BROKER_LEVEL_THROTTLES.stream().map(throttle -> new AlterConfigOp(
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
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = topics.stream().collect(Collectors.toMap(
            topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName),
            topicName -> TOPIC_LEVEL_THROTTLES.stream().map(throttle -> new AlterConfigOp(new ConfigEntry(throttle, null),
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
    public static Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartition, List<Integer>>> generateAssignment(Admin adminClient,
                                                                                                             String reassignmentJson,
                                                                                                             String brokerListString,
                                                                                                             Boolean enableRackAwareness
    ) throws ExecutionException, InterruptedException, JsonProcessingException {
        Entry<List<Integer>, List<String>> t0 = parseGenerateAssignmentArgs(reassignmentJson, brokerListString);

        List<Integer> brokersToReassign = t0.getKey();
        List<String> topicsToReassign = t0.getValue();

        Map<TopicPartition, List<Integer>> currentAssignments = getReplicaAssignmentForTopics(adminClient, topicsToReassign);
        List<BrokerMetadata> brokerMetadatas = getBrokerMetadata(adminClient, brokersToReassign, enableRackAwareness);
        Map<TopicPartition, List<Integer>> proposedAssignments = calculateAssignment(currentAssignments, brokerMetadatas);
        System.out.printf("Current partition replica assignment%n%s%n%n",
            formatAsReassignmentJson(currentAssignments, Collections.emptyMap()));
        System.out.printf("Proposed partition reassignment configuration%n%s%n",
            formatAsReassignmentJson(proposedAssignments, Collections.emptyMap()));
        return new SimpleImmutableEntry<>(proposedAssignments, currentAssignments);
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
        Map<String, List<Entry<TopicPartition, List<Integer>>>> groupedByTopic = new HashMap<>();
        for (Entry<TopicPartition, List<Integer>> e : currentAssignment.entrySet())
            groupedByTopic.computeIfAbsent(e.getKey().topic(), k -> new ArrayList<>()).add(e);
        Map<TopicPartition, List<Integer>> proposedAssignments = new HashMap<>();
        groupedByTopic.forEach((topic, assignment) -> {
            List<Integer> replicas = assignment.get(0).getValue();
            Map<Integer, List<Integer>> assignedReplicas = AdminUtils.
                assignReplicasToBrokers(brokerMetadatas, assignment.size(), replicas.size());
            assignedReplicas.forEach((partition, replicas0) ->
                proposedAssignments.put(new TopicPartition(topic, partition), replicas0));
        });
        return proposedAssignments;
    }

    private static Map<String, TopicDescription> describeTopics(Admin adminClient,
                                                                Set<String> topics) throws ExecutionException, InterruptedException {
        Map<String, KafkaFuture<TopicDescription>> futures = adminClient.describeTopics(topics).topicNameValues();
        Map<String, TopicDescription> res = new HashMap<>();
        for (Entry<String, KafkaFuture<TopicDescription>> e : futures.entrySet()) {
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
    static Map<TopicPartition, List<Integer>> getReplicaAssignmentForTopics(Admin adminClient,
                                                                            List<String> topics
    ) throws ExecutionException, InterruptedException {
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
     *                        If any topic or partition can't be found, an exception will be thrown.
     */
    static Map<TopicPartition, List<Integer>> getReplicaAssignmentForPartitions(Admin adminClient,
                                                                                Set<TopicPartition> partitions
    ) throws ExecutionException, InterruptedException {
        Map<TopicPartition, List<Integer>> res = new HashMap<>();
        describeTopics(adminClient, partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet())).forEach((topicName, topicDescription) ->
            topicDescription.partitions().forEach(info -> {
                TopicPartition tp = new TopicPartition(topicName, info.partition());
                if (partitions.contains(tp))
                    res.put(tp, info.replicas().stream().map(Node::id).collect(Collectors.toList()));
            })
        );

        if (!res.keySet().equals(partitions)) {
            Set<TopicPartition> missingPartitions = new HashSet<>(partitions);
            missingPartitions.removeAll(res.keySet());
            throw new ExecutionException(new UnknownTopicOrPartitionException("Unable to find partition: " +
                missingPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", "))));
        }
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
    static List<BrokerMetadata> getBrokerMetadata(Admin adminClient, List<Integer> brokers, boolean enableRackAwareness) throws ExecutionException, InterruptedException {
        Set<Integer> brokerSet = new HashSet<>(brokers);
        List<BrokerMetadata> results = adminClient.describeCluster().nodes().get().stream()
            .filter(node -> brokerSet.contains(node.id()))
            .map(node -> (enableRackAwareness && node.rack() != null)
                ? new BrokerMetadata(node.id(), Optional.of(node.rack()))
                : new BrokerMetadata(node.id(), Optional.empty())
            ).collect(Collectors.toList());

        long numRackless = results.stream().filter(m -> m.rack.isEmpty()).count();
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
    static Entry<List<Integer>, List<String>> parseGenerateAssignmentArgs(String reassignmentJson,
                                                                           String brokerList) throws JsonMappingException {
        List<Integer> brokerListToReassign = Arrays.stream(brokerList.split(",")).map(Integer::parseInt).collect(Collectors.toList());
        Set<Integer> duplicateReassignments = ToolsUtils.duplicates(brokerListToReassign);
        if (!duplicateReassignments.isEmpty())
            throw new AdminCommandFailedException(String.format("Broker list contains duplicate entries: %s", duplicateReassignments));
        List<String> topicsToReassign = parseTopicsData(reassignmentJson);
        Set<String> duplicateTopicsToReassign = ToolsUtils.duplicates(topicsToReassign);
        if (!duplicateTopicsToReassign.isEmpty())
            throw new AdminCommandFailedException(String.format("List of topics to reassign contains duplicate entries: %s",
                duplicateTopicsToReassign));
        return new SimpleImmutableEntry<>(brokerListToReassign, topicsToReassign);
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
    public static void executeAssignment(Admin adminClient,
                                  Boolean additional,
                                  String reassignmentJson,
                                  Long interBrokerThrottle,
                                  Long logDirThrottle,
                                  Long timeoutMs,
                                  Time time
    ) throws ExecutionException, InterruptedException, JsonProcessingException, TerseException {
        Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartitionReplica, String>> t0 = parseExecuteAssignmentArgs(reassignmentJson);

        Map<TopicPartition, List<Integer>> proposedParts = t0.getKey();
        Map<TopicPartitionReplica, String> proposedReplicas = t0.getValue();
        Map<TopicPartition, PartitionReassignment> currentReassignments = adminClient.
            listPartitionReassignments().reassignments().get();
        // If there is an existing assignment, check for --additional before proceeding.
        // This helps avoid surprising users.
        if (!additional && !currentReassignments.isEmpty()) {
            throw new TerseException(CANNOT_EXECUTE_BECAUSE_OF_EXISTING_MESSAGE);
        }
        Set<Integer> brokers = new HashSet<>();
        proposedParts.values().forEach(brokers::addAll);

        verifyBrokerIds(adminClient, brokers);
        Map<TopicPartition, List<Integer>> currentParts = getReplicaAssignmentForPartitions(adminClient, proposedParts.keySet());
        System.out.println(currentPartitionReplicaAssignmentToString(proposedParts, currentParts));

        if (interBrokerThrottle >= 0 || logDirThrottle >= 0) {
            System.out.println(YOU_MUST_RUN_VERIFY_PERIODICALLY_MESSAGE);

            if (interBrokerThrottle >= 0) {
                Map<String, Map<Integer, PartitionMove>> moveMap = calculateProposedMoveMap(currentReassignments, proposedParts, currentParts);
                modifyReassignmentThrottle(adminClient, moveMap, interBrokerThrottle);
            }

            if (logDirThrottle >= 0) {
                Set<Integer> movingBrokers = calculateMovingBrokers(proposedReplicas.keySet());
                modifyLogDirThrottle(adminClient, movingBrokers, logDirThrottle);
            }
        }

        // Execute the partition reassignments.
        Map<TopicPartition, Throwable> errors = alterPartitionReassignments(adminClient, proposedParts);
        if (!errors.isEmpty()) {
            throw new TerseException(
                String.format("Error reassigning partition(s):%n%s",
                    errors.keySet().stream()
                        .sorted(ReassignPartitionsCommand::compareTopicPartitions)
                        .map(part -> part + ": " + errors.get(part).getMessage())
                        .collect(Collectors.joining(System.lineSeparator()))));
        }
        System.out.printf("Successfully started partition reassignment%s for %s%n",
            proposedParts.size() == 1 ? "" : "s",
            proposedParts.keySet().stream()
                .sorted(ReassignPartitionsCommand::compareTopicPartitions)
                .map(Objects::toString)
                .collect(Collectors.joining(",")));
        if (!proposedReplicas.isEmpty()) {
            executeMoves(adminClient, proposedReplicas, timeoutMs, time);
        }
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
                                     Time time
    ) throws InterruptedException, TerseException {
        long startTimeMs = time.milliseconds();
        Map<TopicPartitionReplica, String> pendingReplicas = new HashMap<>(proposedReplicas);
        boolean done = false;
        do {
            Set<TopicPartitionReplica> completed = alterReplicaLogDirs(adminClient, pendingReplicas);
            if (!completed.isEmpty()) {
                completed.stream().sorted(ReassignPartitionsCommand::compareTopicPartitionReplicas).forEach(replica -> {
                    System.out.printf("Successfully started moving log directory to %s for replica %s-%s with broker %s %n",
                        pendingReplicas.get(replica), replica.topic(), replica.partition(), replica.brokerId());
                });
            }
            completed.forEach(pendingReplicas::remove);
            if (pendingReplicas.isEmpty()) {
                done = true;
            } else if (time.milliseconds() >= startTimeMs + timeoutMs) {
                throw new TerseException(String.format(
                    "Timed out before log directory move%s could be started for: %s",
                        pendingReplicas.size() == 1 ? "" : "s",
                        pendingReplicas.keySet().stream()
                            .sorted(ReassignPartitionsCommand::compareTopicPartitionReplicas)
                            .map(Object::toString)
                            .collect(Collectors.joining(","))));
            } else {
                // If a replica has been moved to a new host and we also specified a particular
                // log directory, we will have to keep retrying the alterReplicaLogDirs
                // call.  It can't take effect until the replica is moved to that host.
                time.sleep(100);
            }
        } while (!done);
    }

    /**
     * Entry point for the --list command.
     *
     * @param adminClient   The AdminClient to use.
     */
    private static void listReassignments(Admin adminClient) throws ExecutionException, InterruptedException {
        System.out.println(curReassignmentsToString(adminClient));
    }

    /**
     * Convert the current partition reassignments to text.
     *
     * @param adminClient   The AdminClient to use.
     * @return              A string describing the current partition reassignments.
     */
    static String curReassignmentsToString(Admin adminClient) throws ExecutionException, InterruptedException {
        Map<TopicPartition, PartitionReassignment> currentReassignments = adminClient.
            listPartitionReassignments().reassignments().get();
        String text = currentReassignments.keySet().stream().sorted(ReassignPartitionsCommand::compareTopicPartitions).map(part -> {
            PartitionReassignment reassignment = currentReassignments.get(part);
            List<Integer> replicas = reassignment.replicas();
            List<Integer> addingReplicas = reassignment.addingReplicas();
            List<Integer> removingReplicas = reassignment.removingReplicas();

            return String.format("%s: replicas: %s.%s%s",
                part,
                replicas.stream().map(Object::toString).collect(Collectors.joining(",")),
                addingReplicas.isEmpty() ? "" : String.format(" adding: %s.", addingReplicas.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","))),
                removingReplicas.isEmpty() ? "" : String.format(" removing: %s.", removingReplicas.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(",")))
            );
        }).collect(Collectors.joining(System.lineSeparator()));

        return text.isEmpty()
            ? "No partition reassignments found."
            : String.format("Current partition reassignments:%n%s", text);
    }

    /**
     * Verify that all the brokers in an assignment exist.
     *
     * @param adminClient                 The AdminClient to use.
     * @param brokers                     The broker IDs to verify.
     */
    private static void verifyBrokerIds(Admin adminClient, Set<Integer> brokers) throws ExecutionException, InterruptedException {
        Set<Integer> allNodeIds = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toSet());
        Optional<Integer> unknown = brokers.stream()
            .filter(brokerId -> !allNodeIds.contains(brokerId))
            .findFirst();
        if (unknown.isPresent())
            throw new AdminCommandFailedException("Unknown broker id " + unknown.get());
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
    static String currentPartitionReplicaAssignmentToString(Map<TopicPartition, List<Integer>> proposedParts,
                                                            Map<TopicPartition, List<Integer>> currentParts) throws JsonProcessingException {
        Map<TopicPartition, List<Integer>> partitionsToBeReassigned = currentParts.entrySet().stream()
            .filter(e -> proposedParts.containsKey(e.getKey()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        return String.format("Current partition replica assignment%n%n%s%n%nSave this to use as the %s",
            formatAsReassignmentJson(partitionsToBeReassigned, Collections.emptyMap()),
            "--reassignment-json-file option during rollback");
    }

    /**
     * Execute the given partition reassignments.
     *
     * @param adminClient       The admin client object to use.
     * @param reassignments     A map from topic names to target replica assignments.
     * @return                  A map from partition objects to error strings.
     */
    static Map<TopicPartition, Throwable> alterPartitionReassignments(Admin adminClient,
                                                                      Map<TopicPartition, List<Integer>> reassignments) throws InterruptedException {
        Map<TopicPartition, Optional<NewPartitionReassignment>> args = new HashMap<>();
        reassignments.forEach((part, replicas) -> args.put(part, Optional.of(new NewPartitionReassignment(replicas))));
        Map<TopicPartition, KafkaFuture<Void>> results = adminClient.alterPartitionReassignments(args).values();
        Map<TopicPartition, Throwable> errors = new HashMap<>();
        for (Entry<TopicPartition, KafkaFuture<Void>> e :  results.entrySet()) {
            try {
                e.getValue().get();
            } catch (ExecutionException t) {
                errors.put(e.getKey(), t.getCause());
            }
        }
        return errors;
    }

    /**
     * Cancel the given partition reassignments.
     *
     * @param adminClient       The admin client object to use.
     * @param reassignments     The partition reassignments to cancel.
     * @return                  A map from partition objects to error strings.
     */
    static Map<TopicPartition, Throwable> cancelPartitionReassignments(Admin adminClient,
                                                                       Set<TopicPartition> reassignments) throws InterruptedException {
        Map<TopicPartition, Optional<NewPartitionReassignment>> args = new HashMap<>();
        reassignments.forEach(part -> args.put(part, Optional.empty()));

        Map<TopicPartition, KafkaFuture<Void>> results = adminClient.alterPartitionReassignments(args).values();
        Map<TopicPartition, Throwable> errors = new HashMap<>();
        for (Entry<TopicPartition, KafkaFuture<Void>> e :  results.entrySet()) {
            try {
                e.getValue().get();
            } catch (ExecutionException t) {
                errors.put(e.getKey(), t.getCause());
            }
        }
        return errors;
    }

    /**
     * Compute the in progress partition move from the current reassignments.
     * @param currentReassignments All replicas, adding replicas and removing replicas of target partitions
     */
    private static Map<String, Map<Integer, PartitionMove>> calculateCurrentMoveMap(Map<TopicPartition, PartitionReassignment> currentReassignments) {
        Map<String, Map<Integer, PartitionMove>> moveMap = new HashMap<>();
        // Add the current reassignments to the move map.
        currentReassignments.forEach((part, reassignment) -> {
            List<Integer> allReplicas = reassignment.replicas();
            List<Integer> addingReplicas = reassignment.addingReplicas();

            // The addingReplicas is included in the replicas during reassignment
            Set<Integer> sources = new HashSet<>(allReplicas);
            addingReplicas.forEach(sources::remove);

            Set<Integer> destinations = new HashSet<>(addingReplicas);

            Map<Integer, PartitionMove> partMoves = moveMap.computeIfAbsent(part.topic(), k -> new HashMap<>());
            partMoves.put(part.partition(), new PartitionMove(sources, destinations));
        });
        return moveMap;
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
    static Map<String, Map<Integer, PartitionMove>> calculateProposedMoveMap(Map<TopicPartition, PartitionReassignment> currentReassignments,
                                                                             Map<TopicPartition, List<Integer>> proposedParts,
                                                                             Map<TopicPartition, List<Integer>> currentParts) {
        Map<String, Map<Integer, PartitionMove>> moveMap = calculateCurrentMoveMap(currentReassignments);
        for (Entry<TopicPartition, List<Integer>> e : proposedParts.entrySet()) {
            TopicPartition part = e.getKey();
            List<Integer> replicas = e.getValue();
            Map<Integer, PartitionMove> partMoves = moveMap.computeIfAbsent(part.topic(), k -> new HashMap<>());

            // If there is a reassignment in progress, use the sources from moveMap, otherwise
            // use the sources from currentParts
            Set<Integer> sources = new HashSet<>();

            if (partMoves.containsKey(part.partition())) {
                PartitionMove move = partMoves.get(part.partition());
                sources.addAll(move.sources);
            } else if (currentParts.containsKey(part))
                sources.addAll(currentParts.get(part));
            else
                throw new RuntimeException("Trying to reassign a topic partition " + part + " with 0 replicas");

            Set<Integer> destinations = new HashSet<>(replicas);
            destinations.removeAll(sources);

            partMoves.put(part.partition(), new PartitionMove(sources, destinations));
        }
        return moveMap;
    }

    /**
     * Calculate the leader throttle configurations to use.
     *
     * @param moveMap   The movements.
     * @return          A map from topic names to leader throttle configurations.
     */
    static Map<String, String> calculateLeaderThrottles(Map<String, Map<Integer, PartitionMove>> moveMap) {
        Map<String, String> results = new HashMap<>();
        moveMap.forEach((topicName, partMoveMap) -> {
            Set<String> components = new TreeSet<>();
            partMoveMap.forEach((partId, move) ->
                move.sources.forEach(source -> components.add(String.format("%d:%d", partId, source))));
            results.put(topicName, String.join(",", components));
        });
        return results;
    }

    /**
     * Calculate the follower throttle configurations to use.
     *
     * @param moveMap   The movements.
     * @return          A map from topic names to follower throttle configurations.
     */
    static Map<String, String> calculateFollowerThrottles(Map<String, Map<Integer, PartitionMove>> moveMap) {
        Map<String, String> results = new HashMap<>();
        moveMap.forEach((topicName, partMoveMap) -> {
            Set<String> components = new TreeSet<>();
            partMoveMap.forEach((partId, move) ->
                move.destinations.forEach(destination -> {
                    if (!move.sources.contains(destination)) {
                        components.add(String.format("%d:%d", partId, destination));
                    }
                })
            );
            results.put(topicName, String.join(",", components));
        });

        return results;
    }

    /**
     * Calculate all the brokers which are involved in the given partition reassignments.
     *
     * @param moveMap       The partition movements.
     * @return              A set of all the brokers involved.
     */
    static Set<Integer> calculateReassigningBrokers(Map<String, Map<Integer, PartitionMove>> moveMap) {
        Set<Integer> reassigningBrokers = new TreeSet<>();
        moveMap.values().forEach(partMoveMap -> partMoveMap.values().forEach(partMove -> {
            reassigningBrokers.addAll(partMove.sources);
            reassigningBrokers.addAll(partMove.destinations);
        }));
        return reassigningBrokers;
    }

    /**
     * Calculate all the brokers which are involved in the given directory movements.
     *
     * @param replicaMoves  The replica movements.
     * @return              A set of all the brokers involved.
     */
    static Set<Integer> calculateMovingBrokers(Set<TopicPartitionReplica> replicaMoves) {
        return replicaMoves.stream().map(TopicPartitionReplica::brokerId).collect(Collectors.toSet());
    }

    /**
     * Modify the topic configurations that control inter-broker throttling.
     *
     * @param adminClient         The adminClient object to use.
     * @param leaderThrottles     A map from topic names to leader throttle configurations.
     * @param followerThrottles   A map from topic names to follower throttle configurations.
     */
    static void modifyTopicThrottles(Admin adminClient,
                                     Map<String, String> leaderThrottles,
                                     Map<String, String> followerThrottles) throws ExecutionException, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        Set<String> topicNames = new HashSet<>(leaderThrottles.keySet());
        topicNames.addAll(followerThrottles.keySet());
        topicNames.forEach(topicName -> {
            List<AlterConfigOp> ops = new ArrayList<>();
            if (leaderThrottles.containsKey(topicName)) {
                ops.add(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, leaderThrottles.get(topicName)), AlterConfigOp.OpType.SET));
            }
            if (followerThrottles.containsKey(topicName)) {
                ops.add(new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, followerThrottles.get(topicName)), AlterConfigOp.OpType.SET));
            }
            if (!ops.isEmpty()) {
                configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topicName), ops);
            }
        });
        adminClient.incrementalAlterConfigs(configs).all().get();
    }

    private static void modifyReassignmentThrottle(
        Admin admin,
        Map<String, Map<Integer, PartitionMove>> moveMap,
        Long interBrokerThrottle
    ) throws ExecutionException, InterruptedException {
        Map<String, String> leaderThrottles = calculateLeaderThrottles(moveMap);
        Map<String, String> followerThrottles = calculateFollowerThrottles(moveMap);
        modifyTopicThrottles(admin, leaderThrottles, followerThrottles);

        Set<Integer> reassigningBrokers = calculateReassigningBrokers(moveMap);
        modifyInterBrokerThrottle(admin, reassigningBrokers, interBrokerThrottle);
    }

    /**
     * Modify the leader/follower replication throttles for a set of brokers.
     *
     * @param adminClient The Admin instance to use
     * @param reassigningBrokers The set of brokers involved in the reassignment
     * @param interBrokerThrottle The new throttle (ignored if less than 0)
     */
    static void modifyInterBrokerThrottle(Admin adminClient,
                                          Set<Integer> reassigningBrokers,
                                          long interBrokerThrottle) throws ExecutionException, InterruptedException {
        if (interBrokerThrottle >= 0) {
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            reassigningBrokers.forEach(brokerId -> {
                List<AlterConfigOp> ops = new ArrayList<>();
                ops.add(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                    Long.toString(interBrokerThrottle)), AlterConfigOp.OpType.SET));
                ops.add(new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                    Long.toString(interBrokerThrottle)), AlterConfigOp.OpType.SET));
                configs.put(new ConfigResource(ConfigResource.Type.BROKER, Long.toString(brokerId)), ops);
            });
            adminClient.incrementalAlterConfigs(configs).all().get();
            System.out.println("The inter-broker throttle limit was set to " + interBrokerThrottle + " B/s");
        }
    }

    /**
     * Modify the log dir reassignment throttle for a set of brokers.
     *
     * @param admin The Admin instance to use
     * @param movingBrokers The set of broker to alter the throttle of
     * @param logDirThrottle The new throttle (ignored if less than 0)
     */
    static void modifyLogDirThrottle(Admin admin,
                                     Set<Integer> movingBrokers,
                                     long logDirThrottle) throws ExecutionException, InterruptedException {
        if (logDirThrottle >= 0) {
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            movingBrokers.forEach(brokerId -> {
                List<AlterConfigOp> ops = new ArrayList<>();
                ops.add(new AlterConfigOp(new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, Long.toString(logDirThrottle)), AlterConfigOp.OpType.SET));
                configs.put(new ConfigResource(ConfigResource.Type.BROKER, Long.toString(brokerId)), ops);
            });
            admin.incrementalAlterConfigs(configs).all().get();
            System.out.println("The replica-alter-dir throttle limit was set to " + logDirThrottle + " B/s");
        }
    }

    /**
     * Parse the reassignment JSON string passed to the --execute command.
     *
     * @param reassignmentJson  The JSON string.
     * @return                  A tuple of the partitions to be reassigned and the replicas
     *                          to be reassigned.
     */
    static Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartitionReplica, String>> parseExecuteAssignmentArgs(
        String reassignmentJson
    ) throws JsonProcessingException {
        Entry<List<Entry<TopicPartition, List<Integer>>>, Map<TopicPartitionReplica, String>> t0 = parsePartitionReassignmentData(reassignmentJson);

        List<Entry<TopicPartition, List<Integer>>> partitionsToBeReassigned = t0.getKey();
        Map<TopicPartitionReplica, String> replicaAssignment = t0.getValue();

        if (partitionsToBeReassigned.isEmpty())
            throw new AdminCommandFailedException("Partition reassignment list cannot be empty");
        if (partitionsToBeReassigned.stream().anyMatch(t -> t.getValue().isEmpty())) {
            throw new AdminCommandFailedException("Partition replica list cannot be empty");
        }
        Set<TopicPartition> duplicateReassignedPartitions = ToolsUtils.duplicates(partitionsToBeReassigned.stream().map(t -> t.getKey()).collect(Collectors.toList()));
        if (!duplicateReassignedPartitions.isEmpty()) {
            throw new AdminCommandFailedException(String.format(
                "Partition reassignment contains duplicate topic partitions: %s",
                duplicateReassignedPartitions.stream().map(Object::toString).collect(Collectors.joining(",")))
            );
        }
        List<Entry<TopicPartition, Set<Integer>>> duplicateEntries = partitionsToBeReassigned.stream()
            .map(t -> new SimpleImmutableEntry<>(t.getKey(), ToolsUtils.duplicates(t.getValue())))
            .filter(t -> !t.getValue().isEmpty())
            .collect(Collectors.toList());
        if (!duplicateEntries.isEmpty()) {
            String duplicatesMsg = duplicateEntries.stream().map(t ->
                String.format("%s contains multiple entries for %s",
                    t.getKey(),
                    t.getValue().stream().map(Object::toString).collect(Collectors.joining(",")))
            ).collect(Collectors.joining(". "));
            throw new AdminCommandFailedException(String.format("Partition replica lists may not contain duplicate entries: %s", duplicatesMsg));
        }
        return new SimpleImmutableEntry<>(partitionsToBeReassigned.stream().collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue())), replicaAssignment);
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
    static Entry<Set<TopicPartition>, Set<TopicPartitionReplica>> cancelAssignment(Admin adminClient,
                                                                                    String jsonString,
                                                                                    Boolean preserveThrottles,
                                                                                    Long timeoutMs,
                                                                                    Time time
    ) throws ExecutionException, InterruptedException, JsonProcessingException, TerseException {
        Entry<List<Entry<TopicPartition, List<Integer>>>, Map<TopicPartitionReplica, String>> t0 = parsePartitionReassignmentData(jsonString);

        List<Entry<TopicPartition, List<Integer>>> targetParts = t0.getKey();
        Map<TopicPartitionReplica, String> targetReplicas = t0.getValue();
        Set<TopicPartition> targetPartsSet = targetParts.stream().map(t -> t.getKey()).collect(Collectors.toSet());
        Set<TopicPartition> curReassigningParts = new HashSet<>();
        adminClient.listPartitionReassignments(targetPartsSet).reassignments().get().forEach((part, reassignment) -> {
            if (reassignment.addingReplicas().isEmpty() || !reassignment.removingReplicas().isEmpty())
                curReassigningParts.add(part);
        });
        if (!curReassigningParts.isEmpty()) {
            Map<TopicPartition, Throwable> errors = cancelPartitionReassignments(adminClient, curReassigningParts);
            if (!errors.isEmpty()) {
                throw new TerseException(String.format(
                    "Error cancelling partition reassignment%s for:%n%s",
                    errors.size() == 1 ? "" : "s",
                    errors.keySet().stream()
                        .sorted(ReassignPartitionsCommand::compareTopicPartitions)
                        .map(part -> part + ": " + errors.get(part).getMessage()).collect(Collectors.joining(System.lineSeparator())))
                );
            }
            System.out.printf("Successfully cancelled partition reassignment%s for: %s%n",
                curReassigningParts.size() == 1 ? "" : "s",
                curReassigningParts.stream().sorted(ReassignPartitionsCommand::compareTopicPartitions).map(Object::toString).collect(Collectors.joining(","))
            );
        } else {
            System.out.println("None of the specified partition reassignments are active.");
        }
        Map<TopicPartitionReplica, String> curMovingParts = new HashMap<>();
        findLogDirMoveStates(adminClient, targetReplicas).forEach((part, moveState) -> {
            if (moveState instanceof ActiveMoveState)
                curMovingParts.put(part, ((ActiveMoveState) moveState).currentLogDir);
        });
        if (curMovingParts.isEmpty()) {
            System.out.print("None of the specified partition moves are active.");
        } else {
            executeMoves(adminClient, curMovingParts, timeoutMs, time);
        }
        if (!preserveThrottles) {
            clearAllThrottles(adminClient, targetParts);
        }
        return new SimpleImmutableEntry<>(curReassigningParts, curMovingParts.keySet());
    }

    public static String formatAsReassignmentJson(Map<TopicPartition, List<Integer>> partitionsToBeReassigned,
                                                   Map<TopicPartitionReplica, String> replicaLogDirAssignment) throws JsonProcessingException {
        List<Map<String, Object>> partitions = new ArrayList<>();
        partitionsToBeReassigned.keySet().stream().sorted(ReassignPartitionsCommand::compareTopicPartitions).forEach(tp -> {
            List<Integer> replicas = partitionsToBeReassigned.get(tp);
            Map<String, Object> data = new LinkedHashMap<>();

            data.put("topic", tp.topic());
            data.put("partition", tp.partition());
            data.put("replicas", replicas);
            data.put("log_dirs", replicas.stream()
                .map(r -> replicaLogDirAssignment.getOrDefault(new TopicPartitionReplica(tp.topic(), tp.partition(), r), ANY_LOG_DIR))
                .collect(Collectors.toList()));

            partitions.add(data);
        });

        Map<String, Object> results = new LinkedHashMap<>();

        results.put("version", 1);
        results.put("partitions", partitions);

        return Json.encodeAsString(results);
    }

    private static List<String> parseTopicsData(String jsonData) throws JsonMappingException {
        Optional<JsonValue> parsed = Json.parseFull(jsonData);
        if (parsed.isPresent()) {
            JsonValue js = parsed.get();
            Optional<JsonValue> version = js.asJsonObject().get("version");
            return parseTopicsData(version.isPresent() ? version.get().to(INT) : EARLIEST_TOPICS_JSON_VERSION, js);
        } else {
            throw new AdminOperationException("The input string is not a valid JSON");
        }
    }

    private static List<String> parseTopicsData(int version, JsonValue js) throws JsonMappingException {
        switch (version) {
            case 1:
                List<String> results = new ArrayList<>();
                Optional<JsonValue> partitionsSeq = js.asJsonObject().get("topics");
                if (partitionsSeq.isPresent()) {
                    Iterator<JsonValue> iter = partitionsSeq.get().asJsonArray().iterator();
                    while (iter.hasNext()) {
                        results.add(iter.next().asJsonObject().apply("topic").to(STRING));
                    }
                }
                return results;

            default:
                throw new AdminOperationException("Not supported version field value " + version);
        }
    }

    private static Entry<List<Entry<TopicPartition, List<Integer>>>, Map<TopicPartitionReplica, String>> parsePartitionReassignmentData(
        String jsonData
    ) throws JsonProcessingException {
        JsonValue js;
        try {
            js = Json.tryParseFull(jsonData);
        } catch (JsonParseException f) {
            throw new AdminOperationException(f);
        }
        Optional<JsonValue> version = js.asJsonObject().get("version");
        return parsePartitionReassignmentData(version.isPresent() ? version.get().to(INT) : EARLIEST_VERSION, js);
    }

    // Parses without deduplicating keys so the data can be checked before allowing reassignment to proceed
    private static Entry<List<Entry<TopicPartition, List<Integer>>>, Map<TopicPartitionReplica, String>> parsePartitionReassignmentData(
        int version, JsonValue jsonData
    ) throws JsonMappingException {
        switch (version) {
            case 1:
                List<Entry<TopicPartition, List<Integer>>> partitionAssignment = new ArrayList<>();
                Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();

                Optional<JsonValue> partitionsSeq = jsonData.asJsonObject().get("partitions");
                if (partitionsSeq.isPresent()) {
                    Iterator<JsonValue> iter = partitionsSeq.get().asJsonArray().iterator();
                    while (iter.hasNext()) {
                        JsonObject partitionFields = iter.next().asJsonObject();
                        String topic = partitionFields.apply("topic").to(STRING);
                        int partition = partitionFields.apply("partition").to(INT);
                        List<Integer> newReplicas = partitionFields.apply("replicas").to(INT_LIST);
                        Optional<JsonValue> logDirsOpts = partitionFields.get("log_dirs");
                        List<String> newLogDirs;
                        if (logDirsOpts.isPresent())
                            newLogDirs = logDirsOpts.get().to(STRING_LIST);
                        else
                            newLogDirs = newReplicas.stream().map(r -> ANY_LOG_DIR).collect(Collectors.toList());
                        if (newReplicas.size() != newLogDirs.size())
                            throw new AdminCommandFailedException("Size of replicas list " + newReplicas + " is different from " +
                                "size of log dirs list " + newLogDirs + " for partition " + new TopicPartition(topic, partition));
                        partitionAssignment.add(new SimpleImmutableEntry<>(new TopicPartition(topic, partition), newReplicas));
                        for (int i = 0; i < newLogDirs.size(); i++) {
                            Integer replica = newReplicas.get(i);
                            String logDir = newLogDirs.get(i);

                            if (logDir.equals(ANY_LOG_DIR))
                                continue;

                            replicaAssignment.put(new TopicPartitionReplica(topic, partition, replica), logDir);
                        }
                    }
                }

                return new SimpleImmutableEntry<>(partitionAssignment, replicaAssignment);

            default:
                throw new AdminOperationException("Not supported version field value " + version);
        }
    }

    static ReassignPartitionsCommandOptions validateAndParseArgs(String[] args) {
        ReassignPartitionsCommandOptions opts = new ReassignPartitionsCommandOptions(args);

        CommandLineUtils.maybePrintHelpOrVersion(opts, HELP_TEXT);

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
        
        if (opts.options.has(opts.bootstrapServerOpt) && opts.options.has(opts.bootstrapControllerOpt))
            CommandLineUtils.printUsageAndExit(opts.parser, "Please don't specify both --bootstrap-server and --bootstrap-controller");
        else if (!opts.options.has(opts.bootstrapServerOpt) && !opts.options.has(opts.bootstrapControllerOpt))
            CommandLineUtils.printUsageAndExit(opts.parser, "Please specify either --bootstrap-server or --bootstrap-controller");

        boolean isBootstrapServer = opts.options.has(opts.bootstrapServerOpt);

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
            isBootstrapServer ? opts.bootstrapServerOpt : opts.bootstrapControllerOpt,
            opts.commandConfigOpt,
            opts.preserveThrottlesOpt,
            opts.timeoutOpt
        ));
        permittedArgs.put(opts.listOpt, Arrays.asList(
            isBootstrapServer ? opts.bootstrapServerOpt : opts.bootstrapControllerOpt,
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

    static Set<TopicPartitionReplica> alterReplicaLogDirs(Admin adminClient,
                                                          Map<TopicPartitionReplica, String> assignment) throws InterruptedException {
        Set<TopicPartitionReplica> results = new HashSet<>();
        Map<TopicPartitionReplica, KafkaFuture<Void>> values = adminClient.alterReplicaLogDirs(assignment).values();

        for (Entry<TopicPartitionReplica, KafkaFuture<Void>> e : values.entrySet()) {
            TopicPartitionReplica replica = e.getKey();
            KafkaFuture<Void> future = e.getValue();
            try {
                future.get();
                results.add(replica);
            } catch (ExecutionException t) {
                // Ignore ReplicaNotAvailableException.  It is OK if the replica is not
                // available at this moment.
                if (t.getCause() instanceof ReplicaNotAvailableException)
                    continue;
                throw new AdminCommandFailedException("Failed to alter dir for " + replica, t);
            }
        }
        return results;
    }
}
