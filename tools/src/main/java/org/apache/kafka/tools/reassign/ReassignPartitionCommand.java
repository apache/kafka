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
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.json.JsonValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

    private static void handleAction(Admin adminClient, ReassignPartitionsCommandOptions opts) throws IOException {
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
    private static VerifyAssignmentResult verifyAssignment(Admin adminClient, String jsonString, Boolean preserveThrottles) {
        return null;
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
                                                                                                              List<Tuple<TopicPartition, List<Integer>>> targets) {
        return null;
    }


    private static boolean compareTopicPartition(TopicPartition a, TopicPartition b) {
        return false;
    }

    private static boolean compareTopicPartitionReplicas(TopicPartition a, TopicPartition b) {
        return false;
    }

    /**
     * Convert partition reassignment states to a human-readable string.
     *
     * @param states      A map from topic partitions to states.
     * @return            A string summarizing the partition reassignment states.
     */
    private static String partitionReassignmentStatesToString(Map<TopicPartition, PartitionReassignmentState> states) {
        return null;
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
                                                                                                                   List<Tuple<TopicPartition, List<Integer>>> targetReassignments) {
        return null;
    }

    private static PartitionReassignmentState topicDescriptionFutureToState(int partition,
                                                                            KafkaFuture<TopicDescription> future,
                                                                            List<Integer> targetReplicas) {
        return null;
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
                                                                                                  Map<TopicPartitionReplica, String> targetReassignments) {
        return null;
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
    private static Map<TopicPartitionReplica, LogDirMoveState>findLogDirMoveStates(Admin adminClient,
                                                                                   Map<TopicPartitionReplica, String> targetMoves) {
        return null;
    }

    /**
     * Convert replica move states to a human-readable string.
     *
     * @param states          A map from topic partition replicas to states.
     * @return                A tuple of a summary string, and a boolean describing
     *                        whether there are any active replica moves.
     */
    private static String replicaMoveStatesToString(Map<TopicPartitionReplica, LogDirMoveState> states) {
        return null;
    }

    /**
     * Clear all topic-level and broker-level throttles.
     *
     * @param adminClient     The AdminClient to use.
     * @param targetParts     The target partitions loaded from the JSON file.
     */
    private static void clearAllThrottles(Admin adminClient, List<Tuple<TopicPartition, List<Integer>>> targetParts) {

    }

    /**
     * Clear all throttles which have been set at the broker level.
     *
     * @param adminClient       The AdminClient to use.
     * @param brokers           The brokers to clear the throttles for.
     */
    private static void clearBrokerLevelThrottles(Admin adminClient, Set<Integer> brokers) {

    }

    /**
     * Clear the reassignment throttles for the specified topics.
     *
     * @param adminClient           The AdminClient to use.
     * @param topics                The topics to clear the throttles for.
     */
    private static void clearTopicLevelThrottles(Admin adminClient, Set<String> topics) {

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
                                                                                                                    Boolean enableRackAwareness) {
        return null;
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
        return null;
    }

    private static Map<String, TopicDescription> describeTopics(Admin adminClient,
                                       Set<String> topics) {
        return null;
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
                                                                                    List<String> topics) {
        return null;
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
                                                                                        Set<TopicPartition> partitions) {
        return null;
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
    private static List<BrokerMetadata> getBrokerMetadata(Admin adminClient, List<Integer> brokers, boolean enableRackAwareness) {
        return null;
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
        return null;
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

    private List<String> parseTopicsData(String jsonData) {
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
