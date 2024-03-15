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

import kafka.utils.TestInfoUtils;
import kafka.server.DynamicConfig;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ToolsTestUtils {
    /** @see TestInfoUtils#TestWithParameterizedQuorumName()  */
    public static final String TEST_WITH_PARAMETERIZED_QUORUM_NAME = "{displayName}.{argumentsWithNames}";
    /** @see TestInfoUtils#TestWithParameterizedQuorumAndGroupProtocolNames()  */
    public static final String TEST_WITH_PARAMETERIZED_QUORUM_AND_GROUP_PROTOCOL_NAMES = "{displayName}.quorum={0}.groupProtocol={1}";

    private static int randomPort = 0;

    public static String captureStandardOut(Runnable runnable) {
        return captureStandardStream(false, runnable);
    }

    public static String captureStandardErr(Runnable runnable) {
        return captureStandardStream(true, runnable);
    }

    private static String captureStandardStream(boolean isErr, Runnable runnable) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream currentStream = isErr ? System.err : System.out;
        PrintStream tempStream = new PrintStream(outputStream);
        if (isErr)
            System.setErr(tempStream);
        else
            System.setOut(tempStream);
        try {
            runnable.run();
            return new String(outputStream.toByteArray()).trim();
        } finally {
            if (isErr)
                System.setErr(currentStream);
            else
                System.setOut(currentStream);

            tempStream.close();
        }
    }

    public static List<Properties> createBrokerProperties(int numConfigs, String zkConnect,
                                                          Map<Integer, String> rackInfo,
                                                          int numPartitions,
                                                          short defaultReplicationFactor) {

        return createBrokerProperties(numConfigs, zkConnect, rackInfo, 1, false, numPartitions,
            defaultReplicationFactor, 0);
    }

    /**
     * Create a test config for the provided parameters.
     *
     * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
     */
    public static List<Properties> createBrokerProperties(int numConfigs, String zkConnect,
                                                          Map<Integer, String> rackInfo, int logDirCount,
                                                          boolean enableToken, int numPartitions, short defaultReplicationFactor,
                                                          int startingIdNumber) {
        List<Properties> result = new ArrayList<>();
        int endingIdNumber = startingIdNumber + numConfigs - 1;
        for (int node = startingIdNumber; node <= endingIdNumber; node++) {
            result.add(TestUtils.createBrokerConfig(node, zkConnect, true, true, randomPort,
                scala.Option.empty(),
                scala.Option.empty(),
                scala.Option.empty(),
                true, false, randomPort, false, randomPort, false, randomPort,
                scala.Option.apply(rackInfo.get(node)),
                logDirCount, enableToken, numPartitions, defaultReplicationFactor, false));
        }
        return result;
    }

    /**
     * Set broker replication quotas and enable throttling for a set of partitions. This
     * will override any previous replication quotas, but will leave the throttling status
     * of other partitions unaffected.
     */
    public static void setReplicationThrottleForPartitions(Admin admin,
                                                           List<Integer> brokerIds,
                                                           Set<TopicPartition> partitions,
                                                           int throttleBytes) throws ExecutionException, InterruptedException {
        throttleAllBrokersReplication(admin, brokerIds, throttleBytes);
        assignThrottledPartitionReplicas(admin, partitions.stream().collect(Collectors.toMap(p -> p, p -> brokerIds)));
    }

    /**
     * Throttles all replication across the cluster.
     * @param adminClient is the adminClient to use for making connection with the cluster
     * @param brokerIds all broker ids in the cluster
     * @param throttleBytes is the target throttle
     */
    public static void throttleAllBrokersReplication(Admin adminClient, List<Integer> brokerIds, int throttleBytes) throws ExecutionException, InterruptedException {
        List<AlterConfigOp> throttleConfigs = new ArrayList<>();
        throttleConfigs.add(new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker$.MODULE$.LeaderReplicationThrottledRateProp(),
            Integer.toString(throttleBytes)), AlterConfigOp.OpType.SET));
        throttleConfigs.add(new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker$.MODULE$.FollowerReplicationThrottledRateProp(),
            Integer.toString(throttleBytes)), AlterConfigOp.OpType.SET));

        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        for (int brokerId : brokerIds) {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId));
            configs.put(configResource, throttleConfigs);
        }

        adminClient.incrementalAlterConfigs(configs).all().get();
    }

    /**
     * Remove a set of throttled partitions and reset the overall replication quota.
     */
    public static void removeReplicationThrottleForPartitions(Admin admin, List<Integer> brokerIds, Set<TopicPartition> partitions) throws ExecutionException, InterruptedException {
        removePartitionReplicaThrottles(admin, partitions);
        resetBrokersThrottle(admin, brokerIds);
    }

    public static void assignThrottledPartitionReplicas(Admin adminClient, Map<TopicPartition, List<Integer>> allReplicasByPartition) throws InterruptedException, ExecutionException {
        Map<ConfigResource, List<Map.Entry<TopicPartition, List<Integer>>>> configResourceToPartitionReplicas =
            allReplicasByPartition.entrySet().stream()
            .collect(Collectors.groupingBy(
                topicPartitionListEntry -> new ConfigResource(ConfigResource.Type.TOPIC, topicPartitionListEntry.getKey().topic()))
            );

        Map<ConfigResource, List<AlterConfigOp>> throttles = configResourceToPartitionReplicas.entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> {
                    List<AlterConfigOp> alterConfigOps = new ArrayList<>();
                    Map<TopicPartition, List<Integer>> replicaThrottle =
                        entry.getValue().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    alterConfigOps.add(new AlterConfigOp(
                        new ConfigEntry(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, formatReplicaThrottles(replicaThrottle)),
                        AlterConfigOp.OpType.SET));
                    alterConfigOps.add(new AlterConfigOp(
                        new ConfigEntry(LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, formatReplicaThrottles(replicaThrottle)),
                        AlterConfigOp.OpType.SET));
                    return alterConfigOps;
                }
            ));
        adminClient.incrementalAlterConfigs(new HashMap<>(throttles)).all().get();
    }

    public static void resetBrokersThrottle(Admin adminClient, List<Integer> brokerIds) throws ExecutionException, InterruptedException {
        throttleAllBrokersReplication(adminClient, brokerIds, Integer.MAX_VALUE);
    }

    public static void removePartitionReplicaThrottles(Admin adminClient, Set<TopicPartition> partitions) throws ExecutionException, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> throttles = partitions.stream().collect(Collectors.toMap(
            tp -> new ConfigResource(ConfigResource.Type.TOPIC, tp.topic()),
            tp -> Arrays.asList(
                    new AlterConfigOp(new ConfigEntry(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""),
                        AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry(LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""),
                        AlterConfigOp.OpType.DELETE))
            ));

        adminClient.incrementalAlterConfigs(throttles).all().get();
    }

    public static String formatReplicaThrottles(Map<TopicPartition, List<Integer>> moves) {
        return moves.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(replicaId -> entry.getKey().partition() + ":" + replicaId))
            .collect(Collectors.joining(","));
    }

    public static File tempPropertiesFile(Map<String, String> properties) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue() + System.lineSeparator());
        }
        return org.apache.kafka.test.TestUtils.tempFile(sb.toString());
    }

    public static class MockExitProcedure implements Exit.Procedure {
        private boolean hasExited = false;
        private int statusCode;

        @Override
        public void execute(int statusCode, String message) {
            if (!this.hasExited) {
                this.hasExited = true;
                this.statusCode = statusCode;
            }
        }

        public boolean hasExited() {
            return hasExited;
        }

        public int statusCode() {
            return statusCode;
        }
    }
}
