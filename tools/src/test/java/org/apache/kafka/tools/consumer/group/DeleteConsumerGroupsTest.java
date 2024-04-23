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
package org.apache.kafka.tools.consumer.group;

import joptsimple.OptionException;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.ALL, serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
})
public class DeleteConsumerGroupsTest {
    private final ClusterInstance cluster;
    private static final String TOPIC = "foo";
    private static final String GROUP = "test.group";

    public DeleteConsumerGroupsTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testDeleteWithTopicOption() {
        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP, "--topic"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ClusterTest
    public void testDeleteCmdNonExistingGroup() {
        String missingGroup = "missing.group";
        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", missingGroup};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);
        String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups);
        assertTrue(output.contains("Group '" + missingGroup + "' could not be deleted due to:") && output.contains(Errors.GROUP_ID_NOT_FOUND.message()),
                "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
    }

    @ClusterTest
    public void testDeleteNonExistingGroup() {
        String missingGroup = "missing.group";
        // note the group to be deleted is a different (non-existing) group
        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", missingGroup};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);
        Map<String, Throwable> result = service.deleteGroups();
        assertTrue(result.size() == 1 && result.containsKey(missingGroup) && result.get(missingGroup).getCause() instanceof GroupIdNotFoundException,
                "The expected error (" + Errors.GROUP_ID_NOT_FOUND + ") was not detected while deleting consumer group");
    }

    @ClusterTest
    public void testDeleteNonEmptyGroup() throws Exception {
        try (ConsumerGroupExecutor consumerGroupExecutor = buildConsumerGroupExecutor(GROUP)) {
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);
            TestUtils.waitForCondition(
                    () -> service.collectGroupMembers(GROUP, false).getValue().get().size() == 1,
                    "The group did not initialize as expected."
            );

            String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups);
            Map<String, Throwable> result = service.deleteGroups();

            assertTrue(output.contains("Group '" + GROUP + "' could not be deleted due to:") && output.contains(Errors.NON_EMPTY_GROUP.message()),
                    "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting consumer group. Output was: (" + output + ")");

            assertNotNull(result.get(GROUP),
                    "Group was deleted successfully, but it shouldn't have been. Result was:(" + result + ")");

            assertTrue(result.size() == 1 && result.containsKey(GROUP) && result.get(GROUP).getCause() instanceof GroupNotEmptyException,
                    "The expected error (" + Errors.NON_EMPTY_GROUP + ") was not detected while deleting consumer group. Result was:(" + result + ")");
        }
    }

    @ClusterTest
    void testDeleteEmptyGroup() throws Exception {
        try (ConsumerGroupExecutor consumerGroupExecutor = buildConsumerGroupExecutor(GROUP)) {
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

            TestUtils.waitForCondition(
                    () -> service.listConsumerGroups().contains(GROUP) && Objects.equals(service.collectGroupState(GROUP).state, "Stable"),
                    "The group did not initialize as expected."
            );

            consumerGroupExecutor.close();

            TestUtils.waitForCondition(
                    () -> Objects.equals(service.collectGroupState(GROUP).state, "Empty"),
                    "The group did not become empty as expected."
            );

            Map<String, Throwable> result = new HashMap<>();
            String output = ToolsTestUtils.grabConsoleOutput(() -> result.putAll(service.deleteGroups()));

            assertTrue(output.contains("Deletion of requested consumer groups ('" + GROUP + "') was successful."),
                    "The consumer group could not be deleted as expected");
            assertTrue(result.size() == 1 && result.containsKey(GROUP) && result.get(GROUP) == null,
                    "The consumer group could not be deleted as expected");
        }
    }

    @ClusterTest
    public void testDeleteCmdAllGroups() throws Exception {
        // Create 3 groups with 1 consumer per each
        Map<String, ConsumerGroupExecutor> groupNameToExecutor = IntStream.rangeClosed(1, 3)
                .mapToObj(i -> GROUP + i)
                .collect(Collectors.toMap(Function.identity(), this::buildConsumerGroupExecutor));

        String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--all-groups"};
        ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

        TestUtils.waitForCondition(() ->
                        new HashSet<>(service.listConsumerGroups()).equals(groupNameToExecutor.keySet()) &&
                                groupNameToExecutor.keySet().stream().allMatch(checkGroupState(service, "Stable")),
                "The group did not initialize as expected.");

        // Shutdown consumers to empty out groups
        for (ConsumerGroupExecutor consumerGroupExecutor : groupNameToExecutor.values()) {
            consumerGroupExecutor.close();
        }

        TestUtils.waitForCondition(() ->
                        groupNameToExecutor.keySet().stream().allMatch(checkGroupState(service, "Empty")),
                "The group did not become empty as expected.");

        String output = ToolsTestUtils.grabConsoleOutput(service::deleteGroups).trim();
        Set<String> expectedGroupsForDeletion = groupNameToExecutor.keySet();
        Set<String> deletedGroupsGrepped = Arrays.stream(output.substring(output.indexOf('(') + 1, output.indexOf(')')).split(","))
                .map(str -> str.replaceAll("'", "").trim())
                .collect(Collectors.toSet());

        assertTrue(output.matches("Deletion of requested consumer groups (.*) was successful.")
                        && Objects.equals(deletedGroupsGrepped, expectedGroupsForDeletion),
                "The consumer group(s) could not be deleted as expected");
    }

    @ClusterTest
    public void testDeleteCmdWithMixOfSuccessAndError() throws Exception {
        try (ConsumerGroupExecutor executor = buildConsumerGroupExecutor(GROUP)) {
            String missingGroup = "missing.group";
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

            TestUtils.waitForCondition(
                    () -> service.listConsumerGroups().contains(GROUP) && Objects.equals(service.collectGroupState(GROUP).state, "Stable"),
                    "The group did not initialize as expected.");

            executor.close();
            TestUtils.waitForCondition(
                    () -> Objects.equals(service.collectGroupState(GROUP).state, "Empty"),
                    "The group did not become empty as expected.");

            cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP, "--group", missingGroup};

            ConsumerGroupCommand.ConsumerGroupService service2 = getConsumerGroupService(cgcArgs);

            String output = ToolsTestUtils.grabConsoleOutput(service2::deleteGroups);
            assertTrue(output.contains("Group '" + missingGroup + "' could not be deleted due to:")
                            && output.contains(Errors.GROUP_ID_NOT_FOUND.message())
                            && output.contains("These consumer groups were deleted successfully: '" + GROUP + "'"),
                    "The consumer group deletion did not work as expected");
        }
    }

    @ClusterTest
    public void testDeleteWithMixOfSuccessAndError() throws Exception {
        try (ConsumerGroupExecutor executor = buildConsumerGroupExecutor(GROUP)) {
            String missingGroup = "missing.group";
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
            ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs);

            TestUtils.waitForCondition(
                    () -> service.listConsumerGroups().contains(GROUP) && Objects.equals(service.collectGroupState(GROUP).state, "Stable"),
                    "The group did not initialize as expected.");

            executor.close();

            TestUtils.waitForCondition(
                    () -> Objects.equals(service.collectGroupState(GROUP).state, "Empty"),
                    "The group did not become empty as expected.");

            cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP, "--group", missingGroup};

            ConsumerGroupCommand.ConsumerGroupService service2 = getConsumerGroupService(cgcArgs);
            Map<String, Throwable> result = service2.deleteGroups();
            assertTrue(result.size() == 2 &&
                            result.containsKey(GROUP) && result.get(GROUP) == null &&
                            result.containsKey(missingGroup) &&
                            result.get(missingGroup).getMessage().contains(Errors.GROUP_ID_NOT_FOUND.message()),
                    "The consumer group deletion did not work as expected");
        }
    }

    @ClusterTest
    public void testDeleteWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", GROUP};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    private ConsumerGroupExecutor buildConsumerGroupExecutor(String group) {
        return new ConsumerGroupExecutor(cluster.bootstrapServers(),
                1,
                null != group ? group : GROUP,
                GroupProtocol.CLASSIC.name,
                TOPIC,
                RangeAssignor.class.getName(),
                Optional.empty(),
                Optional.empty(),
                false);
    }

    private Predicate<String> checkGroupState(ConsumerGroupCommand.ConsumerGroupService service, String stable) {
        return groupId -> {
            try {
                return Objects.equals(service.collectGroupState(groupId).state, stable);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        return new ConsumerGroupCommand.ConsumerGroupService(
                opts,
                Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    static abstract class AbstractConsumerRunnable implements Runnable {
        final String broker;
        final String groupId;
        final Optional<Properties> customPropsOpt;
        final boolean syncCommit;
        volatile boolean isShutdown = false;

        final Properties props = new Properties();
        KafkaConsumer<String, String> consumer;

        boolean configured = false;

        public AbstractConsumerRunnable(String broker, String groupId, Optional<Properties> customPropsOpt, boolean syncCommit) {
            this.broker = broker;
            this.groupId = groupId;
            this.customPropsOpt = customPropsOpt;
            this.syncCommit = syncCommit;
        }

        void configure() {
            configured = true;
            configure(props);
            customPropsOpt.ifPresent(props::putAll);
            consumer = new KafkaConsumer<>(props);
        }

        void configure(Properties props) {
            props.put("bootstrap.servers", broker);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
        }

        abstract void subscribe();

        @Override
        public void run() {
            assert configured : "Must call configure before use";
            try {
                subscribe();
                while (!isShutdown) {
                    consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                    if (syncCommit)
                        consumer.commitSync();
                }
            } catch (WakeupException e) {
                // OK
            } finally {
                consumer.close();
            }
        }

        void shutdown() {
            isShutdown = true;
            consumer.wakeup();
        }
    }

    static class ConsumerRunnable extends AbstractConsumerRunnable {
        final String topic;
        final String groupProtocol;
        final String strategy;
        final Optional<String> remoteAssignor;

        public ConsumerRunnable(String broker,
                                String groupId,
                                String groupProtocol,
                                String topic,
                                String strategy,
                                Optional<String> remoteAssignor,
                                Optional<Properties> customPropsOpt,
                                boolean syncCommit) {
            super(broker, groupId, customPropsOpt, syncCommit);

            this.topic = topic;
            this.groupProtocol = groupProtocol;
            this.strategy = strategy;
            this.remoteAssignor = remoteAssignor;
        }

        @Override
        void configure(Properties props) {
            super.configure(props);
            props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
            if (groupProtocol.toUpperCase(Locale.ROOT).equals(GroupProtocol.CONSUMER.toString())) {
                remoteAssignor.ifPresent(assignor -> props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, assignor));
            } else {
                props.put("partition.assignment.strategy", strategy);
            }
        }

        @Override
        void subscribe() {
            consumer.subscribe(Collections.singleton(topic));
        }
    }


    static class ConsumerGroupExecutor implements AutoCloseable {
        final int numThreads;
        final ExecutorService executor;
        final List<AbstractConsumerRunnable> consumers = new ArrayList<>();

        public ConsumerGroupExecutor(
                String broker,
                int numConsumers,
                String groupId,
                String groupProtocol,
                String topic,
                String strategy,
                Optional<String> remoteAssignor,
                Optional<Properties> customPropsOpt,
                boolean syncCommit
        ) {
            this.numThreads = numConsumers;
            this.executor = Executors.newFixedThreadPool(numThreads);
            IntStream.rangeClosed(1, numConsumers).forEach(i -> {
                ConsumerRunnable th = new ConsumerRunnable(
                        broker,
                        groupId,
                        groupProtocol,
                        topic,
                        strategy,
                        remoteAssignor,
                        customPropsOpt,
                        syncCommit
                );
                th.configure();
                submit(th);
            });
        }

        void submit(AbstractConsumerRunnable consumerThread) {
            consumers.add(consumerThread);
            executor.submit(consumerThread);
        }

        @Override
        public void close() throws Exception {
            consumers.forEach(AbstractConsumerRunnable::shutdown);
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
