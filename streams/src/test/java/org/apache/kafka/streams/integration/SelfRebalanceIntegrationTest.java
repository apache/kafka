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
package org.apache.kafka.streams.integration;

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.AssignedStandbyTasks;
import org.apache.kafka.streams.processor.internals.AssignedStreamsTasks;
import org.apache.kafka.streams.processor.internals.MockChangelogReader;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamThreadTest;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.processor.internals.StreamThread.getSharedAdminClientId;
import static org.junit.Assert.assertEquals;

public class SelfRebalanceIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(QueryableStateIntegrationTest.class);

    private static final int NUM_BROKERS = 1;
    private Properties streamsConfiguration;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private final MockTime mockTime = CLUSTER.time;

    private final String topic1 = "topic1";

//    private void createTopics() throws Exception {
//        streamsConfiguration = new Properties();
//        final String applicationId = "queryable-state-" + testNo.incrementAndGet();
//
//        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
//        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("qs-test").getPath());
//        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//
//        CLUSTER.createTopics(topic1);
//
//    }
//
//    @Test
//    public void shouldHaveAllTasksClosedOnEnforceRebalance() throws InterruptedException, IOException {
//        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
//        String storeName = "store";
//        internalStreamsBuilder
//            .stream(Collections.singleton(topic1), consumed)
//            .groupByKey()
//            .count(Materialized.as(storeName));
//
//        CLUSTER.createTopic(topic1);
//        CLUSTER.start();
//        Properties consumerProperties = mkProperties(mkMap(
//            mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "groupId"),
//            mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
//            mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()),
//            mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName())
//        ));
//
//        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
//
//        final TaskManager taskManager = new TaskManager(
//            new MockChangelogReader(),
//            PROCESS_ID,
//            "log-prefix",
//            consumer,
//            streamsMetadataState,
//            new StreamThread.TaskCreator(
//                internalTopologyBuilder,
//                new StreamsConfig(configProps(false)),
//                streamsMetrics,
//                stateDirectory,
//                new MockChangelogReader(),
//                null,
//                mockTime,
//                clientSupplier,
//                clientSupplier.getProducer(new HashMap<>()),
//                "thread",
//                new LogContext("").logger(StreamThreadTest.class)
//            ),
//            null,
//            null,
//            new AssignedStreamsTasks(new LogContext()),
//            new AssignedStandbyTasks(new LogContext())
//        );
//        taskManager.setConsumer(consumer);
//
//        final StreamThread thread = new StreamThread(
//            mockTime,
//            config,
//            null,
//            consumer,
//            consumer,
//            null,
//            taskManager,
//            streamsMetrics,
//            internalTopologyBuilder,
//            CLIENT_ID,
//            new LogContext(""),
//            new AtomicInteger()
//        ).updateThreadMetadata(getSharedAdminClientId(CLIENT_ID));
//
//        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
//        final List<TopicPartition> assignedPartitions = new ArrayList<>();
//
//        // assign single partition
//        assignedPartitions.add(t1p1);
//        activeTasks.put(task1, Collections.singleton(t1p1));
//
//        thread.taskManager().setPartitionsToTaskId(Collections.singletonMap(t1p1, task1));
//        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());
////        consumer.subscribe(Collections.singletonList(topic1));
//        thread.setStateListener(
//            (t, newState, oldState) -> {
//                if (oldState == StreamThread.State.CREATED) {
////                    thread.setState(StreamThread.State.PARTITIONS_REVOKED);
////                    thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
//                    // Trigger enforce rebalance through version probing.
//                    thread.setAssignmentErrorCode(AssignorError.VERSION_PROBING.code());
//                } else if (newState == StreamThread.State.RUNNING) {
//                    taskManager.updateNewAndRestoringTasks();
//                    assertEquals(1, thread.tasks().size());
//                    thread.setState(StreamThread.State.PENDING_SHUTDOWN);
//                }
//            });
//
//        thread.run();
//        assertEquals(0, thread.tasks().size());
//    }

}
