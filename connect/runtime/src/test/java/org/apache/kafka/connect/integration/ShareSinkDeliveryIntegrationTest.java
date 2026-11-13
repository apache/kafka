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
package org.apache.kafka.connect.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("integration")
@Timeout(120)
public class ShareSinkDeliveryIntegrationTest {
    private static final String CONNECTOR_NAME = "share-sink-delivery";
    private static final String INPUT_TOPIC = "share-sink-delivery-input";
    private static final String AUDIT_TOPIC = "share-sink-delivery-audit";
    private static final String GROUP_ID = "share-sink-delivery-group";
    private static final String CONTROL_ID_CONFIG = "test.control.id";
    private static final String AUDIT_BOOTSTRAP_CONFIG = "test.audit.bootstrap.servers";
    private static final String AUDIT_TOPIC_CONFIG = "test.audit.topic";
    private static final String INPUT_TOPIC_ID_CONFIG = "test.input.topic.id";
    private static final String TASK_ID_CONFIG = "test.task.id";
    private static final int LOCK_DURATION_MS = 3_000;
    private static final long TIMEOUT_MS = 15_000;
    private static final Map<String, Control> CONTROLS = new ConcurrentHashMap<>();

    private EmbeddedConnectCluster connect;
    private Control control;
    private String controlId;
    private String inputTopicId;

    @BeforeEach
    void setup() throws Exception {
        control = new Control();
        controlId = UUID.randomUUID().toString();
        CONTROLS.put(controlId, control);
        Properties brokerProps = new Properties();
        brokerProps.put("share.coordinator.state.topic.replication.factor", "1");
        brokerProps.put("share.coordinator.state.topic.min.isr", "1");
        brokerProps.put("share.coordinator.state.topic.num.partitions", "1");
        brokerProps.put("group.share.min.record.lock.duration.ms", "1000");
        brokerProps.put("group.share.record.lock.duration.ms", Integer.toString(LOCK_DURATION_MS));
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("connector.client.config.override.policy", "All");
        workerProps.put("offset.storage.partitions", "1");
        workerProps.put("status.storage.partitions", "1");
        workerProps.put("scheduled.rebalance.max.delay.ms", "0");
        connect = new EmbeddedConnectCluster.Builder()
                .name(CONNECTOR_NAME)
                .numWorkers(2)
                .numBrokers(1)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .build();
        connect.start();
        connect.kafka().createTopic(INPUT_TOPIC, 1);
        connect.kafka().createTopic(AUDIT_TOPIC, 1);
        try (Admin admin = connect.kafka().createAdminClient()) {
            inputTopicId = admin.describeTopics(List.of(INPUT_TOPIC)).allTopicNames()
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS).get(INPUT_TOPIC).topicId().toString();
            ConfigResource group = new ConfigResource(ConfigResource.Type.GROUP, GROUP_ID);
            admin.incrementalAlterConfigs(Map.of(group, List.of(
                    new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry("share.record.lock.duration.ms", Integer.toString(LOCK_DURATION_MS)), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry("share.renew.acknowledge.enable", "true"), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry("share.delivery.count.limit", "10"), AlterConfigOp.OpType.SET)
            ))).all().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    @AfterEach
    void cleanup() {
        if (control != null) {
            control.releasePuts.countDown();
        }
        try {
            if (connect != null) {
                connect.stop();
            }
        } finally {
            if (controlId != null) {
                CONTROLS.remove(controlId);
            }
        }
    }

    @Test
    void acknowledgedInputsAreNotWrittenAgainAfterConnectorRestart() throws Exception {
        startConnector(1);
        produceInputs(0, 5);
        awaitCompletedInputs(5);
        connect.pauseConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksArePaused(
                CONNECTOR_NAME, 1, "Share sink did not pause after retiring its consumer generation");
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME, 1, "Share sink did not resume after pause");
        produceInputs(5, 1);
        awaitCompletedInputs(6);
        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(CONNECTOR_NAME, "Share sink did not stop");

        List<ConsumerRecord<byte[], byte[]>> beforeRestart = readAudit();
        assertAuditCoverage(beforeRestart, 6);
        Map<String, Long> previousOccurrences = auditOccurrences(beforeRestart);

        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME, 1, "Share sink did not resume");
        produceInputs(6, 1);
        awaitCompletedInputs(7);
        List<ConsumerRecord<byte[], byte[]>> afterRestart = readAudit();
        assertAuditCoverage(afterRestart, 7);
        Map<String, Long> resumedOccurrences = auditOccurrences(afterRestart);
        previousOccurrences.forEach((input, count) -> assertEquals(count, resumedOccurrences.get(input),
                "Previously acknowledged input was written after restart: " + input));
    }

    @Test
    void fatalFailureAfterDurableWriteRedeliversAndPreservesEveryAuditOccurrence() throws Exception {
        control.failAfterWrite.set(true);
        startConnector(1);
        produceInputs(0, 5);
        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(
                CONNECTOR_NAME, 1, "Fatal failure after a durable write did not fail the task");

        String failedInput = control.failedInput.get();
        assertNotNull(failedInput, "Task failed before the injected durable destination write");
        assertTrue(auditOccurrences(readAudit()).getOrDefault(failedInput, 0L) >= 1);
        connect.restartTask(CONNECTOR_NAME, 0);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME, 1, "Share sink did not restart");
        awaitCompletedInputs(5);

        List<ConsumerRecord<byte[], byte[]>> audit = readAudit();
        assertAuditCoverage(audit, 5);
        assertTrue(auditOccurrences(audit).getOrDefault(failedInput, 0L) >= 2,
                "An input written before failure was not redelivered");
        assertTrue(audit.size() > 5, "Audit reader discarded duplicate destination effects");
    }

    @Test
    void tasksConcurrentlyWriteDistinctInputsFromOnePartition() throws Exception {
        control.blockPuts.set(true);
        control.expectedArrivals = new CountDownLatch(2);
        startConnector(2);
        produceInputs(0, 2);
        try {
            assertTrue(control.expectedArrivals.await(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                    "Two share tasks did not enter put concurrently on one input partition");
            assertTrue(control.activePuts.get() >= 2);
            assertEquals(2, control.firstInputByTask.size());
            assertEquals(2, new HashSet<>(control.firstInputByTask.values()).size(),
                    "Concurrent tasks only received an expired duplicate of the same input");
        } finally {
            control.releasePuts.countDown();
        }
        awaitCompletedInputs(2);
        List<ConsumerRecord<byte[], byte[]>> audit = readAudit();
        assertAuditCoverage(audit, 2);
        Set<String> writingTasks = audit.stream()
                .map(record -> new String(record.headers().lastHeader("task.id").value(), StandardCharsets.UTF_8))
                .collect(Collectors.toSet());
        assertTrue(writingTasks.size() >= 2, "One task performed all destination writes");
    }

    @Test
    void slowDestinationPutRenewsClaimsWhileACompetingMemberPolls() throws Exception {
        control.blockPuts.set(true);
        control.expectedArrivals = new CountDownLatch(1);
        startConnector(1);
        produceInputs(0, 1);
        assertTrue(control.expectedArrivals.await(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                "Share sink did not enter its blocked put");

        String competitorId = "share-sink-competing-member";
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, competitorId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        props.put(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        try (KafkaShareConsumer<byte[], byte[]> competitor = new KafkaShareConsumer<>(props);
                Admin admin = connect.kafka().createAdminClient()) {
            competitor.subscribe(List.of(INPUT_TOPIC));
            long joinDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
            boolean assigned = false;
            while (!assigned && System.nanoTime() < joinDeadline) {
                assertNoUnexpectedDelivery(competitor, competitor.poll(Duration.ofMillis(200)));
                ShareGroupDescription group = admin.describeShareGroups(List.of(GROUP_ID)).all()
                        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS).get(GROUP_ID);
                assigned = group.members().stream().anyMatch(member -> competitorId.equals(member.clientId())
                        && member.assignment().topicPartitions().contains(new TopicPartition(INPUT_TOPIC, 0)));
            }
            assertTrue(assigned, "Competing member never became eligible for the input partition");

            long observationDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2L * LOCK_DURATION_MS);
            while (System.nanoTime() < observationDeadline) {
                assertNoUnexpectedDelivery(competitor, competitor.poll(Duration.ofMillis(200)));
                assertTasksHealthy();
            }
            assertTrue(readAudit().isEmpty(), "Blocked destination put unexpectedly finished");
        } finally {
            control.releasePuts.countDown();
        }
        awaitCompletedInputs(1);
        assertAuditCoverage(readAudit(), 1);
    }

    private Map<String, String> connectorConfig(int tasks) {
        Map<String, String> result = new HashMap<>();
        result.put("name", CONNECTOR_NAME);
        result.put("connector.class", AuditShareSinkConnector.class.getName());
        result.put("tasks.max", Integer.toString(tasks));
        result.put("topics", INPUT_TOPIC);
        result.put("consumer.type", "share");
        result.put("share.group.id", GROUP_ID);
        result.put("share.processing.max.age.ms", "60000");
        result.put("share.processing.cancel.grace.ms", "5000");
        result.put("consumer.override.share.acknowledgement.mode", "explicit");
        result.put("consumer.override.share.acquire.mode", "record_limit");
        result.put("consumer.override.max.poll.records", "1");
        result.put("key.converter", StringConverter.class.getName());
        result.put("value.converter", StringConverter.class.getName());
        result.put(CONTROL_ID_CONFIG, controlId);
        result.put(AUDIT_BOOTSTRAP_CONFIG, connect.kafka().bootstrapServers());
        result.put(AUDIT_TOPIC_CONFIG, AUDIT_TOPIC);
        result.put(INPUT_TOPIC_ID_CONFIG, inputTopicId);
        return result;
    }

    private void startConnector(int tasks) throws InterruptedException {
        connect.configureConnector(CONNECTOR_NAME, connectorConfig(tasks));
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                CONNECTOR_NAME, tasks, "Share sink tasks did not start");
    }

    private void produceInputs(int first, int count) {
        List<ProducerRecord<byte[], byte[]>> records = IntStream.range(first, first + count)
                .mapToObj(index -> new ProducerRecord<byte[], byte[]>(INPUT_TOPIC, 0, null,
                        ("input-" + index).getBytes(StandardCharsets.UTF_8)))
                .toList();
        connect.kafka().produce(records);
    }

    private void awaitCompletedInputs(int inputCount) throws Exception {
        TopicPartition partition = new TopicPartition(INPUT_TOPIC, 0);
        try (Admin admin = connect.kafka().createAdminClient()) {
            assertEquals(inputCount, admin.listOffsets(Map.of(partition, OffsetSpec.latest())).all()
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS).get(partition).offset());
            waitForCondition(() -> {
                assertTasksHealthy();
                try {
                    SharePartitionOffsetInfo state = admin.listShareGroupOffsets(Map.of(GROUP_ID,
                                    new ListShareGroupOffsetsSpec().topicPartitions(List.of(partition))))
                            .partitionsToOffsetInfo(GROUP_ID).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).get(partition);
                    return state != null && state.startOffset() >= 0 && state.lag().isPresent() && state.lag().get() == 0;
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof GroupIdNotFoundException) {
                        return false;
                    }
                    throw e;
                }
            }, TIMEOUT_MS, "Share group did not durably complete its fixed input log");
        }
    }

    private void assertTasksHealthy() {
        for (ConnectorStateInfo.TaskState task : connect.connectorStatus(CONNECTOR_NAME).tasks()) {
            assertFalse("FAILED".equals(task.state()), "Share sink task failed: " + task.trace());
        }
    }

    private List<ConsumerRecord<byte[], byte[]>> readAudit() {
        TopicPartition partition = new TopicPartition(AUDIT_TOPIC, 0);
        List<ConsumerRecord<byte[], byte[]>> result = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = connect.kafka().createConsumer(Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long endOffset = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
            while (consumer.position(partition) < endOffset && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.offset() < endOffset) {
                        result.add(record);
                    }
                }
            }
            assertTrue(consumer.position(partition) >= endOffset, "Audit reader did not reach its captured log end");
        }
        return result;
    }

    private Map<String, Long> auditOccurrences(List<ConsumerRecord<byte[], byte[]>> audit) {
        return audit.stream().collect(Collectors.groupingBy(
                record -> new String(record.key(), StandardCharsets.UTF_8), Collectors.counting()));
    }

    private void assertAuditCoverage(List<ConsumerRecord<byte[], byte[]>> audit, int inputCount) {
        Map<String, String> expected = IntStream.range(0, inputCount).boxed().collect(Collectors.toMap(
                offset -> inputTopicId + ":0:" + offset, offset -> "input-" + offset));
        assertEquals(expected.keySet(), auditOccurrences(audit).keySet());
        for (ConsumerRecord<byte[], byte[]> record : audit) {
            String input = new String(record.key(), StandardCharsets.UTF_8);
            assertEquals(expected.get(input), new String(record.value(), StandardCharsets.UTF_8));
            assertNotNull(record.headers().lastHeader("task.id"));
            assertNotNull(record.headers().lastHeader("put.attempt"));
        }
    }

    private void assertNoUnexpectedDelivery(KafkaShareConsumer<byte[], byte[]> competitor,
                                            ConsumerRecords<byte[], byte[]> records) {
        if (!records.isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> record : records) {
                competitor.acknowledge(record, AcknowledgeType.RELEASE);
            }
            competitor.commitSync(Duration.ofMillis(TIMEOUT_MS));
            fail("Competing member acquired an input while the destination write was being renewed");
        }
    }

    private static final class Control {
        private final AtomicBoolean blockPuts = new AtomicBoolean();
        private final AtomicBoolean failAfterWrite = new AtomicBoolean();
        private final AtomicInteger activePuts = new AtomicInteger();
        private final AtomicReference<String> failedInput = new AtomicReference<>();
        private final Map<String, String> firstInputByTask = new ConcurrentHashMap<>();
        private final CountDownLatch releasePuts = new CountDownLatch(1);
        private volatile CountDownLatch expectedArrivals = new CountDownLatch(0);
    }

    public static class AuditShareSinkConnector extends SinkConnector {
        private Map<String, String> config;

        @Override
        public void start(Map<String, String> props) {
            config = new HashMap<>(props);
        }

        @Override
        public Class<? extends Task> taskClass() {
            return AuditShareSinkTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            List<Map<String, String>> result = new ArrayList<>();
            for (int index = 0; index < maxTasks; index++) {
                Map<String, String> task = new HashMap<>(config);
                task.put(TASK_ID_CONFIG, Integer.toString(index));
                result.add(task);
            }
            return result;
        }

        @Override
        public void stop() {
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public String version() {
            return "test";
        }
    }

    public static class AuditShareSinkTask extends ShareSinkTask {
        private KafkaProducer<String, String> producer;
        private Control control;
        private String taskId;
        private String auditTopic;
        private String topicId;
        private String instance;
        private long putAttempt;

        @Override
        public void start(Map<String, String> props) {
            control = CONTROLS.get(props.get(CONTROL_ID_CONFIG));
            if (control == null) {
                throw new ConnectException("Missing test fault controller");
            }
            taskId = props.get(TASK_ID_CONFIG);
            auditTopic = props.get(AUDIT_TOPIC_CONFIG);
            topicId = props.get(INPUT_TOPIC_ID_CONFIG);
            instance = UUID.randomUUID().toString();
            Map<String, Object> producerProps = new HashMap<>();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(AUDIT_BOOTSTRAP_CONFIG));
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
            producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
            producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
            producer = new KafkaProducer<>(producerProps);
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            if (records.isEmpty()) {
                return;
            }
            control.activePuts.incrementAndGet();
            try {
                if (control.blockPuts.get()) {
                    String firstInput = identity(records.iterator().next());
                    if (control.firstInputByTask.putIfAbsent(taskId, firstInput) == null) {
                        control.expectedArrivals.countDown();
                    }
                    if (!control.releasePuts.await(2 * TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                        throw new ConnectException("Timed out waiting for the destination write gate");
                    }
                }
                String attempt = instance + ":" + putAttempt++;
                for (SinkRecord record : records) {
                    ProducerRecord<String, String> audit = new ProducerRecord<>(auditTopic, identity(record), record.value().toString());
                    audit.headers().add("task.id", taskId.getBytes(StandardCharsets.UTF_8));
                    audit.headers().add("put.attempt", attempt.getBytes(StandardCharsets.UTF_8));
                    producer.send(audit).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                }
                if (control.failAfterWrite.compareAndSet(true, false)) {
                    control.failedInput.set(identity(records.iterator().next()));
                    throw new ConnectException("Injected fatal failure after durable destination write");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException("Destination write interrupted", e);
            } catch (ExecutionException | TimeoutException e) {
                throw new ConnectException("Destination write did not complete", e);
            } finally {
                control.activePuts.decrementAndGet();
            }
        }

        private String identity(SinkRecord record) {
            return topicId + ":" + record.originalKafkaPartition() + ":" + record.originalKafkaOffset();
        }

        @Override
        public void stop() {
            if (producer != null) {
                producer.close(Duration.ofSeconds(5));
            }
        }

        @Override
        public String version() {
            return "test";
        }
    }
}
