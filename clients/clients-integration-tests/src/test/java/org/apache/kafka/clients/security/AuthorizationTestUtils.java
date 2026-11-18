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
package org.apache.kafka.clients.security;

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.ClientsTestUtils.waitForPollThrowException;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public final class AuthorizationTestUtils {
    public static final int BROKER_COUNT = 1;
    public static final int NUM_RECORDS = 1;
    public static final String GROUP_PREFIX = "gr";
    public static final String GROUP_ID = GROUP_PREFIX + "oup";
    public static final String TOPIC_PREFIX = "e2e";
    public static final String TOPIC_NAME = TOPIC_PREFIX + "topic";
    public static final TopicPartition TP = new TopicPartition(TOPIC_NAME, 0);

    private AuthorizationTestUtils() {
    }

    public record TestContext(
        ClusterInstance cluster,
        KafkaPrincipal clientPrincipal,
        Map<String, Object> clientConfig,
        Map<String, Object> superuserConfig
    ) {
        public TestContext {
            clientConfig = Map.copyOf(clientConfig);
            superuserConfig = Map.copyOf(superuserConfig);
        }

        public Producer<byte[], byte[]> producer() {
            return producer(Map.of());
        }

        public Producer<byte[], byte[]> producer(Map<String, Object> overrides) {
            Map<String, Object> props = new HashMap<>(clientConfig);
            props.putAll(overrides);
            return cluster.producer(props);
        }

        public Consumer<byte[], byte[]> consumer(GroupProtocol groupProtocol) {
            Map<String, Object> props = new HashMap<>(clientConfig);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1500");
            return cluster.consumer(props);
        }

        public Admin admin() {
            return cluster.admin(clientConfig);
        }

        public Admin superuserAdmin() {
            return cluster.admin(superuserConfig);
        }

        public AclBinding acl(ResourcePattern resource, AclOperation operation) {
            return new AclBinding(resource, new AccessControlEntry(clientPrincipal.toString(), "*", operation, ALLOW));
        }
    }

    public static void createAcls(TestContext context, List<AclBinding> bindings) throws Exception {
        try (Admin admin = context.superuserAdmin()) {
            admin.createAcls(bindings).all().get();
        }
        Map<ResourcePattern, Set<AccessControlEntry>> entries = bindings.stream()
                .collect(Collectors.groupingBy(AclBinding::pattern, Collectors.mapping(AclBinding::entry, Collectors.toSet())));
        for (var entry : entries.entrySet()) {
            waitAcls(context, entry.getKey(), entry.getValue());
        }
    }

    public static void waitAcls(
        TestContext context,
        ResourcePattern resource,
        Set<AccessControlEntry> expected
    ) throws InterruptedException {
        var filter = new AclBindingFilter(resource.toFilter(), AccessControlEntryFilter.ANY);
        context.cluster().waitAcls(filter, expected);
    }

    public static void setReadAndWriteAcls(TestContext context, TopicPartition tp) throws Exception {
        setResourceAcls(context, tp.topic(), GROUP_ID, LITERAL);
    }

    private static void setResourceAcls(
        TestContext context,
        String topic,
        String group,
        PatternType patternType
    ) throws Exception {
        var topicResource = new ResourcePattern(TOPIC, topic, patternType);
        createAcls(
            context,
            List.of(
                context.acl(topicResource, WRITE), context.acl(topicResource, CREATE),
                context.acl(topicResource, DESCRIBE), context.acl(topicResource, READ),
                context.acl(new ResourcePattern(GROUP, group, patternType), READ)
            )
        );
    }

    public static void setAclsAndProduce(TestContext context, TopicPartition tp) throws Exception {
        setReadAndWriteAcls(context, tp);
        try (var producer = context.producer()) {
            sendRecords(producer, NUM_RECORDS, tp);
        }
    }

    public static void sendRecords(Producer<byte[], byte[]> producer, int numRecords, TopicPartition tp) throws Exception {
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            byte[] value = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            futures.add(producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), value, value)));
        }
        try {
            for (var future : futures) {
                future.get();
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw e;
        }
    }

    public static void consumeRecords(Consumer<byte[], byte[]> consumer) throws InterruptedException {
        consumeRecords(consumer, NUM_RECORDS, 0, TP, 10_000);
    }

    public static void consumeRecords(
        Consumer<byte[], byte[]> consumer,
        int numRecords,
        int startingOffset,
        TopicPartition tp,
        long timeoutMs
    ) throws InterruptedException {
        var records = ClientsTestUtils.consumeRecords(consumer, numRecords, Integer.MAX_VALUE, timeoutMs);
        assertEquals(numRecords, records.size(), "Timed out waiting for records");
        for (int i = 0; i < numRecords; i++) {
            var record = records.get(i);
            assertEquals(tp.topic(), record.topic());
            assertEquals(tp.partition(), record.partition());
            assertEquals((long) startingOffset + i, record.offset());
        }
    }

    public static double totalMetricValue(TestContext context, int brokerId, String metricName) {
        return context.cluster()
                .brokers()
                .get(brokerId)
                .metrics()
                .metrics().values().stream()
                .filter(metric -> metric.metricName().name().equals(metricName))
                .mapToDouble(metric -> ((Number) metric.metricValue()).doubleValue())
                .sum();
    }

    public static void confirmReauthenticationMetrics(TestContext context) {
        var gauge = KafkaYammerMetrics.defaultRegistry()
                .allMetrics()
                .entrySet().stream()
                .filter(entry -> entry.getKey().getName().equals("ExpiredConnectionsKilledCount"))
                .map(entry -> (Gauge<?>) entry.getValue())
                .findFirst()
                .orElseThrow();
        assertEquals(0.0, ((Number) gauge.value()).doubleValue(), "Expired connections killed total");
        for (int brokerId : context.cluster().brokers().keySet()) {
            assertEquals(0.0, totalMetricValue(context, brokerId, "expired-connections-killed-count"), "Expired connections killed");
            assertEquals(0.0, totalMetricValue(context, brokerId, "failed-reauthentication-total"), "Failed reauthentications");
        }
    }

    public static final class BaseEndToEndAuthorizationTestcase {
        private BaseEndToEndAuthorizationTestcase() {
        }

        public static void testProduceConsumeViaAssign(TestContext context, GroupProtocol groupProtocol) throws Exception {
            setAclsAndProduce(context, TP);
            try (var consumer = context.consumer(groupProtocol)) {
                consumer.assign(List.of(TP));
                consumeRecords(consumer);
                confirmReauthenticationMetrics(context);
            }
        }

        public static void testProduceConsumeViaSubscribe(TestContext context, GroupProtocol groupProtocol) throws Exception {
            setAclsAndProduce(context, TP);
            try (var consumer = context.consumer(groupProtocol)) {
                consumer.subscribe(List.of(TOPIC_NAME));
                consumeRecords(consumer);
                confirmReauthenticationMetrics(context);
            }
        }

        public static void testProduceConsumeWithWildcardAcls(TestContext context, GroupProtocol groupProtocol) throws Exception {
            setResourceAcls(context, "*", "*", LITERAL);
            produceAndConsume(context, groupProtocol);
        }

        public static void testProduceConsumeWithPrefixedAcls(TestContext context, GroupProtocol groupProtocol) throws Exception {
            setResourceAcls(context, TOPIC_PREFIX, GROUP_PREFIX, PREFIXED);
            produceAndConsume(context, groupProtocol);
        }

        private static void produceAndConsume(TestContext context, GroupProtocol groupProtocol) throws Exception {
            try (var producer = context.producer(); var consumer = context.consumer(groupProtocol)) {
                sendRecords(producer, NUM_RECORDS, TP);
                consumer.subscribe(List.of(TOPIC_NAME));
                consumeRecords(consumer);
                confirmReauthenticationMetrics(context);
            }
        }

        public static void testProduceConsumeTopicAutoCreateTopicCreateAcl(
            TestContext context,
            GroupProtocol groupProtocol
        ) throws Exception {
            var tp = new TopicPartition("topic2", 0);
            setAclsAndProduce(context, tp);
            try (var consumer = context.consumer(groupProtocol)) {
                consumer.assign(List.of(tp));
                consumeRecords(consumer, NUM_RECORDS, 0, tp, 10_000);
                confirmReauthenticationMetrics(context);
            }
        }

        public static void testNoDescribeProduceOrConsumeWithoutTopicDescribeAcl(
            TestContext context,
            GroupProtocol groupProtocol,
            boolean idempotenceEnabled
        ) throws Exception {
            createAcls(context, List.of(context.acl(new ResourcePattern(GROUP, GROUP_ID, LITERAL), READ)));
            Map<String, Object> overrides = Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotenceEnabled);
            try (var producer = context.producer(overrides);
                 var consumer = context.consumer(groupProtocol);
                 var admin = context.admin()) {
                assertThrows(TopicAuthorizationException.class, () -> sendRecords(producer, NUM_RECORDS, TP));
                consumer.assign(List.of(TP));
                waitForPollThrowException(consumer, TopicAuthorizationException.class);
                var e1 = assertThrows(ExecutionException.class, () -> admin.describeTopics(Set.of(TOPIC_NAME)).allTopicNames().get());
                assertInstanceOf(TopicAuthorizationException.class, e1.getCause());

                var tp2 = new TopicPartition("topic2", 0);
                setReadAndWriteAcls(context, tp2);
                // An idempotent producer cannot recover from the fatal authorization error.
                if (idempotenceEnabled) {
                    try (var producer2 = context.producer(overrides)) {
                        verifyAuthorizationRecovery(context, producer2, consumer, admin, tp2);
                    }
                } else {
                    verifyAuthorizationRecovery(context, producer, consumer, admin, tp2);
                }
            }
        }

        private static void verifyAuthorizationRecovery(
            TestContext context,
            Producer<byte[], byte[]> producer,
            Consumer<byte[], byte[]> consumer,
            Admin admin,
            TopicPartition tp2
        ) throws Exception {
            sendRecords(producer, NUM_RECORDS, tp2);
            consumer.assign(List.of(tp2));
            consumeRecords(consumer, NUM_RECORDS, 0, tp2, 10_000);
            var results = admin.describeTopics(Set.of(TOPIC_NAME, tp2.topic())).topicNameValues();
            assertEquals(1, results.get(tp2.topic()).get().partitions().size());
            var e = assertThrows(ExecutionException.class, () -> admin.describeTopics(Set.of(TOPIC_NAME)).allTopicNames().get());
            assertInstanceOf(TopicAuthorizationException.class, e.getCause());

            consumer.assign(List.of(TP, tp2));
            sendRecords(producer, NUM_RECORDS, tp2);
            var topic2RecordConsumed = new AtomicBoolean();
            assertThrows(TopicAuthorizationException.class, () -> {
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TestUtils.DEFAULT_MAX_WAIT_MS);
                while (System.nanoTime() < deadline) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        assertEquals(Set.of(tp2), records.partitions(), "Consumed records from an unauthorized topic");
                        topic2RecordConsumed.set(true);
                    }
                }
                fail("Consumer did not fail with an authorization exception");
            });

            setReadAndWriteAcls(context, TP);
            if (!topic2RecordConsumed.get()) {
                consumeRecords(consumer, NUM_RECORDS, 1, tp2, 10_000);
            }
            sendRecords(producer, NUM_RECORDS, TP);
            consumeRecords(consumer);
            var recoveredResults = admin.describeTopics(Set.of(TOPIC_NAME, tp2.topic())).topicNameValues();
            assertEquals(1, recoveredResults.get(TOPIC_NAME).get().partitions().size());
            assertEquals(1, recoveredResults.get(tp2.topic()).get().partitions().size());
        }

        public static void testNoProduceWithDescribeAcl(TestContext context, boolean idempotenceEnabled) throws Exception {
            createAcls(context, List.of(context.acl(new ResourcePattern(TOPIC, TOPIC_NAME, LITERAL), DESCRIBE)));
            try (var producer = context.producer(Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotenceEnabled))) {
                if (idempotenceEnabled) {
                    assertThrows(KafkaException.class, () -> sendRecords(producer, NUM_RECORDS, TP));
                } else {
                    var e = assertThrows(TopicAuthorizationException.class, () -> sendRecords(producer, NUM_RECORDS, TP));
                    assertEquals(Set.of(TOPIC_NAME), e.unauthorizedTopics());
                }
                confirmReauthenticationMetrics(context);
            }
        }

        public static void testNoConsumeWithoutDescribeAclViaAssign(TestContext context, GroupProtocol groupProtocol) throws Exception {
            noConsumeWithoutDescribeAclSetup(context);
            try (var consumer = context.consumer(groupProtocol)) {
                consumer.assign(List.of(TP));
                waitForPollThrowException(consumer, KafkaException.class);
                confirmReauthenticationMetrics(context);
            }
        }

        public static void testNoConsumeWithoutDescribeAclViaSubscribe(TestContext context, GroupProtocol groupProtocol) throws Exception {
            noConsumeWithoutDescribeAclSetup(context);
            try (var consumer = context.consumer(groupProtocol)) {
                consumer.subscribe(List.of(TOPIC_NAME));
                waitForPollThrowException(consumer, TopicAuthorizationException.class);

                setReadAndWriteAcls(context, TP);
                consumer.subscribe(List.of(TOPIC_NAME, "topic2"));
                waitForPollThrowException(consumer, TopicAuthorizationException.class);

                consumer.subscribe(List.of(TOPIC_NAME));
                consumeRecords(consumer);
            }
        }

        private static void noConsumeWithoutDescribeAclSetup(TestContext context) throws Exception {
            noConsumeWithDescribeAclSetup(context);
            var resource = new ResourcePattern(TOPIC, TOPIC_NAME, LITERAL);
            try (var admin = context.superuserAdmin()) {
                admin.deleteAcls(List.of(context.acl(resource, DESCRIBE).toFilter(), context.acl(resource, WRITE).toFilter())).all().get();
            }
            waitAcls(context, resource, Set.of(context.acl(resource, CREATE).entry()));
            var groupResource = new ResourcePattern(GROUP, GROUP_ID, LITERAL);
            waitAcls(context, groupResource, Set.of(context.acl(groupResource, READ).entry()));
        }

        public static void testNoConsumeWithDescribeAclViaAssign(TestContext context, GroupProtocol groupProtocol) throws Exception {
            noConsumeWithDescribeAclSetup(context);
            try (var consumer = context.consumer(groupProtocol)) {
                consumer.assign(List.of(TP));
                var e = waitForPollThrowException(consumer, TopicAuthorizationException.class);
                assertEquals(Set.of(TOPIC_NAME), e.unauthorizedTopics());
                confirmReauthenticationMetrics(context);
            }
        }

        public static void testNoConsumeWithDescribeAclViaSubscribe(TestContext context, GroupProtocol groupProtocol) throws Exception {
            noConsumeWithDescribeAclSetup(context);
            try (var consumer = context.consumer(groupProtocol)) {
                consumer.subscribe(List.of(TOPIC_NAME));
                var e = waitForPollThrowException(consumer, TopicAuthorizationException.class);
                assertEquals(Set.of(TOPIC_NAME), e.unauthorizedTopics());
                confirmReauthenticationMetrics(context);
            }
        }

        private static void noConsumeWithDescribeAclSetup(TestContext context) throws Exception {
            List<AclBinding> bindings = new ArrayList<>(topicAcls(context, WRITE, CREATE, DESCRIBE));
            bindings.add(context.acl(new ResourcePattern(GROUP, GROUP_ID, LITERAL), READ));
            createAcls(context, bindings);
            try (var producer = context.producer()) {
                sendRecords(producer, NUM_RECORDS, TP);
            }
        }

        public static void testNoGroupAcl(TestContext context, GroupProtocol groupProtocol) throws Exception {
            createAcls(context, topicAcls(context, WRITE, CREATE, DESCRIBE));
            try (var producer = context.producer(); var consumer = context.consumer(groupProtocol)) {
                sendRecords(producer, NUM_RECORDS, TP);
                consumer.assign(List.of(TP));
                var e = waitForPollThrowException(consumer, GroupAuthorizationException.class);
                assertEquals(GROUP_ID, e.groupId());
                confirmReauthenticationMetrics(context);
            }
        }

        private static List<AclBinding> topicAcls(TestContext context, AclOperation... operations) {
            return Arrays.stream(operations)
                    .map(operation -> context.acl(new ResourcePattern(ResourceType.TOPIC, TOPIC_NAME, LITERAL), operation))
                    .toList();
        }
    }
}
