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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One smoke test per {@link KafkaProtocolFaultProxy} fault: every API registered in its {@code ERROR_SETTERS}
 * (transactional producer, plain producer, and consumer paths), plus {@code disconnectOn}, {@code delayResponse},
 * and {@code blackholeOn}. Each test arms exactly one fault and asserts the fault-specific, client-visible
 * consequence (a thrown exception, a slowed-but-successful call, or a client-side timeout).
 *
 * <p>All client-side timeouts here (session/heartbeat/request/api timeouts) are configured far below their
 * defaults so that the tests that legitimately wait for a timeout (the blackhole case) or a fixed delay (the
 * delay case) do so in a couple of seconds rather than the default tens-of-seconds-to-minutes.
 */
@Tag("integration")
@Timeout(60)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProxyFaultAllErrorsSmokeTest {

    // Deliberately far below the client defaults (30s/60s/120s/45s/3s) so a test that has to wait out a real
    // timeout (blackhole) or a fixed delay (delayResponse) still finishes in a couple of seconds.
    private static final int SHORT_REQUEST_TIMEOUT_MS = 5_000;
    private static final int SHORT_API_TIMEOUT_MS = 8_000;
    private static final int VERY_SHORT_REQUEST_TIMEOUT_MS = 1_000;
    private static final int VERY_SHORT_API_TIMEOUT_MS = 2_000;
    private static final int SESSION_TIMEOUT_MS = 6_000; // the broker's group.min.session.timeout.ms floor
    private static final int HEARTBEAT_INTERVAL_MS = 1_000;
    private static final int MAX_BLOCK_MS = 5_000;
    private static final int DELIVERY_TIMEOUT_MS = 10_000;
    private static final int TRANSACTION_TIMEOUT_MS = 10_000;

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;

    @BeforeAll
    public void startClusterAndProxy() {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());
    }

    @AfterAll
    public void stopClusterAndProxy() {
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @BeforeEach
    public void resetFaults() {
        proxy.clearFaults();
    }

    // ------------------------------------------------------------------
    // injectError: plain producer / consumer data-plane APIs
    // ------------------------------------------------------------------

    @Test
    public void produceErrorIsSurfacedToTheProducer(final TestInfo info) throws Exception {
        final String topic = newTopic(info);
        proxy.injectError(ApiKeys.PRODUCE, Errors.TOPIC_AUTHORIZATION_FAILED).once();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig())) {
            final ExecutionException ex = assertThrows(ExecutionException.class,
                () -> producer.send(new ProducerRecord<>(topic, "k", "v")).get());
            assertInstanceOf(TopicAuthorizationException.class, ex.getCause(), "expected TopicAuthorizationException, got " + ex.getCause());
        }
    }

    @Test
    public void fetchErrorIsSurfacedToTheConsumer(final TestInfo info) throws Exception {
        final String topic = newTopic(info);
        final TopicPartition tp = new TopicPartition(topic, 0);
        proxy.injectError(ApiKeys.FETCH, Errors.TOPIC_AUTHORIZATION_FAILED).once();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(uniqueGroupId(info)))) {
            consumer.assign(singletonList(tp));
            assertPollEventuallyThrows(consumer, TopicAuthorizationException.class);
        }
    }

    @Test
    public void offsetCommitErrorIsSurfacedToTheConsumer(final TestInfo info) throws Exception {
        final String topic = newTopic(info);
        final TopicPartition tp = new TopicPartition(topic, 0);
        proxy.injectError(ApiKeys.OFFSET_COMMIT, Errors.TOPIC_AUTHORIZATION_FAILED).once();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(uniqueGroupId(info)))) {
            consumer.assign(singletonList(tp));
            assertThrows(TopicAuthorizationException.class,
                () -> consumer.commitSync(Map.of(tp, new OffsetAndMetadata(0L))));
        }
    }

    @Test
    public void listOffsetsErrorIsSurfacedToTheConsumer(final TestInfo info) throws Exception {
        final String topic = newTopic(info);
        final TopicPartition tp = new TopicPartition(topic, 0);
        proxy.injectError(ApiKeys.LIST_OFFSETS, Errors.TOPIC_AUTHORIZATION_FAILED).once();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(uniqueGroupId(info)))) {
            assertThrows(TopicAuthorizationException.class,
                () -> consumer.endOffsets(Collections.singleton(tp)));
        }
    }

    // ------------------------------------------------------------------
    // injectError: consumer group protocol (classic: JoinGroup/SyncGroup/Heartbeat)
    // ------------------------------------------------------------------

    @Test
    public void joinGroupErrorIsSurfacedToTheConsumer(final TestInfo info) throws Exception {
        final String topic = newTopic(info);
        proxy.injectError(ApiKeys.JOIN_GROUP, Errors.GROUP_AUTHORIZATION_FAILED).once();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(uniqueGroupId(info)))) {
            consumer.subscribe(singletonList(topic));
            assertPollEventuallyThrows(consumer, GroupAuthorizationException.class);
        }
    }

    @Test
    public void syncGroupErrorIsSurfacedToTheConsumer(final TestInfo info) throws Exception {
        final String topic = newTopic(info);
        proxy.injectError(ApiKeys.SYNC_GROUP, Errors.GROUP_AUTHORIZATION_FAILED).once();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(uniqueGroupId(info)))) {
            consumer.subscribe(singletonList(topic));
            assertPollEventuallyThrows(consumer, GroupAuthorizationException.class);
        }
    }

    @Test
    public void heartbeatErrorIsSurfacedToTheConsumer(final TestInfo info) throws Exception {
        // Unlike Join/SyncGroup, the classic consumer treats a failed Heartbeat response as retriable background
        // noise (AbstractCoordinator.HeartbeatThread swallows it via heartbeat.failHeartbeat(), it never reaches
        // the application as a thrown exception) - by design, so one bad heartbeat can't kill a healthy member.
        // Observing its real consequence (persistent failures eventually forcing a session timeout + rebalance)
        // takes several multiples of the broker's session-timeout floor, which is too slow/variable for a smoke
        // test. Talk raw wire protocol instead, with a real group's memberId/generationId, to directly prove the
        // fault-injection path itself (which is what this test is actually about) without waiting on it.
        final String topic = newTopic(info);
        final ConsumerGroupMetadata groupMetadata;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig(uniqueGroupId(info)))) {
            consumer.subscribe(singletonList(topic));
            consumer.poll(Duration.ofSeconds(5)); // real join, so memberId/generationId below are valid
            groupMetadata = consumer.groupMetadata();

            final HeartbeatRequestData data = new HeartbeatRequestData()
                .setGroupId(groupMetadata.groupId())
                .setGenerationId(groupMetadata.generationId())
                .setMemberId(groupMetadata.memberId());
            final HeartbeatRequest request =
                new HeartbeatRequest.Builder(data).build(ApiKeys.HEARTBEAT.latestVersion());

            proxy.injectError(ApiKeys.HEARTBEAT, Errors.GROUP_AUTHORIZATION_FAILED).once();
            final HeartbeatResponse response = (HeartbeatResponse) sendRawRequest(request);
            assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), response.data().errorCode());
        }
    }

    // ------------------------------------------------------------------
    // injectError: transactional producer path
    // ------------------------------------------------------------------

    @Test
    public void initProducerIdErrorIsSurfacedToTheProducer(final TestInfo info) {
        proxy.injectError(ApiKeys.INIT_PRODUCER_ID, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED).once();

        final KafkaProducer<String, String> producer = new KafkaProducer<>(transactionalProducerConfig(info));
        try {
            assertThrows(TransactionalIdAuthorizationException.class, producer::initTransactions);
        } finally {
            closeQuietly(producer);
        }
    }

    @Test
    public void addOffsetsToTxnErrorIsSurfacedToTheProducer(final TestInfo info) throws Exception {
        // Under Transaction Protocol V2 (the default here), TransactionManager.sendOffsetsToTransaction()
        // skips AddOffsetsToTxn entirely and folds its purpose into TxnOffsetCommit - so a real producer never
        // sends this request and can't exercise the fault. Talk raw wire protocol instead, straight through the
        // proxy, to prove the fault-injection path itself (which is what this test is actually about).
        final AddOffsetsToTxnRequestData data = new AddOffsetsToTxnRequestData()
            .setTransactionalId("txn-" + safeUniqueTestName(info))
            .setProducerId(1L)
            .setProducerEpoch((short) 0)
            .setGroupId("g-" + safeUniqueTestName(info));
        final AddOffsetsToTxnRequest request =
            new AddOffsetsToTxnRequest.Builder(data).build(ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion());

        proxy.injectError(ApiKeys.ADD_OFFSETS_TO_TXN, Errors.GROUP_AUTHORIZATION_FAILED).once();
        final AddOffsetsToTxnResponse response = (AddOffsetsToTxnResponse) sendRawRequest(request);
        assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), response.data().errorCode());
    }

    @Test
    public void endTxnErrorIsSurfacedToTheProducer(final TestInfo info) throws Exception {
        final String topic = newTopic(info);
        final KafkaProducer<String, String> producer = new KafkaProducer<>(transactionalProducerConfig(info));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "k", "v")).get();

            proxy.injectError(ApiKeys.END_TXN, Errors.INVALID_TXN_STATE).once();
            assertThrows(InvalidTxnStateException.class, producer::commitTransaction);
        } finally {
            closeQuietly(producer);
        }
    }

    // ------------------------------------------------------------------
    // disconnectOn / delayResponse / blackholeOn
    // ------------------------------------------------------------------

    @Test
    public void disconnectIsRetriedTransparentlyByAnIdempotentProducer(final TestInfo info) throws Exception {
        final String topic = newTopic(info);
        final var rule = proxy.disconnectOn(ApiKeys.PRODUCE).once();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig())) {
            // idempotence is on by default: the broker-side write already succeeded before the connection was
            // dropped, so the retried send is deduplicated and this still completes successfully.
            producer.send(new ProducerRecord<>(topic, "k", "v")).get();
        }
        assertTrue(rule.timesTriggered() >= 1, "expected the disconnect fault to have fired at least once");
    }

    @Test
    public void delayResponseSlowsTheCallDownButItStillSucceeds() throws Exception {
        final Duration delay = Duration.ofMillis(1_500);
        proxy.delayResponse(ApiKeys.METADATA, delay).once();

        try (Admin admin = Admin.create(adminConfig(SHORT_REQUEST_TIMEOUT_MS, SHORT_API_TIMEOUT_MS))) {
            final long start = System.nanoTime();
            admin.describeCluster().clusterId().get();
            final long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs >= delay.toMillis() - 100,
                "expected the call to take at least ~" + delay.toMillis() + "ms, took " + elapsedMs + "ms");
        }
    }

    @Test
    public void blackholeCausesAClientSideTimeoutInsteadOfHanging() {
        proxy.blackholeOn(ApiKeys.METADATA).everyTime();

        try (Admin admin = Admin.create(adminConfig(VERY_SHORT_REQUEST_TIMEOUT_MS, VERY_SHORT_API_TIMEOUT_MS))) {
            final long start = System.nanoTime();
            final ExecutionException ex = assertThrows(ExecutionException.class,
                () -> admin.describeCluster().clusterId().get());
            final long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertInstanceOf(TimeoutException.class, ex.getCause(), "expected a client-side TimeoutException, got " + ex.getCause());
            assertTrue(elapsedMs < 10_000,
                "expected the blackholed call to fail fast via its own (shortened) timeout, took " + elapsedMs + "ms");
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private String newTopic(final TestInfo info) throws InterruptedException {
        final String topic = "t-" + safeUniqueTestName(info);
        cluster.createTopic(topic, 1, 1);
        return topic;
    }

    private String uniqueGroupId(final TestInfo info) {
        return "g-" + safeUniqueTestName(info);
    }

    private Properties adminConfig(final int requestTimeoutMs, final int apiTimeoutMs) {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, apiTimeoutMs);
        return props;
    }

    private Properties producerConfig() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, SHORT_REQUEST_TIMEOUT_MS);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        return props;
    }

    private Properties transactionalProducerConfig(final TestInfo info) {
        final Properties props = producerConfig();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-" + safeUniqueTestName(info));
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TRANSACTION_TIMEOUT_MS);
        return props;
    }

    private Properties consumerConfig(final String groupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, SHORT_REQUEST_TIMEOUT_MS);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, SHORT_API_TIMEOUT_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_INTERVAL_MS);
        return props;
    }

    /** Polls in a bounded loop until either the expected exception is thrown or the deadline passes. */
    private static <T extends RuntimeException> void assertPollEventuallyThrows(
            final KafkaConsumer<?, ?> consumer, final Class<T> expected) throws InterruptedException {
        final AtomicReference<RuntimeException> caught = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            try {
                consumer.poll(Duration.ofMillis(200));
                return false;
            } catch (final RuntimeException e) {
                caught.set(e);
                return true;
            }
        }, 10_000, "expected a " + expected.getSimpleName() + " while polling");
        assertNotNull(caught.get());
        assertTrue(expected.isInstance(caught.get()), "expected " + expected.getSimpleName() + ", got " + caught.get());
    }

    /** Sends one request directly over the wire to the proxy and decodes its response, bypassing any client. */
    private AbstractResponse sendRawRequest(final AbstractRequest request) throws Exception {
        final String[] hostPort = proxy.bootstrapServers().split(":");
        try (Socket socket = new Socket(hostPort[0], Integer.parseInt(hostPort[1]))) {
            socket.setSoTimeout(SHORT_REQUEST_TIMEOUT_MS);
            final RequestHeader header = new RequestHeader(request.apiKey(), request.version(), "smoke-test-raw", 1);
            final ByteBuffer requestBuffer = RequestUtils.serialize(
                    header.data(), header.headerVersion(), request.data(), request.version());
            final byte[] payload = new byte[requestBuffer.remaining()];
            requestBuffer.get(payload);
            final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();

            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final byte[] frame = new byte[in.readInt()];
            in.readFully(frame);
            return AbstractResponse.parseResponse(ByteBuffer.wrap(frame), header);
        }
    }

    private static void closeQuietly(final KafkaProducer<?, ?> producer) {
        try {
            producer.close(Duration.ofSeconds(5));
        } catch (final RuntimeException ignored) {
            // best-effort cleanup; the test's assertion already ran
        }
    }
}
