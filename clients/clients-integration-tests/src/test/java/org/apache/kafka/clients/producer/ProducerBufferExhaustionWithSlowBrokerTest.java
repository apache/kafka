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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(key = "authorizer.class.name",
            value = "org.apache.kafka.clients.producer.ProducerBufferExhaustionWithSlowBrokerTest$DelayingProduceAuthorizer"),
        @ClusterConfigProperty(key = ProducerBufferExhaustionWithSlowBrokerTest.DelayingProduceAuthorizer.DELAY_MS_CONFIG, value = "3000"),
    }
)
public class ProducerBufferExhaustionWithSlowBrokerTest {

    private static final String TOPIC = "produce-delay";
    private static final int PARTITIONS = 8;
    private static final byte[] KEY = "k".getBytes();
    private static final byte[] VALUE = new byte[100];

    private static final int BATCH_SIZE = 256 * 1024;
    // Holds only 2 full batches (256 KB) but all 8 incremental chunks (16 KB = 128 KB).
    private static final long BUFFER_MEMORY = BATCH_SIZE * 2;
    // Realistic and non-zero, but smaller than the broker's 3 s produce delay: full can't free in time.
    private static final int MAX_BLOCK_MS = 2000;

    @ClusterTest
    public void testFullExhaustsButIncrementalFitsWithSlowBroker(ClusterInstance cluster) throws Exception {
        cluster.createTopic(TOPIC, PARTITIONS, (short) 1);

        // verifying that allocation strategy full faces buffer exhaustion as a baseline
        try (Producer<byte[], byte[]> producer = cluster.producer(config(ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_FULL))) {
            ExecutionException exhausted = assertThrows(ExecutionException.class, () -> produceToAllPartitions(producer),
                "full should exhaust buffer.memory across many partitions while the broker is slow to ack");
            assertInstanceOf(BufferExhaustedException.class, exhausted.getCause());
        }

        // verifying that allocation strategy incremental is able to send the same load without exhausting the buffer
        try (Producer<byte[], byte[]> producer = cluster.producer(config(ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL))) {
            produceToAllPartitions(producer);
        }
    }

    /**
     * Produce one record to each partition, then wait for every send to complete.
     */
    private void produceToAllPartitions(Producer<byte[], byte[]> producer) throws Exception {
        // warm up metadata; only WRITE is delayed, so this returns promptly
        producer.partitionsFor(TOPIC);

        List<Future<RecordMetadata>> futures = new ArrayList<>(PARTITIONS);
        for (int p = 0; p < PARTITIONS; p++) {
            futures.add(producer.send(new ProducerRecord<>(TOPIC, p, KEY, VALUE)));
        }
        for (Future<RecordMetadata> future : futures) {
            future.get();
        }
    }

    private Map<String, Object> config(String allocationStrategy) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG, allocationStrategy);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFER_MEMORY);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        configs.put(ProducerConfig.ACKS_CONFIG, "1");
        return configs;
    }

    /**
     * Allows everything, but sleeps {@value #DELAY_MS_CONFIG} ms before each {@code WRITE} so the produce response
     * is delayed independently of batch size — a stand-in for a high-latency broker.
     */
    public static class DelayingProduceAuthorizer implements Authorizer {

        public static final String DELAY_MS_CONFIG = "produce.authorizer.delay.ms";

        private volatile long delayMs = 0L;

        @Override
        public void configure(Map<String, ?> configs) {
            Object value = configs.get(DELAY_MS_CONFIG);
            if (value != null)
                delayMs = Long.parseLong(value.toString());
        }

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            Map<Endpoint, CompletableFuture<Void>> futures = new HashMap<>();
            for (Endpoint endpoint : serverInfo.endpoints())
                futures.put(endpoint, CompletableFuture.completedFuture(null));
            return futures;
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
            List<AuthorizationResult> results = new ArrayList<>(actions.size());
            for (Action action : actions) {
                if (delayMs > 0 && action.operation() == AclOperation.WRITE) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                results.add(AuthorizationResult.ALLOWED);
            }
            return results;
        }

        @Override
        public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
            return List.of();
        }

        @Override
        public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
            return List.of();
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return List.of();
        }

        @Override
        public void close() {
        }
    }
}
