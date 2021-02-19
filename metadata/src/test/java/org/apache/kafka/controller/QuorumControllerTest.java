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

package org.apache.kafka.controller;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.Listener;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.controller.BrokersToIsrs.TopicPartition;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metalog.LocalLogManagerTestEnv;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.BROKER0;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.CONFIGS;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class QuorumControllerTest {
    private static final Logger log =
        LoggerFactory.getLogger(QuorumControllerTest.class);

    /**
     * Test creating a new QuorumController and closing it.
     */
    @Test
    public void testCreateAndClose() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1)) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, __ -> { })) {
            }
        }
    }

    /**
     * Test setting some configuration values and reading them back.
     */
    @Test
    public void testConfigurationOperations() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1)) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS))) {
                testConfigurationOperations(controlEnv.activeController());
            }
        }
    }

    private void testConfigurationOperations(QuorumController controller) throws Throwable {
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE),
            controller.incrementalAlterConfigs(Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), true).get());
        assertEquals(Collections.singletonMap(BROKER0,
            new ResultOrError<>(Collections.emptyMap())),
            controller.describeConfigs(Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE),
            controller.incrementalAlterConfigs(Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), false).get());
        assertEquals(Collections.singletonMap(BROKER0, new ResultOrError<>(Collections.
                singletonMap("baz", "123"))),
            controller.describeConfigs(Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
    }

    /**
     * Test that an incrementalAlterConfigs operation doesn't complete until the records
     * can be written to the metadata log.
     */
    @Test
    public void testDelayedConfigurationOperations() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1)) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS))) {
                testDelayedConfigurationOperations(logEnv, controlEnv.activeController());
            }
        }
    }

    private void testDelayedConfigurationOperations(LocalLogManagerTestEnv logEnv,
                                                    QuorumController controller)
                                                    throws Throwable {
        logEnv.logManagers().forEach(m -> m.setMaxReadOffset(0L));
        CompletableFuture<Map<ConfigResource, ApiError>> future1 =
            controller.incrementalAlterConfigs(Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), false);
        assertFalse(future1.isDone());
        assertEquals(Collections.singletonMap(BROKER0,
            new ResultOrError<>(Collections.emptyMap())),
            controller.describeConfigs(Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
        logEnv.logManagers().forEach(m -> m.setMaxReadOffset(1L));
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE), future1.get());
    }

    @Test
    public void testUnregisterBroker() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1)) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS))) {
                ListenerCollection listeners = new ListenerCollection();
                listeners.add(new Listener().setName("PLAINTEXT").
                    setHost("localhost").setPort(9092));
                QuorumController active = controlEnv.activeController();
                CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                    new BrokerRegistrationRequestData().
                        setBrokerId(0).
                        setClusterId(Uuid.fromString("06B-K3N1TBCNYFgruEVP0Q")).
                        setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwBA")).
                        setListeners(listeners));
                assertEquals(0L, reply.get().epoch());
                CreateTopicsRequestData createTopicsRequestData =
                    new CreateTopicsRequestData().setTopics(
                        new CreatableTopicCollection(Collections.singleton(
                            new CreatableTopic().setName("foo").setNumPartitions(1).
                                setReplicationFactor((short) 1)).iterator()));
                // TODO: place on a fenced broker if we have no choice
                assertEquals(Errors.INVALID_REPLICATION_FACTOR.code(), active.createTopics(
                    createTopicsRequestData).get().topics().find("foo").errorCode());
                assertEquals(new BrokerHeartbeatReply(true, false, false, false),
                    active.processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                            setWantFence(false).setBrokerEpoch(0L).setBrokerId(0).
                            setCurrentMetadataOffset(100000L)).get());
                assertEquals(Errors.NONE.code(), active.createTopics(
                    createTopicsRequestData).get().topics().find("foo").errorCode());
                CompletableFuture<TopicPartition> topicPartitionFuture = active.appendReadEvent(
                    "debugGetPartition", () -> {
                        Iterator<TopicPartition> iterator = active.
                            replicationControl().brokersToIsrs().iterator(0, true);
                        assertTrue(iterator.hasNext());
                        return iterator.next();
                    });
                assertEquals(0, topicPartitionFuture.get().partitionId());
                active.unregisterBroker(0).get();
                topicPartitionFuture = active.appendReadEvent(
                    "debugGetPartition", () -> {
                        Iterator<TopicPartition> iterator = active.
                            replicationControl().brokersToIsrs().noLeaderIterator();
                        assertTrue(iterator.hasNext());
                        return iterator.next();
                    });
                assertEquals(0, topicPartitionFuture.get().partitionId());
            }
        }
    }
}
