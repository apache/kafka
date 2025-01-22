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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.GetReplicaLogInfoRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.GetReplicaLogInfoRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.TopicIdPartition;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecoveryManagerTest {
    static final int DEFAULT_BROKER_EPOCH = 10;

    static BrokerRegistration createBrokerRegistration(int brokerId) {
        return new BrokerRegistration.Builder().
                setId(brokerId).
                setEpoch(DEFAULT_BROKER_EPOCH).
                setRack(Optional.empty()).
                setDirectories(List.of()).
                setListeners(List.of(new Endpoint(MockRecoveryManagerContext.MOCK_LISTENER_NAME, SecurityProtocol.PLAINTEXT, MockRecoveryManagerContext.MOCK_HOST, MockRecoveryManagerContext.MOCK_PORT))).
                build();
    }

    static Node createNode(int brokerId) {
        return new Node(brokerId, MockRecoveryManagerContext.MOCK_HOST, MockRecoveryManagerContext.MOCK_PORT);
    }

    @Test
    public void testReceiveLogInfoRequestHappyTrail() {
        MockRecoveryManagerContext context = new MockRecoveryManagerContext();
        RecoveryManager recoveryManager = context.createRecoveryManager();

        List<RecoveryManager.TopicPartitionReplicas> commands = new ArrayList<>();
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), 1);
        commands.add(new RecoveryManager.TopicPartitionReplicas(tp1, new int[] {0, 1, 2}));
        Map<Integer, BrokerRegistration> brokers = new HashMap<>();
        brokers.put(0, createBrokerRegistration(0));
        brokers.put(1, createBrokerRegistration(1));
        brokers.put(2, createBrokerRegistration(2));

        recoveryManager.startRecovery(commands, brokers, 1000);
        assertEquals(3, context.requestThread.work.size());
        assertTrue(recoveryManager.machine.isFetchingState());
        context.replication.nextElectionResults = List.of(ApiError.NONE);
        ControllerResult<Void> controllerResult = null;
        for (int i = 0; i < context.requestThread.work.size(); i++) {
            RecoveryRequestThread.RequestAndReceiver rr = context.requestThread.work.get(i);
            assertNotNull(rr.receiver);
            assertNotNull(rr.request);
            GetReplicaLogInfoResponseData expectedData = new GetReplicaLogInfoResponseData();
            expectedData.setBrokerEpoch(DEFAULT_BROKER_EPOCH);
            GetReplicaLogInfoResponseData.PartitionLogInfo pli = new GetReplicaLogInfoResponseData.PartitionLogInfo().
                    setPartition(1).
                    setErrorCode(Errors.NONE.code()).
                    setLogEndOffset(10);
            expectedData.topicPartitionLogInfoList().add(new GetReplicaLogInfoResponseData.TopicPartitionLogInfo().
                    setTopicId(tp1.topicId()).
                    setPartitionLogInfo(List.of(pli)));
            RecoveryFetcher.Result result = new RecoveryFetcher.Result(RecoveryFetcher.ResultStatus.HasResults, expectedData, rr.request);
            rr.receiver.receive(result);
            MockRecoveryManagerContext.MockQueueEntry entry = context.queueAccessor.immediate.getLast();
            // We inserted this after the scheduled event, so it should be i + 1...
            entry.check("LogInfoReceivedEvent", i + 1, RecoveryManager.LogInfoReceivedEvent.class);
            RecoveryManager.LogInfoReceivedEvent event = (RecoveryManager.LogInfoReceivedEvent) entry.op;
            assertEquals(expectedData, event.result.response);
            controllerResult = event.get();
            // while we are running
            if (i < 2) {
                assertTrue(recoveryManager.machine.isFetchingState());
            }
        }
        assertNotNull(controllerResult);
        // 3 "response received"; no batched election
        assertEquals(3, context.queueAccessor.immediate.size());
        assertTrue(recoveryManager.machine.isDoneState());
    }

    @Test
    public void testTriggerElectionHappyTrail() {
        MockRecoveryManagerContext context = new MockRecoveryManagerContext();
        RecoveryManager recoveryManager = context.createRecoveryManager();

        List<RecoveryManager.TopicPartitionReplicas> commands = new ArrayList<>();
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), 1);
        commands.add(new RecoveryManager.TopicPartitionReplicas(tp1, new int[] {0, 1, 2}));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), 1);
        commands.add(new RecoveryManager.TopicPartitionReplicas(tp2, new int[] {0, 1, 2}));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), 1);
        commands.add(new RecoveryManager.TopicPartitionReplicas(tp3, new int[] {0, 1, 2}));

        Map<Integer, BrokerRegistration> brokers = new HashMap<>();
        brokers.put(0, createBrokerRegistration(0));
        brokers.put(1, createBrokerRegistration(1));
        brokers.put(2, createBrokerRegistration(2));

        recoveryManager.startRecovery(commands, brokers, 1000);

        assertEquals(1, context.queueAccessor.deferred.size());
        assertEquals(0, context.queueAccessor.immediate.size());
        MockRecoveryManagerContext.MockQueueEntry onlyEntry = context.queueAccessor.deferred.getLast();
        onlyEntry.check("unclean-recovery-stop-event-0", 0, RecoveryManager.StopFetchingEvent.class);

        assertEquals(3, context.requestThread.work.size());
        for (int i = 0; i < 3; i++) {
            int finalI = i;
            Optional<RecoveryFetcher.Request> maybeWork =
                    context.requestThread.work.
                            stream().
                            filter(w -> w.request.node.id() == finalI).
                            findFirst().
                            map(r -> r.request);
            assertTrue(maybeWork.isPresent());
            RecoveryFetcher.Request work = maybeWork.get();
            assertEquals(MockRecoveryManagerContext.MOCK_PORT, work.node.port());
            assertEquals("localhost", work.node.host());
            assertEquals(work.node.id(), finalI);
        }
    }

    @Test
    public void testAmoritizerHappyTrail() {
        RecoveryManager.RequestsAmortizer amortizer
                = new RecoveryManager.RequestsAmortizer();
        final int numberBrokers = 10;
        final int numTopics = 10;
        final int partsPerTopic = 10;
        TopicIdPartition[] tips = new TopicIdPartition[numTopics * partsPerTopic];
        for (int i = 0; i < numTopics; i++) {
            Uuid topicId = Uuid.randomUuid();
            for (int j = 0; j < partsPerTopic; j++) {
                tips[i * partsPerTopic + j] = new TopicIdPartition(topicId, j);
                for (int k = 0; k < numberBrokers; k++) {
                    amortizer.addTopic(k, tips[i * partsPerTopic + j]);
                }
            }
        }

        List<List<GetReplicaLogInfoRequestData>> requests = amortizer.buildRequests();
        assertEquals(numberBrokers, requests.size());

        Map<Integer, GetReplicaLogInfoRequestData> brokerRequestMap = new HashMap<>();
        for (List<GetReplicaLogInfoRequestData> brokerRequests : requests) {
            // no duplicates
            assertEquals(1, brokerRequests.size());
            GetReplicaLogInfoRequestData data = brokerRequests.get(0);
            assertNull(brokerRequestMap.get(data.brokerId()));
            assertEquals(numTopics, data.topicPartitions().size());
            brokerRequestMap.put(data.brokerId(), data);
        }

        assertEquals(numberBrokers, brokerRequestMap.size());
        // Tests that every topic partition is sent to every broker
        // 1. tests that every TopicPartition will be requested from all brokers
        // 2. tests that the expected # of topic-partitions are present on a node
        for (int i = 0; i < tips.length; i++) {
            for (int j = 0; j < numberBrokers; j++) {
                GetReplicaLogInfoRequestData datum = brokerRequestMap.get(j);
                assertNotNull(datum);
                boolean found = false;
                int partitionsCount = 0;
                for (GetReplicaLogInfoRequestData.TopicPartitions tp : datum.topicPartitions()) {
                    partitionsCount += tp.partitions().size();
                    if (tp.topicId().equals(tips[i].topicId())) {
                        for (Integer p: tp.partitions()) {
                            if (p == tips[i].partitionId()) {
                                found = true;
                                break;
                            }
                        }
                    }
                }
                assertEquals(numTopics * partsPerTopic, partitionsCount);
                assertTrue(found);
            }
        }
    }

    @Test
    public void testAmoritizerRespectsRequestLimit() {
        RecoveryManager.RequestsAmortizer amortizer
                = new RecoveryManager.RequestsAmortizer();
        Uuid uuid = Uuid.randomUuid();
        for (int i = 0; i < 2 * GetReplicaLogInfoRequest.MAX_PARTITIONS_PER_REQUEST; i++) {
            amortizer.addTopic(0, new TopicIdPartition(uuid, i));
        }
        Map<Integer, Boolean> seen = new HashMap<>();
        IntStream.range(0, 2 * GetReplicaLogInfoRequest.MAX_PARTITIONS_PER_REQUEST).forEach(i -> {
            seen.put(i, false);
        });
        List<List<GetReplicaLogInfoRequestData>> requests = amortizer.buildRequests();
        assertEquals(1, requests.size());
        for (List<GetReplicaLogInfoRequestData> brokerRequests : requests) {
            for (GetReplicaLogInfoRequestData datum : brokerRequests) {
                assertEquals(1, datum.topicPartitions().size());
                GetReplicaLogInfoRequestData.TopicPartitions tp = datum.topicPartitions().get(0);
                assertEquals(GetReplicaLogInfoRequest.MAX_PARTITIONS_PER_REQUEST, tp.partitions().size());
                assertEquals(uuid, tp.topicId());
                for (Integer p: tp.partitions()) {
                    assertTrue(seen.containsKey(p));
                    // Each partition should be uniquely allocated to a single request.
                    assertFalse(seen.get(p));
                    seen.put(p, true);
                }
            }
        }
        // Check that every single partition was noted
        assertTrue(seen.values().stream().allMatch(Boolean::booleanValue));
    }
}
