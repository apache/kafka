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
package org.apache.kafka.server;

import kafka.server.BrokerServer;

import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.ProducerIdsBlock;

import java.io.IOException;
import java.util.Map;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = Type.KRAFT)
public class AllocateProducerIdsRequestTest {
    private final ClusterInstance cluster;

    AllocateProducerIdsRequestTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testAllocateProducerIdsSentToController() throws IOException {
        var broker = broker();
        var response = allocateProducerIds(activeControllerId(), broker.config().brokerId(), broker.lifecycleManager().brokerEpoch());
        assertSuccessfulAllocation(response);
    }

    @ClusterTest(controllers = 3)
    public void testAllocateProducerIdsSentToStandbyController() throws IOException {
        var broker = broker();
        int activeControllerId = activeControllerId();
        int standbyControllerId = cluster.controllerIds().stream()
            .filter(id -> id != activeControllerId)
            .findFirst()
            .orElseThrow();

        var response = allocateProducerIds(standbyControllerId, broker.config().brokerId(), broker.lifecycleManager().brokerEpoch());
        assertEquals(Errors.NOT_CONTROLLER, response.error());
        assertEquals(0, response.data().producerIdLen());
    }

    @ClusterTest
    public void testAllocateProducerIdsWithInvalidBrokerEpoch() throws IOException {
        var broker = broker();
        int controllerId = activeControllerId();
        int brokerId = broker.config().brokerId();
        long brokerEpoch = broker.lifecycleManager().brokerEpoch();
        var previous = allocateProducerIds(controllerId, brokerId, brokerEpoch);
        assertSuccessfulAllocation(previous);

        var response = allocateProducerIds(controllerId, brokerId, brokerEpoch + 1);
        assertEquals(Errors.STALE_BROKER_EPOCH, response.error());
        assertEquals(0, response.data().producerIdLen());

        // A rejected request must not consume a block.
        assertConsecutiveAllocations(previous, allocateProducerIds(controllerId, brokerId, brokerEpoch));
    }

    @ClusterTest
    public void testAllocateProducerIdsWithUnknownBroker() throws IOException {
        var broker = broker();
        int controllerId = activeControllerId();
        int brokerId = broker.config().brokerId();
        long brokerEpoch = broker.lifecycleManager().brokerEpoch();
        int unknownBrokerId = cluster.brokerIds().stream().mapToInt(Integer::intValue).max().orElseThrow() + 1;
        var previous = allocateProducerIds(controllerId, brokerId, brokerEpoch);
        assertSuccessfulAllocation(previous);

        var response = allocateProducerIds(controllerId, unknownBrokerId, brokerEpoch);
        assertEquals(Errors.STALE_BROKER_EPOCH, response.error());
        assertEquals(0, response.data().producerIdLen());

        // A rejected request must not consume a block.
        assertConsecutiveAllocations(previous, allocateProducerIds(controllerId, brokerId, brokerEpoch));
    }

    @ClusterTest(controllers = 3)
    public void testAllocateProducerIdsAfterControllerFailover() throws Exception {
        var broker = broker();
        int oldControllerId = activeControllerId();
        int brokerId = broker.config().brokerId();
        long brokerEpoch = broker.lifecycleManager().brokerEpoch();
        var previous = allocateProducerIds(oldControllerId, brokerId, brokerEpoch);
        assertSuccessfulAllocation(previous);

        cluster.controllers().get(oldControllerId).shutdown();
        var remainingControllers = cluster.controllers().entrySet().stream()
            .filter(entry -> entry.getKey() != oldControllerId)
            .toList();
        waitForCondition(() -> remainingControllers.stream().anyMatch(entry -> entry.getValue().controller().isActive()),
            30_000, "Timed out waiting for a new active controller");
        int newControllerId = remainingControllers.stream()
            .filter(entry -> entry.getValue().controller().isActive())
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow();

        // The allocation is committed to the metadata log, so the new controller must not reuse it.
        var next = allocateProducerIds(newControllerId, brokerId, brokerEpoch);
        assertSuccessfulAllocation(next);
        assertTrue(next.data().producerIdStart() >= previous.data().producerIdStart() + previous.data().producerIdLen(),
            "The new controller must not reuse previously allocated producer IDs");
    }

    @ClusterTest
    public void testAllocateProducerIdsAfterBrokerRestart() throws Exception {
        var broker = broker();
        int controllerId = activeControllerId();
        int brokerId = broker.config().brokerId();
        long oldBrokerEpoch = broker.lifecycleManager().brokerEpoch();
        var previous = allocateProducerIds(controllerId, brokerId, oldBrokerEpoch);
        assertSuccessfulAllocation(previous);

        cluster.restartBroker(brokerId, Map.of());
        cluster.waitForReadyBrokers();
        var restartedBroker = (BrokerServer) cluster.brokers().get(brokerId);
        long newBrokerEpoch = restartedBroker.lifecycleManager().brokerEpoch();
        assertTrue(newBrokerEpoch > oldBrokerEpoch, "The restarted broker must have a new epoch");

        var staleResponse = allocateProducerIds(controllerId, brokerId, oldBrokerEpoch);
        assertEquals(Errors.STALE_BROKER_EPOCH, staleResponse.error());
        assertEquals(0, staleResponse.data().producerIdLen());

        assertConsecutiveAllocations(previous, allocateProducerIds(controllerId, brokerId, newBrokerEpoch));
    }

    private BrokerServer broker() {
        return (BrokerServer) cluster.brokers().values().iterator().next();
    }

    private int activeControllerId() {
        return cluster.controllers().entrySet().stream()
            .filter(entry -> entry.getValue().controller().isActive())
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow();
    }

    private AllocateProducerIdsResponse allocateProducerIds(int controllerId, int brokerId, long brokerEpoch) throws IOException {
        var request = new AllocateProducerIdsRequest.Builder(new AllocateProducerIdsRequestData()
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch)).build();
        int port = cluster.controllers().get(controllerId).socketServer().boundPort(cluster.controllerListenerName());
        return IntegrationTestUtils.connectAndReceive(request, port);
    }

    private static void assertSuccessfulAllocation(AllocateProducerIdsResponse response) {
        assertEquals(Errors.NONE, response.error());
        assertEquals(ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE, response.data().producerIdLen());
        assertTrue(response.data().producerIdStart() >= 0);
    }

    private static void assertConsecutiveAllocations(AllocateProducerIdsResponse previous, AllocateProducerIdsResponse next) {
        assertSuccessfulAllocation(next);
        // These tests have no producers, so blocks handed out by the same controller must be contiguous.
        assertEquals(previous.data().producerIdStart() + previous.data().producerIdLen(), next.data().producerIdStart());
    }
}
