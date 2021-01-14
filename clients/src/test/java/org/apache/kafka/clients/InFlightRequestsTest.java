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

package org.apache.kafka.clients;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InFlightRequestsTest {

    private InFlightRequests inFlightRequests;
    private int correlationId;
    private String dest = "dest";

    @BeforeEach
    public void setup() {
        inFlightRequests = new InFlightRequests(12);
        correlationId = 0;
    }

    @Test
    public void testCompleteLastSent() {
        int correlationId1 = addRequest(dest);
        int correlationId2 = addRequest(dest);
        assertEquals(2, inFlightRequests.count());

        assertEquals(correlationId2, inFlightRequests.completeLastSent(dest).header.correlationId());
        assertEquals(1, inFlightRequests.count());

        assertEquals(correlationId1, inFlightRequests.completeLastSent(dest).header.correlationId());
        assertEquals(0, inFlightRequests.count());
    }

    @Test
    public void testClearAll() {
        int correlationId1 = addRequest(dest);
        int correlationId2 = addRequest(dest);

        List<NetworkClient.InFlightRequest> clearedRequests = TestUtils.toList(this.inFlightRequests.clearAll(dest));
        assertEquals(0, inFlightRequests.count());
        assertEquals(2, clearedRequests.size());
        assertEquals(correlationId1, clearedRequests.get(0).header.correlationId());
        assertEquals(correlationId2, clearedRequests.get(1).header.correlationId());
    }

    @Test
    public void testTimedOutNodes() {
        Time time = new MockTime();

        addRequest("A", time.milliseconds(), 50);
        addRequest("B", time.milliseconds(), 200);
        addRequest("B", time.milliseconds(), 100);

        time.sleep(50);
        assertEquals(Collections.emptyList(), inFlightRequests.nodesWithTimedOutRequests(time.milliseconds()));

        time.sleep(25);
        assertEquals(Collections.singletonList("A"), inFlightRequests.nodesWithTimedOutRequests(time.milliseconds()));

        time.sleep(50);
        assertEquals(Arrays.asList("A", "B"), inFlightRequests.nodesWithTimedOutRequests(time.milliseconds()));
    }

    @Test
    public void testCompleteNext() {
        int correlationId1 = addRequest(dest);
        int correlationId2 = addRequest(dest);
        assertEquals(2, inFlightRequests.count());

        assertEquals(correlationId1, inFlightRequests.completeNext(dest).header.correlationId());
        assertEquals(1, inFlightRequests.count());

        assertEquals(correlationId2, inFlightRequests.completeNext(dest).header.correlationId());
        assertEquals(0, inFlightRequests.count());
    }

    @Test
    public void testCompleteNextThrowsIfNoInflights() {
        assertThrows(IllegalStateException.class, () -> inFlightRequests.completeNext(dest));
    }

    @Test
    public void testCompleteLastSentThrowsIfNoInFlights() {
        assertThrows(IllegalStateException.class, () -> inFlightRequests.completeLastSent(dest));
    }

    private int addRequest(String destination) {
        return addRequest(destination, 0, 10000);
    }

    private int addRequest(String destination, long sendTimeMs, int requestTimeoutMs) {
        int correlationId = this.correlationId;
        this.correlationId += 1;

        RequestHeader requestHeader = new RequestHeader(ApiKeys.METADATA, (short) 0, "clientId", correlationId);
        NetworkClient.InFlightRequest ifr = new NetworkClient.InFlightRequest(requestHeader, requestTimeoutMs, 0,
                destination, null, false, false, null, null, sendTimeMs);
        inFlightRequests.add(ifr);
        return correlationId;
    }

}
