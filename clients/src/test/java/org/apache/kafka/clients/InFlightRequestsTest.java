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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InFlightRequestsTest {

    private InFlightRequests inFlightRequests;
    @Before
    public void setup() {
        inFlightRequests = new InFlightRequests(12);
        NetworkClient.InFlightRequest ifr =
                new NetworkClient.InFlightRequest(null, 0, "dest", null, false, false, null, null, 0);
        inFlightRequests.add(ifr);
    }

    @Test
    public void checkIncrementAndDecrementOnLastSent() {
        assertEquals(1, inFlightRequests.count());

        inFlightRequests.completeLastSent("dest");
        assertEquals(0, inFlightRequests.count());
    }

    @Test
    public void checkDecrementOnClear() {
        inFlightRequests.clearAll("dest");
        assertEquals(0, inFlightRequests.count());
    }

    @Test
    public void checkDecrementOnCompleteNext() {
        inFlightRequests.completeNext("dest");
        assertEquals(0, inFlightRequests.count());
    }

    @Test(expected = IllegalStateException.class)
    public void throwExceptionOnNeverBeforeSeenNode() {
        inFlightRequests.completeNext("not-added");
    }
}
