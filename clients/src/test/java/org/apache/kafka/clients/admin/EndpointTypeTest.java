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

package org.apache.kafka.clients.admin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(60)
public class EndpointTypeTest {
    @Test
    public void testRoundTripBroker() {
        testRoundTrip(EndpointType.BROKER);
    }

    @Test
    public void testRoundTripController() {
        testRoundTrip(EndpointType.CONTROLLER);
    }

    @Test
    public void testUnknown() {
        assertEquals(EndpointType.UNKNOWN, EndpointType.fromId((byte) 0));
        assertEquals(EndpointType.UNKNOWN, EndpointType.fromId((byte) 3));
    }

    private void testRoundTrip(EndpointType type) {
        byte id = type.id();
        assertEquals(type, EndpointType.fromId(id));
    }
}
