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

package org.apache.kafka.common.network;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerEndPointTest {

    @Test
    public void testParseHostPort() {
        Optional<BrokerEndPoint> brokerEndPoint = BrokerEndPoint.parseHostPort("myhost:9092");
        assertTrue(brokerEndPoint.isPresent());
        assertEquals("myhost", brokerEndPoint.get().host());
        assertEquals(9092, brokerEndPoint.get().port());

        brokerEndPoint = BrokerEndPoint.parseHostPort("127.0.0.1:9092");
        assertTrue(brokerEndPoint.isPresent());
        assertEquals("127.0.0.1", brokerEndPoint.get().host());
        assertEquals(9092, brokerEndPoint.get().port());

        // IPv6 endpoint
        brokerEndPoint = BrokerEndPoint.parseHostPort("[2001:db8::1]:9092");
        assertTrue(brokerEndPoint.isPresent());
        assertEquals("2001:db8::1", brokerEndPoint.get().host());
        assertEquals(9092, brokerEndPoint.get().port());
    }

    @Test
    public void testParseHostPortInvalid() {
        // Invalid separator
        Optional<BrokerEndPoint> brokerEndPoint = BrokerEndPoint.parseHostPort("myhost-9092");
        assertFalse(brokerEndPoint.isPresent());

        // No separator
        brokerEndPoint = BrokerEndPoint.parseHostPort("myhost9092");
        assertFalse(brokerEndPoint.isPresent());

        // Invalid port
        brokerEndPoint = BrokerEndPoint.parseHostPort("myhost:abcd");
        assertFalse(brokerEndPoint.isPresent());
    }
}
