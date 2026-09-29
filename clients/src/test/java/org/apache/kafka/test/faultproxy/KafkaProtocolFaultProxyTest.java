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
package org.apache.kafka.test.faultproxy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaProtocolFaultProxyTest {

    @Test
    public void shouldRejectMultiBrokerBootstrap() {
        // The proxy rewrites all routing to itself and forwards to a single upstream broker, so a
        // multi-broker bootstrap must fail fast rather than silently proxy only the first broker.
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> KafkaProtocolFaultProxy.inFrontOf("localhost:9092,localhost:9093,localhost:9094"));
        assertTrue(e.getMessage().contains("single broker"), e.getMessage());
        assertTrue(e.getMessage().contains("3 servers"), e.getMessage());
    }

    @Test
    public void shouldAcceptSingleBrokerBootstrap() throws Exception {
        // A single host:port is accepted; bootstrapServers() exposes the proxy's own listening address.
        try (KafkaProtocolFaultProxy proxy = KafkaProtocolFaultProxy.inFrontOf("localhost:9092")) {
            final String bootstrap = proxy.bootstrapServers();
            assertTrue(bootstrap.startsWith("localhost:"), bootstrap);
            assertEquals(1, bootstrap.split(",").length, bootstrap);
        }
    }
}
