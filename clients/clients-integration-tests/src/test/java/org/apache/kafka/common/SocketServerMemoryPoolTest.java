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
package org.apache.kafka.common;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.TestKitDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.IntegrationTestUtils;

import java.io.EOFException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the (bounded) memory pool releases the memory also after
 * invalid and unsupported protocol requests
 */
@ClusterTestDefaults(types = {Type.CO_KRAFT}, serverProperties = {
    @ClusterConfigProperty(key = SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG, value = "50000"),
    @ClusterConfigProperty(key = SocketServerConfigs.QUEUED_MAX_BYTES_CONFIG, value = "100000"),
})
public class SocketServerMemoryPoolTest {
    @ClusterTest
    public void testRequestWithUnsupportedVersion(ClusterInstance clusterInstance) throws Exception {
        RequestHeader header = IntegrationTestUtils.nextRequestHeader(ApiKeys.PRODUCE, Short.MAX_VALUE);
        ByteBuffer buffer = RequestUtils.serialize(header.data(), header.headerVersion(), new ProduceRequestData(), header.apiVersion());
        byte[] rawRequestBytes = buffer.array();

        sendAndAssert(clusterInstance, rawRequestBytes);
    }

    @ClusterTest
    public void testRequestWithCorruptBody(ClusterInstance clusterInstance) throws Exception {
        RequestHeader header = IntegrationTestUtils.nextRequestHeader(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion());
        ByteBuffer buffer = RequestUtils.serialize(header.data(), header.headerVersion(), new ProduceRequestData(), header.apiVersion());
        byte[] rawRequestBytes = buffer.array();

        // corrupt body but leave header valid
        assertTrue(rawRequestBytes.length > header.size(), "must have body bytes to corrupt");
        for (int i = header.size(); i < rawRequestBytes.length; i++) {
            rawRequestBytes[i] = (byte) 0xFF;
        }
        sendAndAssert(clusterInstance, rawRequestBytes);
    }

    private void sendAndAssert(ClusterInstance clusterInstance, byte[] rawRequestBytes) throws Exception {
        long initialMemoryPoolAvailable = getMemoryPoolAvailable(clusterInstance);

        try (Socket socket = IntegrationTestUtils.connect(clusterInstance.brokerBoundPorts().get(0))) {
            socket.setSoTimeout(/* readTimeoutMs */ 5_000);
            IntegrationTestUtils.sendRequest(socket, rawRequestBytes);
            assertTrue(drainUntilClosed(socket.getInputStream()), "expected connection closed");
        }

        long finalMemoryPoolAvailable = getMemoryPoolAvailable(clusterInstance);
        assertEquals(100000, initialMemoryPoolAvailable);
        assertEquals(initialMemoryPoolAvailable, finalMemoryPoolAvailable);
    }

    private long getMemoryPoolAvailable(ClusterInstance clusterInstance) {
        return clusterInstance.brokers().get(TestKitDefaults.BROKER_ID_OFFSET).socketServer().memoryPool().availableMemory();
    }

    /*
     * Reads and discards bytes until the stream ends or times out.
     *
     * @return true if the remote end closed the connection (EOF or connection-reset),
     *         false if the socket timeout expired before closure.
     */
    private static boolean drainUntilClosed(InputStream in) {
        try {
            while (true) {
                if (in.read() == -1) {
                    // Clean EOF – broker closed its side of the connection.
                    return true;
                }
                // Some broker versions send a partial error response before closing; keep draining.
            }
        } catch (EOFException e) {
            return true;
        } catch (SocketTimeoutException e) {
            // SO_TIMEOUT fired before EOF – broker did not close within the allotted time.
            return false;
        } catch (Exception e) {
            // Any other I/O error (e.g., "Connection reset by peer") means the broker
            // unilaterally terminated the connection, which is the expected outcome.
            return true;
        }
    }
}
