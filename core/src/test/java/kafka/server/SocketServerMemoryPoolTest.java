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
package kafka.server;

import kafka.network.SocketServer;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;

import org.junit.jupiter.api.extension.ExtendWith;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the (bounded) memory pool releases the memory also after
 * invalid and unsupported protocol requests
 */
@ClusterTestDefaults(types = {Type.ZK}, serverProperties = {
        @ClusterConfigProperty(key = SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG, value = "50000"),
        @ClusterConfigProperty(key = SocketServerConfigs.QUEUED_MAX_BYTES_CONFIG, value = "100000"),
})
@ExtendWith(ClusterTestExtensions.class)
public class SocketServerMemoryPoolTest {
    @ClusterTest
    public void testProduceRequestWithUnsupportedVersion(ClusterInstance clusterInstance) throws Exception {
        short unsupportedVersion = Short.MAX_VALUE;
        byte[] rawRequestBytes = buildRawRequest(
                ApiKeys.PRODUCE.id,
                unsupportedVersion,
                /* correlationId */ 1,
                /* clientId     */ "test-unsupported-version",
                new byte[10000]
        );

        sendAndAssert(clusterInstance, rawRequestBytes);
    }

    @ClusterTest
    public void testProduceRequestWithCorruptBody(ClusterInstance clusterInstance) throws Exception {
        short validVersion = 3;
        byte[] corruptBody = new byte[10000];
        for (int i = 0; i < corruptBody.length; i++) {
            corruptBody[i] = (byte) 0xFF; // The corrupt body (0xFF ... 0xFF) makes Schema.read() throw SchemaException.
        }

        byte[] rawRequestBytes = buildRawRequest(
                ApiKeys.PRODUCE.id,
                validVersion,
                /* correlationId */ 2,
                /* clientId     */ "test-corrupt-body",
                corruptBody
        );

        sendAndAssert(clusterInstance, rawRequestBytes);
    }

    private void sendAndAssert(ClusterInstance clusterInstance, byte[] rawRequestBytes) throws IOException {
        long initialMemoryPoolAvailable = getMemoryPoolAvailable();
        ListenerName listenerName = ListenerName.normalised("PLAINTEXT");
        SocketServer ss = clusterInstance.anyBrokerSocketServer();

        try (Socket socket = IntegrationTestUtils.connect(ss, listenerName)) {
            socket.setSoTimeout(/* readTimeoutMs */ 5_000);
            IntegrationTestUtils.sendRequest(socket, rawRequestBytes);
            assertTrue(drainUntilClosed(socket.getInputStream()), "expected connection closed");
        }

        long finalMemoryPoolAvailable = getMemoryPoolAvailable();
        assertEquals(100000, initialMemoryPoolAvailable);
        assertEquals(initialMemoryPoolAvailable, finalMemoryPoolAvailable);
    }

    long getMemoryPoolAvailable() {
        // there is only one broker in this cluster, so the singleton KafkaYammerMetrics.defaultRegistry()
        // contains only the metric for that single broker memory pool
        Map<com.yammer.metrics.core.MetricName, Metric> metricNameMetricMap = KafkaYammerMetrics.defaultRegistry().allMetrics();
        for (com.yammer.metrics.core.MetricName name : metricNameMetricMap.keySet()) {
            if (name.toString().contains("MemoryPoolAvailable")) {
                return ((Gauge<Long>) metricNameMetricMap.get(name)).value();
            }
        }
        return -1;
    }

    /**
     * Builds a raw Kafka request excluding the frame length
     *
     * <p>Wire layout:
     * <pre>
     *   4 bytes – frame length (payload size, not including these 4 bytes)
     *
     *   2 bytes – api_key
     *   2 bytes – api_version
     *   4 bytes – correlation_id
     *   2 bytes – client_id string length
     *   N bytes – client_id (UTF-8)
     *   X bytes - request body
     * </pre>
     */
    private static byte[] buildRawRequest(short apiKey, short apiVersion, int correlationId, String clientId, byte[] body) {
        byte[] clientIdBytes = clientId.getBytes(StandardCharsets.UTF_8);

        // Header: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id
        int headerSize = 2 + 2 + 4 + 2 + clientIdBytes.length;
        int payloadSize = headerSize + body.length;

        ByteBuffer buf = ByteBuffer.allocate(payloadSize);
        buf.putShort(apiKey);                          // api_key
        buf.putShort(apiVersion);                      // api_version
        buf.putInt(correlationId);                     // correlation_id
        buf.putShort((short) clientIdBytes.length);    // client_id string length
        buf.put(clientIdBytes);                        // client_id bytes
        buf.put(body);                                 // request body (possibly empty / corrupt)
        return buf.array();
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
