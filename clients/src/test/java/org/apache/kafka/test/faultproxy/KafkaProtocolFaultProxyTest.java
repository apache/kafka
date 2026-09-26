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

import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaProtocolFaultProxyTest {

    private static final String UPSTREAM_HOST = "upstream-broker";
    private static final int UPSTREAM_PORT = 19092;
    private static final String GROUP_ID = "group";

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

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.FIND_COORDINATOR)
    @Timeout(30)
    public void shouldRewriteFindCoordinatorResponsesToPointAtTheProxy(final short version) throws Exception {
        // Unless the coordinator address is rewritten, clients talk to the coordinator directly and every
        // group-coordinator request bypasses the proxy, so faults registered for those APIs never fire.
        try (ServerSocket upstream = new ServerSocket(0);
             KafkaProtocolFaultProxy proxy = KafkaProtocolFaultProxy.inFrontOf("localhost:" + upstream.getLocalPort())) {
            final CompletableFuture<Void> upstreamReplied =
                CompletableFuture.runAsync(() -> replyWithUpstreamCoordinator(upstream, version));

            final RequestHeader header = new RequestHeader(ApiKeys.FIND_COORDINATOR, version, "fault-proxy-test", 1);
            final ByteBuffer request = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                    .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                    .setCoordinatorKeys(List.of(GROUP_ID)))
                .build(version)
                .serializeWithHeader(header);

            final String[] proxyAddress = proxy.bootstrapServers().split(":");
            try (Socket client = new Socket(proxyAddress[0], Integer.parseInt(proxyAddress[1]))) {
                client.setSoTimeout(10_000);
                writeFrame(new DataOutputStream(client.getOutputStream()), request);
                final FindCoordinatorResponseData response = ((FindCoordinatorResponse) AbstractResponse.parseResponse(
                    ByteBuffer.wrap(readFrame(new DataInputStream(client.getInputStream()))), header)).data();

                if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION) {
                    assertEquals(proxy.bootstrapServers(), response.host() + ":" + response.port());
                } else {
                    assertEquals(1, response.coordinators().size());
                    final FindCoordinatorResponseData.Coordinator coordinator = response.coordinators().get(0);
                    assertEquals(proxy.bootstrapServers(), coordinator.host() + ":" + coordinator.port());
                }
            }
            upstreamReplied.get(10, TimeUnit.SECONDS);
        }
    }

    private static void replyWithUpstreamCoordinator(final ServerSocket upstream, final short version) {
        try (Socket broker = upstream.accept()) {
            final RequestHeader header =
                RequestHeader.parse(ByteBuffer.wrap(readFrame(new DataInputStream(broker.getInputStream()))));
            final FindCoordinatorResponseData response = new FindCoordinatorResponseData();
            if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION) {
                response.setNodeId(0).setHost(UPSTREAM_HOST).setPort(UPSTREAM_PORT);
            } else {
                response.setCoordinators(List.of(new FindCoordinatorResponseData.Coordinator()
                    .setKey(GROUP_ID).setNodeId(0).setHost(UPSTREAM_HOST).setPort(UPSTREAM_PORT)));
            }
            writeFrame(new DataOutputStream(broker.getOutputStream()), RequestUtils.serialize(
                new ResponseHeaderData().setCorrelationId(header.correlationId()),
                ApiKeys.FIND_COORDINATOR.responseHeaderVersion(version),
                response,
                version));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] readFrame(final DataInputStream in) throws IOException {
        final byte[] frame = new byte[in.readInt()];
        in.readFully(frame);
        return frame;
    }

    private static void writeFrame(final DataOutputStream out, final ByteBuffer frame) throws IOException {
        final byte[] bytes = new byte[frame.remaining()];
        frame.duplicate().get(bytes);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }
}
