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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.config.ReplicationConfigs;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

class NodeToControllerRequestThreadTest {

    private static AbstractConfig createConfig() {
        return new AbstractConfig(ReplicationConfigs.CONFIG_DEF, Map.of());
    }

    private static ControllerInformation controllerInfo(Optional<Node> node) {
        return new ControllerInformation(node, new ListenerName(""), SecurityProtocol.PLAINTEXT, "");
    }

    private static ControllerInformation emptyControllerInfo() {
        return controllerInfo(Optional.empty());
    }

    /**
     * Creates a supplier that returns {@code first} on the first call,
     * and {@code second} on all subsequent calls, matching the behavior
     * of Mockito's {@code thenReturn(first, second)}.
     *
     * <p>This avoids mocking {@code Supplier<ControllerInformation>} which would
     * require {@code @SuppressWarnings("unchecked")} due to generic type erasure.
     */
    private static Supplier<ControllerInformation> sequentialProvider(
            ControllerInformation first, ControllerInformation second) {
        AtomicReference<ControllerInformation> ref = new AtomicReference<>(first);
        return () -> ref.getAndSet(second);
    }

    @Test
    void testRetryTimeoutWhileControllerNotAvailable() {
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();
        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);

        Supplier<ControllerInformation> controllerNodeProvider = NodeToControllerRequestThreadTest::emptyControllerInfo;

        long retryTimeoutMs = 30000;
        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", retryTimeoutMs);
        testRequestThread.setStarted(true);

        TestControllerRequestCompletionHandler completionHandler =
            new TestControllerRequestCompletionHandler(null);
        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            new MetadataRequest.Builder(new MetadataRequestData()),
            completionHandler
        );

        testRequestThread.enqueue(queueItem);
        testRequestThread.doWork();
        assertEquals(1, testRequestThread.queueSize());

        time.sleep(retryTimeoutMs);
        testRequestThread.doWork();
        assertEquals(0, testRequestThread.queueSize());
        assertTrue(completionHandler.timedOut.get());
    }

    @Test
    void testRequestsSent() {
        // just a simple test that tests whether the request from 1 -> 2 is sent and the response callback is called
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();
        int controllerId = 2;

        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);

        Node activeController = new Node(controllerId, "host", 1234);
        Supplier<ControllerInformation> controllerNodeProvider =
            () -> controllerInfo(Optional.of(activeController));

        MetadataResponse expectedResponse = RequestTestUtils.metadataUpdateWith(2, Map.of("a", 2));
        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", Long.MAX_VALUE);
        testRequestThread.setStarted(true);
        mockClient.prepareResponse(expectedResponse);

        TestControllerRequestCompletionHandler completionHandler =
            new TestControllerRequestCompletionHandler(expectedResponse);
        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            new MetadataRequest.Builder(new MetadataRequestData()),
            completionHandler
        );

        testRequestThread.enqueue(queueItem);
        assertEquals(1, testRequestThread.queueSize());

        // initialize to the controller
        testRequestThread.doWork();
        // send and process the request
        testRequestThread.doWork();

        assertEquals(0, testRequestThread.queueSize());
        assertTrue(completionHandler.completed.get());
    }

    @Test
    void testControllerChanged() {
        // in this test the controller changes from node 1 -> node 2
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();
        int oldControllerId = 1;
        int newControllerId = 2;

        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);

        Node oldController = new Node(oldControllerId, "host1", 1234);
        Node newController = new Node(newControllerId, "host2", 1234);
        Supplier<ControllerInformation> controllerNodeProvider = sequentialProvider(
            controllerInfo(Optional.of(oldController)),
            controllerInfo(Optional.of(newController)));

        MetadataResponse expectedResponse = RequestTestUtils.metadataUpdateWith(3, Map.of("a", 2));
        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", Long.MAX_VALUE);
        testRequestThread.setStarted(true);

        TestControllerRequestCompletionHandler completionHandler =
            new TestControllerRequestCompletionHandler(expectedResponse);
        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            new MetadataRequest.Builder(new MetadataRequestData()),
            completionHandler
        );

        testRequestThread.enqueue(queueItem);
        mockClient.prepareResponse(expectedResponse);
        // initialize the thread with oldController
        testRequestThread.doWork();
        assertFalse(completionHandler.completed.get());

        // disconnect the node
        mockClient.setUnreachable(oldController, time.milliseconds() + 5000);
        // verify that the client closed the connection to the faulty controller
        testRequestThread.doWork();
        // should connect to the new controller
        testRequestThread.doWork();
        // should send the request and process the response
        testRequestThread.doWork();

        assertTrue(completionHandler.completed.get());
    }

    @Test
    void testNotController() {
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();
        int oldControllerId = 1;
        int newControllerId = 2;

        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);

        int port = 1234;
        Node oldController = new Node(oldControllerId, "host1", port);
        Node newController = new Node(newControllerId, "host2", port);
        Supplier<ControllerInformation> controllerNodeProvider = sequentialProvider(
            controllerInfo(Optional.of(oldController)),
            controllerInfo(Optional.of(newController)));

        MetadataResponse responseWithNotControllerError = RequestTestUtils.metadataUpdateWith("cluster1", 2,
            Map.of("a", Errors.NOT_CONTROLLER),
            Map.of("a", 2));
        MetadataResponse expectedResponse = RequestTestUtils.metadataUpdateWith(3, Map.of("a", 2));
        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", Long.MAX_VALUE);
        testRequestThread.setStarted(true);

        TestControllerRequestCompletionHandler completionHandler =
            new TestControllerRequestCompletionHandler(expectedResponse);
        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            new MetadataRequest.Builder(new MetadataRequestData()
                .setAllowAutoTopicCreation(true)),
            completionHandler
        );
        testRequestThread.enqueue(queueItem);
        // initialize to the controller
        testRequestThread.doWork();

        Node oldBrokerNode = new Node(oldControllerId, "host1", port);
        assertEquals(Optional.of(oldBrokerNode), testRequestThread.activeControllerAddress());

        // send and process the request
        mockClient.prepareResponse(
            body -> body instanceof MetadataRequest && ((MetadataRequest) body).allowAutoTopicCreation(),
            responseWithNotControllerError);
        testRequestThread.doWork();
        assertEquals(Optional.empty(), testRequestThread.activeControllerAddress());
        // reinitialize the controller to a different node
        testRequestThread.doWork();
        // process the request again
        mockClient.prepareResponse(expectedResponse);
        testRequestThread.doWork();

        Node newControllerNode = new Node(newControllerId, "host2", port);
        assertEquals(Optional.of(newControllerNode), testRequestThread.activeControllerAddress());

        assertTrue(completionHandler.completed.get());
    }

    @Test
    void testEnvelopeResponseWithNotControllerError() {
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();
        int oldControllerId = 1;
        int newControllerId = 2;

        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);
        // enable envelope API
        mockClient.setNodeApiVersions(NodeApiVersions.create(ApiKeys.ENVELOPE.id, (short) 0, (short) 0));

        int port = 1234;
        Node oldController = new Node(oldControllerId, "host1", port);
        Node newController = new Node(newControllerId, "host2", port);
        Supplier<ControllerInformation> controllerNodeProvider = sequentialProvider(
            controllerInfo(Optional.of(oldController)),
            controllerInfo(Optional.of(newController)));

        // create an envelopeResponse with NOT_CONTROLLER error
        EnvelopeResponse envelopeResponseWithNotControllerError = new EnvelopeResponse(
            new EnvelopeResponseData().setErrorCode(Errors.NOT_CONTROLLER.code()));

        // response for retry request after receiving NOT_CONTROLLER error
        MetadataResponse expectedResponse = RequestTestUtils.metadataUpdateWith(3, Map.of("a", 2));

        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", Long.MAX_VALUE);
        testRequestThread.setStarted(true);

        TestControllerRequestCompletionHandler completionHandler =
            new TestControllerRequestCompletionHandler(expectedResponse);
        KafkaPrincipal kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "principal", true);
        DefaultKafkaPrincipalBuilder kafkaPrincipalBuilder = new DefaultKafkaPrincipalBuilder(null, null);

        // build an EnvelopeRequest by dummy data
        EnvelopeRequest.Builder envelopeRequestBuilder = new EnvelopeRequest.Builder(ByteBuffer.allocate(0),
            kafkaPrincipalBuilder.serialize(kafkaPrincipal), "client-address".getBytes());

        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            envelopeRequestBuilder,
            completionHandler
        );

        testRequestThread.enqueue(queueItem);
        // initialize to the controller
        testRequestThread.doWork();

        Node oldBrokerNode = new Node(oldControllerId, "host1", port);
        assertEquals(Optional.of(oldBrokerNode), testRequestThread.activeControllerAddress());

        // send and process the envelope request
        mockClient.prepareResponse(
            body -> body instanceof EnvelopeRequest,
            envelopeResponseWithNotControllerError);
        testRequestThread.doWork();
        // expect to reset the activeControllerAddress after finding the NOT_CONTROLLER error
        assertEquals(Optional.empty(), testRequestThread.activeControllerAddress());
        // reinitialize the controller to a different node
        testRequestThread.doWork();
        // process the request again
        mockClient.prepareResponse(expectedResponse);
        testRequestThread.doWork();

        Node newControllerNode = new Node(newControllerId, "host2", port);
        assertEquals(Optional.of(newControllerNode), testRequestThread.activeControllerAddress());

        assertTrue(completionHandler.completed.get());
    }

    @Test
    void testRetryTimeout() {
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();
        int controllerId = 1;

        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);

        Node controller = new Node(controllerId, "host1", 1234);
        Supplier<ControllerInformation> controllerNodeProvider =
            () -> controllerInfo(Optional.of(controller));

        long retryTimeoutMs = 30000;
        MetadataResponse responseWithNotControllerError = RequestTestUtils.metadataUpdateWith("cluster1", 2,
            Map.of("a", Errors.NOT_CONTROLLER),
            Map.of("a", 2));
        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", retryTimeoutMs);
        testRequestThread.setStarted(true);

        TestControllerRequestCompletionHandler completionHandler =
            new TestControllerRequestCompletionHandler();
        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            new MetadataRequest.Builder(new MetadataRequestData()
                .setAllowAutoTopicCreation(true)),
            completionHandler
        );

        testRequestThread.enqueue(queueItem);

        // initialize to the controller
        testRequestThread.doWork();

        time.sleep(retryTimeoutMs);

        // send and process the request
        mockClient.prepareResponse(
            body -> body instanceof MetadataRequest && ((MetadataRequest) body).allowAutoTopicCreation(),
            responseWithNotControllerError);

        testRequestThread.doWork();

        assertTrue(completionHandler.timedOut.get());
    }

    @Test
    void testUnsupportedVersionHandling() {
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();
        int controllerId = 2;

        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);

        Node activeController = new Node(controllerId, "host", 1234);
        Supplier<ControllerInformation> controllerNodeProvider =
            () -> controllerInfo(Optional.of(activeController));

        AtomicReference<ClientResponse> callbackResponse = new AtomicReference<>();
        ControllerRequestCompletionHandler completionHandler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                fail("Unexpected timeout exception");
            }

            @Override
            public void onComplete(ClientResponse response) {
                callbackResponse.set(response);
            }
        };

        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            new MetadataRequest.Builder(new MetadataRequestData()),
            completionHandler
        );

        mockClient.prepareUnsupportedVersionResponse(request -> request.apiKey() == ApiKeys.METADATA);

        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", Long.MAX_VALUE);
        testRequestThread.setStarted(true);

        testRequestThread.enqueue(queueItem);
        pollUntil(testRequestThread, () -> callbackResponse.get() != null);
        assertNotNull(callbackResponse.get().versionMismatch());
    }

    @Test
    void testAuthenticationExceptionHandling() {
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();
        int controllerId = 2;

        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);

        Node activeController = new Node(controllerId, "host", 1234);
        Supplier<ControllerInformation> controllerNodeProvider =
            () -> controllerInfo(Optional.of(activeController));

        AtomicReference<ClientResponse> callbackResponse = new AtomicReference<>();
        ControllerRequestCompletionHandler completionHandler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                fail("Unexpected timeout exception");
            }

            @Override
            public void onComplete(ClientResponse response) {
                callbackResponse.set(response);
            }
        };

        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            new MetadataRequest.Builder(new MetadataRequestData()),
            completionHandler
        );

        mockClient.createPendingAuthenticationError(activeController, 50);

        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", Long.MAX_VALUE);
        testRequestThread.setStarted(true);

        testRequestThread.enqueue(queueItem);
        pollUntil(testRequestThread, () -> callbackResponse.get() != null);
        assertNotNull(callbackResponse.get().authenticationException());
        assertEquals(Optional.empty(), testRequestThread.activeControllerAddress());
    }

    @Test
    void testThreadNotStarted() {
        // Make sure we throw if we enqueue anything while the thread is not running
        MockTime time = new MockTime();
        AbstractConfig config = createConfig();

        Metadata metadata = mock(Metadata.class);
        MockClient mockClient = new MockClient(time, metadata);

        Supplier<ControllerInformation> controllerNodeProvider = NodeToControllerRequestThreadTest::emptyControllerInfo;

        NodeToControllerRequestThread testRequestThread = new NodeToControllerRequestThread(
            mockClient, new ManualMetadataUpdater(),
            controllerNodeProvider, config, time, "", Long.MAX_VALUE);

        TestControllerRequestCompletionHandler completionHandler =
            new TestControllerRequestCompletionHandler(null);
        NodeToControllerQueueItem queueItem = new NodeToControllerQueueItem(
            time.milliseconds(),
            new MetadataRequest.Builder(new MetadataRequestData()),
            completionHandler
        );

        assertThrows(IllegalStateException.class, () -> testRequestThread.enqueue(queueItem));
        assertEquals(0, testRequestThread.queueSize());
    }

    private void pollUntil(NodeToControllerRequestThread requestThread, java.util.function.BooleanSupplier condition) {
        pollUntil(requestThread, condition, 10);
    }

    private void pollUntil(NodeToControllerRequestThread requestThread, java.util.function.BooleanSupplier condition, int maxRetries) {
        int tries = 0;
        do {
            requestThread.doWork();
            tries++;
        } while (!condition.getAsBoolean() && tries < maxRetries);

        if (!condition.getAsBoolean()) {
            fail("Condition failed to be met after polling " + tries + " times");
        }
    }

    private static class TestControllerRequestCompletionHandler implements ControllerRequestCompletionHandler {
        private final AbstractResponse expectedResponse;
        final AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicBoolean timedOut = new AtomicBoolean(false);

        TestControllerRequestCompletionHandler() {
            this(null);
        }

        TestControllerRequestCompletionHandler(AbstractResponse expectedResponse) {
            this.expectedResponse = expectedResponse;
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (expectedResponse != null) {
                assertEquals(expectedResponse, response.responseBody());
            }
            completed.set(true);
        }

        @Override
        public void onTimeout() {
            timedOut.set(true);
        }
    }
}
