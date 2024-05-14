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
package org.apache.kafka.server.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;

public class InterBrokerSendThreadTest {

    private final Time time = new MockTime();
    private final KafkaClient networkClient = mock(KafkaClient.class);
    private final StubCompletionHandler completionHandler = new StubCompletionHandler();
    private final int requestTimeoutMs = 1000;

    class TestInterBrokerSendThread extends InterBrokerSendThread {

        private final Consumer<Throwable> exceptionCallback;
        private final Queue<RequestAndCompletionHandler> queue = new ArrayDeque<>();

        TestInterBrokerSendThread() {
            this(
                InterBrokerSendThreadTest.this.networkClient,
                t -> {
                    throw (t instanceof RuntimeException)
                        ? ((RuntimeException) t)
                        : new RuntimeException(t);
                });
        }

        TestInterBrokerSendThread(KafkaClient networkClient, Consumer<Throwable> exceptionCallback) {
            super("name", networkClient, requestTimeoutMs, time);
            this.exceptionCallback = exceptionCallback;
        }

        void enqueue(RequestAndCompletionHandler request) {
            queue.offer(request);
        }

        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            return queue.isEmpty() ? Collections.emptyList() : Collections.singletonList(queue.poll());
        }

        @Override
        protected void pollOnce(long maxTimeoutMs) {
            try {
                super.pollOnce(maxTimeoutMs);
            } catch (Throwable t) {
                exceptionCallback.accept(t);
            }
        }
    }

    @Test
    public void testShutdownThreadShouldNotCauseException() throws InterruptedException, IOException {
        // InterBrokerSendThread#shutdown calls NetworkClient#initiateClose first so NetworkClient#poll
        // can throw DisconnectException when thread is running
        when(networkClient.poll(anyLong(), anyLong())).thenThrow(new DisconnectException());
        when(networkClient.active()).thenReturn(false);

        AtomicReference<Throwable> exception = new AtomicReference<>();
        final InterBrokerSendThread thread =
            new TestInterBrokerSendThread(networkClient, exception::getAndSet);
        thread.shutdown();
        thread.pollOnce(100);

        verify(networkClient).poll(anyLong(), anyLong());
        verify(networkClient).initiateClose();
        verify(networkClient).close();
        verify(networkClient).active();
        verifyNoMoreInteractions(networkClient);

        assertNull(exception.get());
    }

    @Test
    public void testDisconnectWithoutShutdownShouldCauseException() {
        DisconnectException de = new DisconnectException();
        when(networkClient.poll(anyLong(), anyLong())).thenThrow(de);
        when(networkClient.active()).thenReturn(true);

        AtomicReference<Throwable> throwable = new AtomicReference<>();
        final InterBrokerSendThread thread =
            new TestInterBrokerSendThread(networkClient, throwable::getAndSet);
        thread.pollOnce(100);

        verify(networkClient).poll(anyLong(), anyLong());
        verify(networkClient).active();
        verifyNoMoreInteractions(networkClient);

        Throwable thrown = throwable.get();
        assertNotNull(thrown);
        assertInstanceOf(FatalExitError.class, thrown);
    }

    @Test
    public void testShouldNotSendAnythingWhenNoRequests() {
        final InterBrokerSendThread sendThread = new TestInterBrokerSendThread();

        // poll is always called but there should be no further invocations on NetworkClient
        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        sendThread.doWork();

        verify(networkClient).poll(anyLong(), anyLong());
        verifyNoMoreInteractions(networkClient);

        assertFalse(completionHandler.executedWithDisconnectedResponse);
    }

    @Test
    public void testShouldCreateClientRequestAndSendWhenNodeIsReady() {
        final AbstractRequest.Builder<?> request = new StubRequestBuilder<>();
        final Node node = new Node(1, "", 8080);
        final RequestAndCompletionHandler handler =
            new RequestAndCompletionHandler(time.milliseconds(), node, request, completionHandler);
        final TestInterBrokerSendThread sendThread = new TestInterBrokerSendThread();

        final ClientRequest clientRequest =
            new ClientRequest("dest", request, 0, "1", 0, true, requestTimeoutMs, handler.handler);

        when(networkClient.newClientRequest(
            ArgumentMatchers.eq("1"),
            same(handler.request),
            anyLong(),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(requestTimeoutMs),
            same(handler.handler)
        )).thenReturn(clientRequest);

        when(networkClient.ready(node, time.milliseconds())).thenReturn(true);

        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        sendThread.enqueue(handler);
        sendThread.doWork();

        verify(networkClient)
            .newClientRequest(
                ArgumentMatchers.eq("1"),
                same(handler.request),
                anyLong(),
                ArgumentMatchers.eq(true),
                ArgumentMatchers.eq(requestTimeoutMs),
                same(handler.handler));
        verify(networkClient).ready(any(), anyLong());
        verify(networkClient).send(same(clientRequest), anyLong());
        verify(networkClient).poll(anyLong(), anyLong());
        verifyNoMoreInteractions(networkClient);

        assertFalse(completionHandler.executedWithDisconnectedResponse);
    }

    @Test
    public void testShouldCallCompletionHandlerWithDisconnectedResponseWhenNodeNotReady() {
        final AbstractRequest.Builder<?> request = new StubRequestBuilder<>();
        final Node node = new Node(1, "", 8080);
        final RequestAndCompletionHandler handler =
            new RequestAndCompletionHandler(time.milliseconds(), node, request, completionHandler);
        final TestInterBrokerSendThread sendThread = new TestInterBrokerSendThread();

        final ClientRequest clientRequest =
            new ClientRequest("dest", request, 0, "1", 0, true, requestTimeoutMs, handler.handler);

        when(networkClient.newClientRequest(
            ArgumentMatchers.eq("1"),
            same(handler.request),
            anyLong(),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(requestTimeoutMs),
            same(handler.handler)
        )).thenReturn(clientRequest);

        when(networkClient.ready(node, time.milliseconds())).thenReturn(false);

        when(networkClient.connectionDelay(any(), anyLong())).thenReturn(0L);

        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        when(networkClient.connectionFailed(node)).thenReturn(true);

        when(networkClient.authenticationException(node)).thenReturn(new AuthenticationException(""));

        sendThread.enqueue(handler);
        sendThread.doWork();

        verify(networkClient)
            .newClientRequest(
                ArgumentMatchers.eq("1"),
                same(handler.request),
                anyLong(),
                ArgumentMatchers.eq(true),
                ArgumentMatchers.eq(requestTimeoutMs),
                same(handler.handler));
        verify(networkClient).ready(any(), anyLong());
        verify(networkClient).connectionDelay(any(), anyLong());
        verify(networkClient).poll(anyLong(), anyLong());
        verify(networkClient).connectionFailed(any());
        verify(networkClient).authenticationException(any());
        verifyNoMoreInteractions(networkClient);

        assertTrue(completionHandler.executedWithDisconnectedResponse);
    }

    @Test
    public void testFailingExpiredRequests() {
        final AbstractRequest.Builder<?> request = new StubRequestBuilder<>();
        final Node node = new Node(1, "", 8080);
        final RequestAndCompletionHandler handler =
            new RequestAndCompletionHandler(time.milliseconds(), node, request, completionHandler);
        final TestInterBrokerSendThread sendThread = new TestInterBrokerSendThread();

        final ClientRequest clientRequest =
            new ClientRequest(
                "dest", request, 0, "1", time.milliseconds(), true, requestTimeoutMs, handler.handler);
        time.sleep(1500L);

        when(networkClient.newClientRequest(
            ArgumentMatchers.eq("1"),
            same(handler.request),
            ArgumentMatchers.eq(handler.creationTimeMs),
            ArgumentMatchers.eq(true),
            ArgumentMatchers.eq(requestTimeoutMs),
            same(handler.handler)
        )).thenReturn(clientRequest);

        // make the node unready so the request is not cleared
        when(networkClient.ready(node, time.milliseconds())).thenReturn(false);

        when(networkClient.connectionDelay(any(), anyLong())).thenReturn(0L);

        when(networkClient.poll(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        // rule out disconnects so the request stays for the expiry check
        when(networkClient.connectionFailed(node)).thenReturn(false);

        sendThread.enqueue(handler);
        sendThread.doWork();

        verify(networkClient)
            .newClientRequest(
                ArgumentMatchers.eq("1"),
                same(handler.request),
                ArgumentMatchers.eq(handler.creationTimeMs),
                ArgumentMatchers.eq(true),
                ArgumentMatchers.eq(requestTimeoutMs),
                same(handler.handler));
        verify(networkClient).ready(any(), anyLong());
        verify(networkClient).connectionDelay(any(), anyLong());
        verify(networkClient).poll(anyLong(), anyLong());
        verify(networkClient).connectionFailed(any());
        verifyNoMoreInteractions(networkClient);

        assertFalse(sendThread.hasUnsentRequests());
        assertTrue(completionHandler.executedWithDisconnectedResponse);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInterruption(boolean isShuttingDown) throws InterruptedException, IOException {
        Exception interrupted = new InterruptedException();

        // InterBrokerSendThread#shutdown calls NetworkClient#initiateClose first so NetworkClient#poll
        // can throw InterruptedException if a callback request that throws it is handled
        when(networkClient.poll(anyLong(), anyLong())).thenAnswer(t -> {
            throw interrupted;
        });

        AtomicReference<Throwable> exception = new AtomicReference<>();
        final InterBrokerSendThread thread =
            new TestInterBrokerSendThread(networkClient, t -> {
                if (isShuttingDown)
                    assertInstanceOf(InterruptedException.class, t);
                else
                    assertInstanceOf(FatalExitError.class, t);
                exception.getAndSet(t);
            });

        if (isShuttingDown)
            thread.shutdown();
        thread.pollOnce(100);

        verify(networkClient).poll(anyLong(), anyLong());
        if (isShuttingDown) {
            verify(networkClient).initiateClose();
            verify(networkClient).close();
        }
        verifyNoMoreInteractions(networkClient);
        assertNotNull(exception.get());
    }

    private static class StubRequestBuilder<T extends AbstractRequest>
        extends AbstractRequest.Builder<T> {

        private StubRequestBuilder() {
            super(ApiKeys.END_TXN);
        }

        @Override
        public T build(short version) {
            return null;
        }
    }

    private static class StubCompletionHandler implements RequestCompletionHandler {

        public boolean executedWithDisconnectedResponse = false;
        ClientResponse response = null;

        @Override
        public void onComplete(ClientResponse response) {
            this.executedWithDisconnectedResponse = response.wasDisconnected();
            this.response = response;
        }
    }
}
