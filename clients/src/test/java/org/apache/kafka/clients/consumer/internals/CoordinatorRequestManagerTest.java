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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.apache.log4j.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class CoordinatorRequestManagerTest {
    private static final int RETRY_BACKOFF_MS = 500;
    private static final String GROUP_ID = "group-1";
    private MockTime time;
    private BackgroundEventHandler backgroundEventHandler;
    private Node node;

    @BeforeEach
    public void setup() {
        this.time = new MockTime(0);
        this.node = new Node(1, "localhost", 9092);
        this.backgroundEventHandler = mock(BackgroundEventHandler.class);
    }

    @Test
    public void testSuccessfulResponse() {
        CoordinatorRequestManager coordinatorManager = setupCoordinatorManager(GROUP_ID);
        expectFindCoordinatorRequest(coordinatorManager, Errors.NONE);

        Optional<Node> coordinatorOpt = coordinatorManager.coordinator();
        assertTrue(coordinatorOpt.isPresent());
        assertEquals(Integer.MAX_VALUE - node.id(), coordinatorOpt.get().id());
        assertEquals(node.host(), coordinatorOpt.get().host());
        assertEquals(node.port(), coordinatorOpt.get().port());

        NetworkClientDelegate.PollResult pollResult = coordinatorManager.poll(time.milliseconds());
        assertEquals(Collections.emptyList(), pollResult.unsentRequests);
    }

    /**
     * This test mimics a client that has been disconnected from the coordinator. When the client remains disconnected
     * from the coordinator for 60 seconds, the client will begin to emit a warning log every minute thereafter to
     * alert the user about the ongoing disconnect status. The warning log includes the length of time of the ongoing
     * disconnect:
     *
     * <code>
     *     Consumer has been disconnected from the group coordinator for XXXXXms
     * </code>
     *
     * <p/>
     *
     * However, the logic used to calculate the length of the disconnect was not correct. This test exercises the
     * disconnect logic, controlling the logging and system time, to ensure the warning message is correct.
     *
     * @see CoordinatorRequestManager#markCoordinatorUnknown(String, long)
     */
    @Test
    public void testMarkCoordinatorUnknownLoggingAccuracy() {
        long oneMinute = 60000;

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            // You'd be forgiven for assuming that a warning message would be logged at WARN, but
            // markCoordinatorUnknown logs the warning at DEBUG. This is partly for historical parity with the
            // ClassicKafkaConsumer.
            appender.setClassLogger(CoordinatorRequestManager.class, Level.DEBUG);
            CoordinatorRequestManager coordinatorRequestManager = setupCoordinatorManager(GROUP_ID);
            assertFalse(coordinatorRequestManager.coordinator().isPresent());

            // Step 1: mark the coordinator as disconnected right after creation of the CoordinatorRequestManager.
            // Because the disconnect occurred immediately, no warning should be logged.
            coordinatorRequestManager.markCoordinatorUnknown("test", time.milliseconds());
            assertTrue(millisecondsFromLog(appender).isEmpty());

            // Step 2: sleep for one minute and mark the coordinator unknown again. Then verify that the warning was
            // logged and the reported time is accurate.
            time.sleep(oneMinute);
            coordinatorRequestManager.markCoordinatorUnknown("test", time.milliseconds());
            Optional<Long> firstLogMs = millisecondsFromLog(appender);
            assertTrue(firstLogMs.isPresent());
            assertEquals(oneMinute, firstLogMs.get());

            // Step 3: sleep for *another* minute, mark the coordinator unknown again, and verify the accuracy.
            time.sleep(oneMinute);
            coordinatorRequestManager.markCoordinatorUnknown("test", time.milliseconds());
            Optional<Long> secondLogMs = millisecondsFromLog(appender);
            assertTrue(secondLogMs.isPresent());
            assertEquals(oneMinute * 2, secondLogMs.get());
        }
    }

    private Optional<Long> millisecondsFromLog(LogCaptureAppender appender) {
        Pattern pattern = Pattern.compile("\\s+(?<millis>\\d+)+ms");
        List<Long> milliseconds = appender.getMessages().stream()
            .map(pattern::matcher)
            .filter(Matcher::find)
            .map(matcher -> matcher.group("millis"))
            .filter(Objects::nonNull)
            .map(millisString -> {
                try {
                    return Long.parseLong(millisString);
                } catch (NumberFormatException e) {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        // Return the most recent log entry that matches the message in markCoordinatorUnknown, if present.
        return milliseconds.isEmpty() ? Optional.empty() : Optional.of(milliseconds.get(milliseconds.size() - 1));
    }

    @Test
    public void testMarkCoordinatorUnknown() {
        CoordinatorRequestManager coordinatorManager = setupCoordinatorManager(GROUP_ID);

        expectFindCoordinatorRequest(coordinatorManager, Errors.NONE);
        assertTrue(coordinatorManager.coordinator().isPresent());

        // It may take time for metadata to converge between after a coordinator has
        // been demoted. This can cause a tight loop in which FindCoordinator continues to
        // return node X while that node continues to reply with NOT_COORDINATOR. Hence we
        // still want to ensure a backoff after successfully finding the coordinator.
        coordinatorManager.markCoordinatorUnknown("coordinator changed", time.milliseconds());
        assertEquals(Collections.emptyList(), coordinatorManager.poll(time.milliseconds()).unsentRequests);

        time.sleep(RETRY_BACKOFF_MS - 1);
        assertEquals(Collections.emptyList(), coordinatorManager.poll(time.milliseconds()).unsentRequests);

        time.sleep(RETRY_BACKOFF_MS);
        expectFindCoordinatorRequest(coordinatorManager, Errors.NONE);
        assertTrue(coordinatorManager.coordinator().isPresent());
    }

    @Test
    public void testBackoffAfterRetriableFailure() {
        CoordinatorRequestManager coordinatorManager = setupCoordinatorManager(GROUP_ID);
        expectFindCoordinatorRequest(coordinatorManager, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        verifyNoInteractions(backgroundEventHandler);

        time.sleep(RETRY_BACKOFF_MS - 1);
        assertEquals(Collections.emptyList(), coordinatorManager.poll(time.milliseconds()).unsentRequests);

        time.sleep(1);
        expectFindCoordinatorRequest(coordinatorManager, Errors.NONE);
    }

    @Test
    public void testPropagateAndBackoffAfterFatalError() {
        CoordinatorRequestManager coordinatorManager = setupCoordinatorManager(GROUP_ID);
        expectFindCoordinatorRequest(coordinatorManager, Errors.GROUP_AUTHORIZATION_FAILED);

        verify(backgroundEventHandler).add(argThat(backgroundEvent -> {
            if (!(backgroundEvent instanceof ErrorEvent))
                return false;

            RuntimeException exception = ((ErrorEvent) backgroundEvent).error();

            if (!(exception instanceof GroupAuthorizationException))
                return false;

            GroupAuthorizationException groupAuthException = (GroupAuthorizationException) exception;
            return groupAuthException.groupId().equals(GROUP_ID);
        }));

        time.sleep(RETRY_BACKOFF_MS - 1);
        assertEquals(Collections.emptyList(), coordinatorManager.poll(time.milliseconds()).unsentRequests);

        time.sleep(1);
        assertEquals(1, coordinatorManager.poll(time.milliseconds()).unsentRequests.size());
        assertEquals(Optional.empty(), coordinatorManager.coordinator());
    }

    @Test
    public void testNullGroupIdShouldThrow() {
        assertThrows(RuntimeException.class, () -> setupCoordinatorManager(null));
    }

    @Test
    public void testFindCoordinatorResponseVersions() {
        // v4
        FindCoordinatorResponse respNew = FindCoordinatorResponse.prepareResponse(Errors.NONE, GROUP_ID, this.node);
        assertTrue(respNew.coordinatorByKey(GROUP_ID).isPresent());
        assertEquals(GROUP_ID, respNew.coordinatorByKey(GROUP_ID).get().key());
        assertEquals(this.node.id(), respNew.coordinatorByKey(GROUP_ID).get().nodeId());

        // <= v3
        FindCoordinatorResponse respOld = FindCoordinatorResponse.prepareOldResponse(Errors.NONE, this.node);
        assertTrue(respOld.coordinatorByKey(GROUP_ID).isPresent());
        assertEquals(this.node.id(), respNew.coordinatorByKey(GROUP_ID).get().nodeId());
    }

    @Test
    public void testNetworkTimeout() {
        CoordinatorRequestManager coordinatorManager = setupCoordinatorManager(GROUP_ID);
        NetworkClientDelegate.PollResult res = coordinatorManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        // Mimic a network timeout
        res.unsentRequests.get(0).handler().onFailure(time.milliseconds(), new TimeoutException());

        // Sleep for exponential backoff - 1ms
        time.sleep(RETRY_BACKOFF_MS - 1);
        NetworkClientDelegate.PollResult res2 = coordinatorManager.poll(this.time.milliseconds());
        assertEquals(0, res2.unsentRequests.size());

        time.sleep(1);
        res2 = coordinatorManager.poll(time.milliseconds());
        assertEquals(1, res2.unsentRequests.size());
    }

    private void expectFindCoordinatorRequest(
        CoordinatorRequestManager  coordinatorManager,
        Errors error
    ) {
        NetworkClientDelegate.PollResult res = coordinatorManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        unsentRequest.handler().onComplete(buildResponse(unsentRequest, error));

        boolean expectCoordinatorFound = error == Errors.NONE;
        assertEquals(expectCoordinatorFound, coordinatorManager.coordinator().isPresent());
    }

    private CoordinatorRequestManager setupCoordinatorManager(String groupId) {
        return new CoordinatorRequestManager(
            new LogContext(),
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MS,
            this.backgroundEventHandler,
            groupId
        );
    }

    private ClientResponse buildResponse(
        NetworkClientDelegate.UnsentRequest request,
        Errors error
    ) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertInstanceOf(FindCoordinatorRequest.class, abstractRequest);
        FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) abstractRequest;

        FindCoordinatorResponse findCoordinatorResponse =
            FindCoordinatorResponse.prepareResponse(error, GROUP_ID, node);
        return new ClientResponse(
            new RequestHeader(ApiKeys.FIND_COORDINATOR, findCoordinatorRequest.version(), "", 1),
            request.handler(),
            node.idString(),
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            findCoordinatorResponse
        );
    }
}
