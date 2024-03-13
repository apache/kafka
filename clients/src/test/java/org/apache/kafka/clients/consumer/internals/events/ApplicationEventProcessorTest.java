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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.HeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.MembershipManager;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApplicationEventProcessorTest {
    private final Time time = new MockTime(1);
    private ApplicationEventProcessor processor;
    private BlockingQueue applicationEventQueue = mock(BlockingQueue.class);
    private RequestManagers requestManagers;
    private ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private NetworkClientDelegate networkClientDelegate = mock(NetworkClientDelegate.class);
    private OffsetsRequestManager offsetRequestManager;
    private OffsetsRequestManager offsetsRequestManager;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private FetchRequestManager fetchRequestManager;
    private CoordinatorRequestManager coordinatorRequestManager;
    private CommitRequestManager commitRequestManager;
    private HeartbeatRequestManager heartbeatRequestManager;
    private MembershipManager membershipManager;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        LogContext logContext = new LogContext();
        offsetRequestManager = mock(OffsetsRequestManager.class);
        offsetsRequestManager = mock(OffsetsRequestManager.class);
        topicMetadataRequestManager = mock(TopicMetadataRequestManager.class);
        fetchRequestManager = mock(FetchRequestManager.class);
        coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        commitRequestManager = mock(CommitRequestManager.class);
        heartbeatRequestManager = mock(HeartbeatRequestManager.class);
        membershipManager = mock(MembershipManager.class);
        requestManagers = new RequestManagers(
            logContext,
            offsetsRequestManager,
            topicMetadataRequestManager,
            fetchRequestManager,
            Optional.of(coordinatorRequestManager),
            Optional.of(commitRequestManager),
            Optional.of(heartbeatRequestManager),
            Optional.of(membershipManager)
        );
        processor = new ApplicationEventProcessor(
            new LogContext(),
            applicationEventQueue,
            requestManagers,
            metadata
        );
    }

    @Test
    public void testPrepClosingCommitEvents() {
        List<NetworkClientDelegate.UnsentRequest> results = mockCommitResults();
        doReturn(new NetworkClientDelegate.PollResult(100, results)).when(commitRequestManager).pollOnClose();
        processor.process(new CommitOnCloseEvent());
        verify(commitRequestManager).signalClose();
    }

    @Test
    public void testPrepClosingLeaveGroupEvent() {
        Timer timer = time.timer(100);
        LeaveOnCloseEvent event = new LeaveOnCloseEvent(timer);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(membershipManager.leaveGroup()).thenReturn(CompletableFuture.completedFuture(null));
        processor.process(event);
        verify(membershipManager).leaveGroup();
        assertTrue(event.future().isDone());
    }

    private List<NetworkClientDelegate.UnsentRequest> mockCommitResults() {
        return Collections.singletonList(mock(NetworkClientDelegate.UnsentRequest.class));
    }
}
