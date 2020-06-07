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
package org.apache.kafka.streams.processor.internals;

import static java.util.Collections.emptyList;
import static org.apache.kafka.streams.processor.internals.ClientUtils.fetchEndOffsets;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.errors.StreamsException;
import org.easymock.EasyMock;
import org.junit.Test;

public class ClientUtilsTest {

    @Test
    public void fetchEndOffsetsShouldRethrowRuntimeExceptionAsStreamsException() {
        final Admin adminClient = EasyMock.createMock(AdminClient.class);
        EasyMock.expect(adminClient.listOffsets(EasyMock.anyObject())).andThrow(new RuntimeException());
        replay(adminClient);
        assertThrows(StreamsException.class, () ->  fetchEndOffsets(emptyList(), adminClient));
        verify(adminClient);
    }

    @Test
    public void fetchEndOffsetsShouldRethrowInterruptedExceptionAsStreamsException() throws Exception {
        final Admin adminClient = EasyMock.createMock(AdminClient.class);
        final ListOffsetsResult result = EasyMock.createNiceMock(ListOffsetsResult.class);
        final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = EasyMock.createMock(KafkaFuture.class);

        EasyMock.expect(adminClient.listOffsets(EasyMock.anyObject())).andStubReturn(result);
        EasyMock.expect(result.all()).andStubReturn(allFuture);
        EasyMock.expect(allFuture.get()).andThrow(new InterruptedException());
        replay(adminClient, result, allFuture);

        assertThrows(StreamsException.class, () -> fetchEndOffsets(emptyList(), adminClient));
        verify(adminClient);
    }

    @Test
    public void fetchEndOffsetsShouldRethrowExecutionExceptionAsStreamsException() throws Exception {
        final Admin adminClient = EasyMock.createMock(AdminClient.class);
        final ListOffsetsResult result = EasyMock.createNiceMock(ListOffsetsResult.class);
        final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = EasyMock.createMock(KafkaFuture.class);

        EasyMock.expect(adminClient.listOffsets(EasyMock.anyObject())).andStubReturn(result);
        EasyMock.expect(result.all()).andStubReturn(allFuture);
        EasyMock.expect(allFuture.get()).andThrow(new ExecutionException(new RuntimeException()));
        replay(adminClient, result, allFuture);

        assertThrows(StreamsException.class, () -> fetchEndOffsets(emptyList(), adminClient));
        verify(adminClient);
    }

    @Test
    public void fetchEndOffsetsWithTimeoutShouldRethrowTimeoutExceptionAsStreamsException() throws Exception {
        final Admin adminClient = EasyMock.createMock(AdminClient.class);
        final ListOffsetsResult result = EasyMock.createNiceMock(ListOffsetsResult.class);
        final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> allFuture = EasyMock.createMock(KafkaFuture.class);

        EasyMock.expect(adminClient.listOffsets(EasyMock.anyObject())).andStubReturn(result);
        EasyMock.expect(result.all()).andStubReturn(allFuture);
        EasyMock.expect(allFuture.get()).andThrow(new TimeoutException());
        replay(adminClient, result, allFuture);

        assertThrows(StreamsException.class, () -> fetchEndOffsets(emptyList(), adminClient));
        verify(adminClient);
    }

}
