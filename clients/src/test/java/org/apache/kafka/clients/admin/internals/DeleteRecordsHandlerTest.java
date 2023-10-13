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
package org.apache.kafka.clients.admin.internals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteRecordsHandlerTest {
    private final LogContext logContext = new LogContext();
    private final int timeout = 2000;
    private final TopicPartition t0p0 = new TopicPartition("t0", 0);
    private final TopicPartition t0p1 = new TopicPartition("t0", 1);
    private final TopicPartition t0p2 = new TopicPartition("t0", 2);
    private final TopicPartition t0p3 = new TopicPartition("t0", 3);
    private final Node node = new Node(1, "host", 1234);
    private final Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<TopicPartition, RecordsToDelete>() {
        {
            put(t0p0, RecordsToDelete.beforeOffset(10L));
            put(t0p1, RecordsToDelete.beforeOffset(10L));
            put(t0p2, RecordsToDelete.beforeOffset(10L));
            put(t0p3, RecordsToDelete.beforeOffset(10L));
        }
    };

    @Test
    public void testBuildRequestSimple() {
        DeleteRecordsHandler handler = new DeleteRecordsHandler(recordsToDelete, logContext, timeout);
        DeleteRecordsRequest request = handler.buildBatchedRequest(node.id(), mkSet(t0p0, t0p1)).build();
        List<DeleteRecordsRequestData.DeleteRecordsTopic> topicPartitions = request.data().topics();
        assertEquals(1, topicPartitions.size());
        DeleteRecordsRequestData.DeleteRecordsTopic topic = topicPartitions.get(0);
        assertEquals(4, topic.partitions().size());
    }

    @Test
    public void testHandleSuccessfulResponse() {
        AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> result =
                handleResponse(createResponse(emptyMap(), recordsToDelete.keySet()));
        assertResult(result, recordsToDelete.keySet(), emptyMap(), emptyList(), emptySet());
    }

    @Test
    public void testHandleRetriablePartitionTimeoutResponse() {
        TopicPartition errorPartition = t0p0;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, Errors.REQUEST_TIMED_OUT.code());

        AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> result =
                handleResponse(createResponse(errorsByPartition));

        // Timeouts should be retried within the fulfillment stage as they are a common type of
        // retriable error.
        Set<TopicPartition> retriable = singleton(errorPartition);
        Set<TopicPartition> completed = new HashSet<>(recordsToDelete.keySet());
        completed.removeAll(retriable);
        assertResult(result, completed, emptyMap(), emptyList(), retriable);
    }

    @Test
    public void testHandleLookupRetriablePartitionInvalidMetadataResponse() {
        TopicPartition errorPartition = t0p0;
        Errors error = Errors.NOT_LEADER_OR_FOLLOWER;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, error.code());

        AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> result =
                handleResponse(createResponse(errorsByPartition));

        // Some invalid metadata errors should be retried from the lookup stage as the partition-to-leader
        // mappings should be recalculated.
        List<TopicPartition> unmapped = new ArrayList<>();
        unmapped.add(errorPartition);
        Set<TopicPartition> completed = new HashSet<>(recordsToDelete.keySet());
        completed.removeAll(unmapped);
        assertResult(result, completed, emptyMap(), unmapped, emptySet());
    }

    @Test
    public void testHandlePartitionErrorResponse() {
        TopicPartition errorPartition = t0p0;
        Errors error = Errors.TOPIC_AUTHORIZATION_FAILED;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, error.code());

        AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> result =
                handleResponse(createResponse(errorsByPartition));

        Map<TopicPartition, Throwable> failed = new HashMap<>();
        failed.put(errorPartition, error.exception());
        Set<TopicPartition> completed = new HashSet<>(recordsToDelete.keySet());
        completed.removeAll(failed.keySet());
        assertResult(result, completed, failed, emptyList(), emptySet());
    }

    @Test
    public void testHandleUnexpectedPartitionErrorResponse() {
        TopicPartition errorPartition = t0p0;
        Errors error = Errors.UNKNOWN_SERVER_ERROR;
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();
        errorsByPartition.put(errorPartition, error.code());

        AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> result =
                handleResponse(createResponse(errorsByPartition));

        Map<TopicPartition, Throwable> failed = new HashMap<>();
        failed.put(errorPartition, error.exception());
        Set<TopicPartition> completed = new HashSet<>(recordsToDelete.keySet());
        completed.removeAll(failed.keySet());
        assertResult(result, completed, failed, emptyList(), emptySet());
    }

    @Test
    public void testMixedResponse() {
        Map<TopicPartition, Short> errorsByPartition = new HashMap<>();

        TopicPartition errorPartition = t0p0;
        Errors error = Errors.UNKNOWN_SERVER_ERROR;
        errorsByPartition.put(errorPartition, error.code());

        TopicPartition retriableErrorPartition = t0p1;
        Errors retriableError = Errors.NOT_LEADER_OR_FOLLOWER;
        errorsByPartition.put(retriableErrorPartition, retriableError.code());

        TopicPartition retriableErrorPartition2 = t0p2;
        Errors retriableError2 = Errors.REQUEST_TIMED_OUT;
        errorsByPartition.put(retriableErrorPartition2, retriableError2.code());

        AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> result =
                handleResponse(createResponse(errorsByPartition));

        Set<TopicPartition> completed = new HashSet<>(recordsToDelete.keySet());

        Map<TopicPartition, Throwable> failed = new HashMap<>();
        failed.put(errorPartition, error.exception());
        completed.removeAll(failed.keySet());

        List<TopicPartition> unmapped = new ArrayList<>();
        unmapped.add(retriableErrorPartition);
        completed.removeAll(unmapped);

        Set<TopicPartition> retriable = singleton(retriableErrorPartition2);
        completed.removeAll(retriable);

        assertResult(result, completed, failed, unmapped, retriable);
    }

    @Test
    public void testHandleResponseSanityCheck() {
        TopicPartition errorPartition = t0p0;
        Map<TopicPartition, RecordsToDelete> recordsToDeleteMap = new HashMap<>(recordsToDelete);
        recordsToDeleteMap.remove(errorPartition);

        AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> result =
                handleResponse(createResponse(emptyMap(), recordsToDeleteMap.keySet()));

        assertEquals(recordsToDelete.size() - 1, result.completedKeys.size());
        assertEquals(1, result.failedKeys.size());
        assertEquals(errorPartition, result.failedKeys.keySet().iterator().next());
        String sanityCheckMessage = result.failedKeys.get(errorPartition).getMessage();
        assertTrue(sanityCheckMessage.contains("did not contain a result for topic partition"));
        assertTrue(result.unmappedKeys.isEmpty());
    }

    private DeleteRecordsResponse createResponse(Map<TopicPartition, Short> errorsByPartition) {
        return createResponse(errorsByPartition, recordsToDelete.keySet());
    }

    private DeleteRecordsResponse createResponse(
            Map<TopicPartition, Short> errorsByPartition,
            Set<TopicPartition> topicPartitions
    ) {
        Map<String, DeleteRecordsResponseData.DeleteRecordsTopicResultCollection> responsesByTopic = new HashMap<>();

        DeleteRecordsResponseData.DeleteRecordsTopicResultCollection topicResponse = null;
        for (TopicPartition topicPartition : topicPartitions) {
            topicResponse = responsesByTopic.computeIfAbsent(
                    topicPartition.topic(), t -> new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection());
            topicResponse.add(new DeleteRecordsResponseData.DeleteRecordsTopicResult().setName(topicPartition.topic()));
            DeleteRecordsResponseData.DeleteRecordsPartitionResult partitionResponse = new DeleteRecordsResponseData.DeleteRecordsPartitionResult();
            partitionResponse.setPartitionIndex(topicPartition.partition());
            partitionResponse.setErrorCode(errorsByPartition.getOrDefault(topicPartition, (short) 0));
            topicResponse.find(topicPartition.topic()).partitions().add(partitionResponse);
        }
        DeleteRecordsResponseData responseData = new DeleteRecordsResponseData();
        responseData.setTopics(topicResponse);
        return new DeleteRecordsResponse(responseData);
    }

    private AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> handleResponse(DeleteRecordsResponse response) {
        DeleteRecordsHandler handler =
                new DeleteRecordsHandler(recordsToDelete, logContext, timeout);
        return handler.handleResponse(node, recordsToDelete.keySet(), response);
    }

    private void assertResult(
            AdminApiHandler.ApiResult<TopicPartition, DeletedRecords> result,
            Set<TopicPartition> expectedCompleted,
            Map<TopicPartition, Throwable> expectedFailed,
            List<TopicPartition> expectedUnmapped,
            Set<TopicPartition> expectedRetriable
    ) {
        assertEquals(expectedCompleted, result.completedKeys.keySet());
        assertEquals(expectedFailed, result.failedKeys);
        assertEquals(expectedUnmapped, result.unmappedKeys);
        Set<TopicPartition> actualRetriable = new HashSet<>(recordsToDelete.keySet());
        actualRetriable.removeAll(result.completedKeys.keySet());
        actualRetriable.removeAll(result.failedKeys.keySet());
        actualRetriable.removeAll(new HashSet<>(result.unmappedKeys));
        assertEquals(expectedRetriable, actualRetriable);
    }
}
