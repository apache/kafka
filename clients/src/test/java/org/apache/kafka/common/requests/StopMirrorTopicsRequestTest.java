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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.StopMirrorTopicsRequestData;
import org.apache.kafka.common.message.StopMirrorTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StopMirrorTopicsRequestTest {
    @Test
    void testUnstableForwardableApi() {
        assertTrue(ApiKeys.STOP_MIRROR_TOPICS.forwardable);
        assertTrue(ApiKeys.brokerApis().contains(ApiKeys.STOP_MIRROR_TOPICS));
        assertTrue(ApiKeys.controllerApis().contains(ApiKeys.STOP_MIRROR_TOPICS));
        assertEquals(-1, ApiKeys.STOP_MIRROR_TOPICS.latestVersion(false));
        assertEquals(0, ApiKeys.STOP_MIRROR_TOPICS.latestVersion(true));
        assertThrows(UnsupportedVersionException.class,
            () -> new StopMirrorTopicsRequest.Builder(new StopMirrorTopicsRequestData()).build());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRequestRoundTrip(boolean usePatterns) {
        StopMirrorTopicsRequestData data = new StopMirrorTopicsRequestData()
            .setMirrorName("mirror-a")
            .setTimeoutMs(30_000);
        assertEquals(-1, data.stateOffset());
        if (usePatterns) {
            data.setTopics(null).setTopicPatterns(List.of("topic-.*"));
        } else {
            data.topics().add(new StopMirrorTopicsRequestData.TopicMetadata().setTopicName("topic-a"));
            data.setTopicPatterns(null);
        }
        data.setStateOffset(42); // Metadata log offset used to validate partition state.
        StopMirrorTopicsRequest request = new StopMirrorTopicsRequest.Builder(data, true).build();
        AbstractRequest parsed = AbstractRequest.parseRequest(request.apiKey(), request.version(), request.serialize()).request;
        assertEquals(data, parsed.data());
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"UNSUPPORTED_VERSION", "CLUSTER_MIRROR_AUTHORIZATION_FAILED"})
    void testErrorResponse(Errors error) {
        StopMirrorTopicsRequest request = new StopMirrorTopicsRequest.Builder(
            new StopMirrorTopicsRequestData().setMirrorName("mirror-a"), true).build();
        StopMirrorTopicsResponse response = request.getErrorResponse(25, error.exception("request rejected"));
        assertEquals(error.code(), response.data().errorCode());
        assertEquals("request rejected", response.data().errorMessage());
        assertEquals(25, response.throttleTimeMs());
        assertEquals(Map.of(error, 1), response.errorCounts());
    }

    @Test
    void testResponseRoundTripAndErrorCounts() {
        StopMirrorTopicsResponseData data = new StopMirrorTopicsResponseData()
            .setErrorCode(Errors.NONE.code())
            .setTopics(new StopMirrorTopicsResponseData.TopicResultCollection(List.of(
                new StopMirrorTopicsResponseData.TopicResult().setTopicName("topic-a")
                    .setErrorCode(Errors.TOPIC_NOT_IN_CLUSTER_MIRROR.code()).setErrorMessage("Not in mirror"),
                new StopMirrorTopicsResponseData.TopicResult().setTopicName("topic-b")
                    .setErrorCode(Errors.TOPIC_NOT_IN_CLUSTER_MIRROR.code())).iterator()));
        StopMirrorTopicsResponse response = new StopMirrorTopicsResponse(data);
        response.maybeSetThrottleTimeMs(25);
        assertTrue(response.shouldClientThrottle((short) 0));
        assertEquals(25, response.throttleTimeMs());
        assertEquals(Map.of(Errors.NONE, 1, Errors.TOPIC_NOT_IN_CLUSTER_MIRROR, 2), response.errorCounts());
        StopMirrorTopicsResponse parsed = StopMirrorTopicsResponse.parse(
            response.serialize((short) 0), (short) 0);
        assertEquals(data, parsed.data());
        assertEquals(Errors.TOPIC_NOT_IN_CLUSTER_MIRROR.code(),
            parsed.data().topics().find("topic-a").errorCode());
    }
}
