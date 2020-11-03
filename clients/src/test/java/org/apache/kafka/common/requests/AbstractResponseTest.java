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

import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AbstractResponseTest {

    @Test
    public void testResponseSerde() {
        CreateTopicsResponseData.CreatableTopicResultCollection collection =
            new CreateTopicsResponseData.CreatableTopicResultCollection();
        collection.add(new CreateTopicsResponseData.CreatableTopicResult()
                           .setTopicConfigErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                           .setNumPartitions(5));
        CreateTopicsResponse createTopicsResponse = new CreateTopicsResponse(
            new CreateTopicsResponseData()
                .setThrottleTimeMs(10)
                .setTopics(collection)
        );

        final short version = (short) (CreateTopicsResponseData.SCHEMAS.length - 1);
        final RequestHeader header = new RequestHeader(ApiKeys.CREATE_TOPICS, version, "client", 4);

        final EnvelopeResponse envelopeResponse = new EnvelopeResponse(
            createTopicsResponse.serialize(version, header.toResponseHeader()),
            Errors.NONE
        );

        CreateTopicsResponse extractedResponse = (CreateTopicsResponse) CreateTopicsResponse.deserializeBody(
            envelopeResponse.responseData(), header);
        assertEquals(createTopicsResponse.data(), extractedResponse.data());
    }
}
