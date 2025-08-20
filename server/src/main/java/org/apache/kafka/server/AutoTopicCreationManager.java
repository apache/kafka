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

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.requests.RequestContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface AutoTopicCreationManager {

    /**
     * Initiate auto topic creation for the given topics.
     *
     * @param topics the topics to create
     * @param metadataRequestContext defined when creating topics on behalf of the client. The goal here is to preserve
     *                               original client principal for auditing, thus needing to wrap a plain CreateTopicsRequest
     *                               inside Envelope to send to the controller when forwarding is enabled.
     * @return auto created topic metadata responses
     */
    List<MetadataResponseTopic> createTopics(
            Set<String> topics,
            Optional<RequestContext> metadataRequestContext
    );

    /**
     * Initiate auto topic creation for the given topics.
     * This method is used for creating internal topics for streams.
     *
     * @param topics the topics to create
     * @param metadataRequestContext defined when creating topics on behalf of the client. The goal here is to preserve
     *                               original client principal for auditing, thus needing to wrap a plain CreateTopicsRequest
     *                               inside Envelope to send to the controller when forwarding is enabled.
     */
    void createStreamsInternalTopics(
            Map<String, CreatableTopic> topics,
            RequestContext metadataRequestContext
    );
}
