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
import org.apache.kafka.server.quota.ControllerMutationQuota;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AutoTopicCreationManager {

    /**
     * Initiate auto topic creation for the given topics.
     *
     * @param topics the topics to create
     * @param controllerMutationQuota the controller mutation quota for topic creation
     * @param metadataRequestContext defined when creating topics on behalf of the client. The goal here is to preserve
     *                               original client principal for auditing, thus needing to wrap a plain CreateTopicsRequest
     *                               inside Envelope to send to the controller when forwarding is enabled.
     * @return auto created topic metadata responses
     */
    List<MetadataResponseTopic> createTopics(Set<String> topics, ControllerMutationQuota controllerMutationQuota, RequestContext metadataRequestContext);

    /**
     * Initiate auto topic creation for the given topics without providing a RequestContext.
     * This is a convenience method that calls the main createTopics method with a null RequestContext.
     *
     * @param topics the topics to create
     * @param controllerMutationQuota the controller mutation quota for topic creation
     * @return auto created topic metadata responses
     */
    default List<MetadataResponseTopic> createTopics(Set<String> topics, ControllerMutationQuota controllerMutationQuota) {
        return createTopics(topics, controllerMutationQuota, null);
    }

    /**
     * Initiate auto topic creation for the given topics.
     * This method is used for creating internal topics for streams.
     *
     * @param topics the topics to create
     * @param metadataRequestContext defined when creating topics on behalf of the client. The goal here is to preserve
     *                               original client principal for auditing, thus needing to wrap a plain CreateTopicsRequest
     *                               inside Envelope to send to the controller when forwarding is enabled.
     * @param timeoutMs the time in milliseconds for which topic creation errors should be cached. This serves as the
     *                  TTL (Time To Live) for error cache entries. If topic creation fails, the error will be cached
     *                  for this duration to avoid repeated failed attempts and provide consistent error responses
     *                  during streams group heartbeat requests.
     */
    void createStreamsInternalTopics(Map<String, CreatableTopic> topics, RequestContext metadataRequestContext, long timeoutMs);

    /**
     * Retrieve cached topic creation errors for the specified streams internal topics.
     * This method returns error messages for topics that failed to be created and are still
     * within their cache TTL period. Only non-expired error entries are returned.
     *
     * @param topicNames the set of topic names to check for cached errors
     * @param currentTimeMs the current time in milliseconds, used to filter out expired cache entries
     * @return a map of topic names to their corresponding error messages for topics that have
     *         cached errors and are not yet expired. Empty map if no cached errors exist for the topics.
     */
    Map<String, String> getStreamsInternalTopicCreationErrors(Set<String> topicNames, long currentTimeMs);

    /**
     * Close the AutoTopicCreationManager and clean up any resources.
     */
    void close();
}
