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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedProtocolFieldException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<MetadataRequest> {
        private static final MetadataRequestData ALL_TOPICS_REQUEST_DATA = new MetadataRequestData().
            setTopics(null).setAllowAutoTopicCreation(true);

        private final MetadataRequestData data;

        public Builder(MetadataRequestData data) {
            super(ApiKeys.METADATA);
            this.data = data;
        }

        public Builder(List<String> topics, boolean allowAutoTopicCreation, short allowedVersion) {
            this(topics, allowAutoTopicCreation, allowedVersion, allowedVersion);
        }

        public Builder(List<String> topics, boolean allowAutoTopicCreation, short minVersion, short maxVersion) {
            super(ApiKeys.METADATA, minVersion, maxVersion);
            MetadataRequestData data = new MetadataRequestData();
            if (topics == null)
                data.setTopics(null);
            else {
                topics.forEach(topic -> data.topics().add(new MetadataRequestTopic().setName(topic)));
            }

            data.setAllowAutoTopicCreation(allowAutoTopicCreation);
            this.data = data;
        }

        public Builder(List<String> topics, boolean allowAutoTopicCreation) {
            this(topics, allowAutoTopicCreation, ApiKeys.METADATA.oldestVersion(),  ApiKeys.METADATA.latestVersion());
        }

        public Builder(List<Uuid> topicIds) {
            super(ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
            MetadataRequestData data = new MetadataRequestData();
            if (topicIds == null)
                data.setTopics(null);
            else {
                topicIds.forEach(topicId -> data.topics().add(new MetadataRequestTopic().setTopicId(topicId)));
            }

            // It's impossible to create topic with topicId
            data.setAllowAutoTopicCreation(false);
            this.data = data;
        }

        public static Builder allTopics() {
            // This never causes auto-creation, but we set the boolean to true because that is the default value when
            // deserializing V2 and older. This way, the value is consistent after serialization and deserialization.
            return new Builder(ALL_TOPICS_REQUEST_DATA);
        }

        public boolean emptyTopicList() {
            return data.topics().isEmpty();
        }

        public boolean isAllTopics() {
            return data.topics() == null;
        }

        public List<String> topics() {
            return data.topics()
                .stream()
                .map(MetadataRequestTopic::name)
                .collect(Collectors.toList());
        }

        @Override
        public MetadataRequest build(short version) {
            if (version < 1)
                throw new UnsupportedVersionException("MetadataRequest versions older than 1 are not supported.");
            if (!data.allowAutoTopicCreation() && version < 4)
                throw new UnsupportedProtocolFieldException("allowAutoTopicCreation", apiKey().name(), version, 4);
            if (data.topics() != null) {
                data.topics().forEach(topic -> {
                    if (topic.name() == null && version < 12)
                        throw new UnsupportedProtocolFieldException("null topic names", apiKey().name(), version, 12);
                    if (!Uuid.ZERO_UUID.equals(topic.topicId()) && version < 12)
                        throw new UnsupportedProtocolFieldException("non-zero topic IDs", apiKey().name(), version, 12);
                });
            }
            return new MetadataRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final MetadataRequestData data;

    public MetadataRequest(MetadataRequestData data, short version) {
        super(ApiKeys.METADATA, version);
        this.data = data;
    }

    @Override
    public MetadataRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        MetadataResponseData responseData = new MetadataResponseData();
        if (data.topics() != null) {
            for (MetadataRequestTopic topic : data.topics()) {
                // the response does not allow null, so convert to empty string if necessary
                String topicName = topic.name() == null ? "" : topic.name();
                responseData.topics().add(new MetadataResponseData.MetadataResponseTopic()
                    .setName(topicName)
                    .setTopicId(topic.topicId())
                    .setErrorCode(error.code())
                    .setIsInternal(false)
                    .setPartitions(Collections.emptyList()));
            }
        }

        responseData.setThrottleTimeMs(throttleTimeMs);
        responseData.setErrorCode(error.code());
        return new MetadataResponse(responseData, true);
    }

    public boolean isAllTopics() {
        return (data.topics() == null) ||
            (data.topics().isEmpty() && version() == 0); // In version 0, an empty topic list indicates
                                                         // "request metadata for all topics."
    }

    public List<String> topics() {
        if (isAllTopics()) // In version 0, we return null for empty topic list
            return null;
        else
            return data.topics()
                .stream()
                .map(MetadataRequestTopic::name)
                .collect(Collectors.toList());
    }

    public List<Uuid> topicIds() {
        if (isAllTopics())
            return Collections.emptyList();
        else if (version() < 10)
            return Collections.emptyList();
        else
            return data.topics()
                    .stream()
                    .map(MetadataRequestTopic::topicId)
                    .collect(Collectors.toList());
    }

    public boolean allowAutoTopicCreation() {
        return data.allowAutoTopicCreation();
    }

    public static MetadataRequest parse(ByteBuffer buffer, short version) {
        return new MetadataRequest(new MetadataRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static List<MetadataRequestTopic> convertToMetadataRequestTopic(final Collection<String> topics) {
        return topics.stream().map(topic -> new MetadataRequestTopic()
            .setName(topic))
            .collect(Collectors.toList());
    }

    public static List<MetadataRequestTopic> convertTopicIdsToMetadataRequestTopic(final Collection<Uuid> topicIds) {
        return topicIds.stream().map(topicId -> new MetadataRequestTopic()
                .setTopicId(topicId))
                .collect(Collectors.toList());
    }
}
