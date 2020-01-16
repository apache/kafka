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
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

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
                throw new UnsupportedVersionException("MetadataRequest versions older than 4 don't support the " +
                        "allowAutoTopicCreation field");
            return new MetadataRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final MetadataRequestData data;
    private final short version;

    public MetadataRequest(MetadataRequestData data, short version) {
        super(ApiKeys.METADATA, version);
        this.data = data;
        this.version = version;
    }

    public MetadataRequest(Struct struct, short version) {
        super(ApiKeys.METADATA, version);
        this.data = new MetadataRequestData(struct, version);
        this.version = version;
    }

    public MetadataRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        MetadataResponseData responseData = new MetadataResponseData();
        if (topics() != null) {
            for (String topic :topics())
                responseData.topics().add(new MetadataResponseData.MetadataResponseTopic()
                    .setName(topic)
                    .setErrorCode(error.code())
                    .setIsInternal(false)
                    .setPartitions(Collections.emptyList()));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new MetadataResponse(responseData);
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
                responseData.setThrottleTimeMs(throttleTimeMs);
                return new MetadataResponse(responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.METADATA.latestVersion()));
        }
    }

    public boolean isAllTopics() {
        return (data.topics() == null) ||
            (data.topics().isEmpty() && version == 0); //In version 0, an empty topic list indicates
                                                         // "request metadata for all topics."
    }

    public List<String> topics() {
        if (isAllTopics()) //In version 0, we return null for empty topic list
            return null;
        else
            return data.topics()
                .stream()
                .map(MetadataRequestTopic::name)
                .collect(Collectors.toList());
    }

    public boolean allowAutoTopicCreation() {
        return data.allowAutoTopicCreation();
    }

    public static MetadataRequest parse(ByteBuffer buffer, short version) {
        return new MetadataRequest(ApiKeys.METADATA.parseRequest(version, buffer), version);
    }

    public static List<MetadataRequestTopic> convertToMetadataRequestTopic(final Collection<String> topics) {
        return topics.stream().map(topic -> new MetadataRequestTopic()
            .setName(topic))
            .collect(Collectors.toList());
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }
}
