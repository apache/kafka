/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetadataRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<MetadataRequest> {
        private static final List<String> ALL_TOPICS = null;

        // The list of topics, or null if we want to request metadata about all topics.
        private final List<String> topics;

        public static Builder allTopics() {
            return new Builder(ALL_TOPICS);
        }

        public Builder(List<String> topics) {
            super(ApiKeys.METADATA);
            this.topics = topics;
        }

        public List<String> topics() {
            return this.topics;
        }

        public boolean isAllTopics() {
            return this.topics == ALL_TOPICS;
        }

        @Override
        public MetadataRequest build() {
            short version = version();
            if (version < 1) {
                throw new UnsupportedVersionException("MetadataRequest " +
                        "versions older than 1 are not supported.");
            }
            return new MetadataRequest(this.topics, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=MetadataRequest").
                append(", topics=");
            if (topics == null) {
                bld.append("<ALL>");
            } else {
                bld.append(Utils.join(topics, ","));
            }
            bld.append(")");
            return bld.toString();
        }
    }

    private static final String TOPICS_KEY_NAME = "topics";

    private final List<String> topics;

    public static MetadataRequest allTopics(short version) {
        return new MetadataRequest.Builder(null).setVersion(version).build();
    }

    /**
     * In v0 null is not allowed and and empty list indicates requesting all topics.
     * Note: modern clients do not support sending v0 requests.
     * In v1 null indicates requesting all topics, and an empty list indicates requesting no topics.
     */
    public MetadataRequest(List<String> topics, short version) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.METADATA.id, version)),
                version);
        if (topics == null)
            struct.set(TOPICS_KEY_NAME, null);
        else
            struct.set(TOPICS_KEY_NAME, topics.toArray());
        this.topics = topics;
    }

    public MetadataRequest(Struct struct, short version) {
        super(struct, version);
        Object[] topicArray = struct.getArray(TOPICS_KEY_NAME);
        if (topicArray != null) {
            topics = new ArrayList<>();
            for (Object topicObj: topicArray) {
                topics.add((String) topicObj);
            }
        } else {
            topics = null;
        }
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        List<MetadataResponse.TopicMetadata> topicMetadatas = new ArrayList<>();
        Errors error = Errors.forException(e);
        List<MetadataResponse.PartitionMetadata> partitions = Collections.emptyList();

        if (topics != null) {
            for (String topic : topics)
                topicMetadatas.add(new MetadataResponse.TopicMetadata(error, topic, false, partitions));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new MetadataResponse(Collections.<Node>emptyList(), null, MetadataResponse.NO_CONTROLLER_ID, topicMetadatas, versionId);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.METADATA.id)));
        }
    }

    public boolean isAllTopics() {
        return topics == null;
    }

    public List<String> topics() {
        return topics;
    }

    public static MetadataRequest parse(ByteBuffer buffer, int versionId) {
        return new MetadataRequest(ProtoUtils.parseRequest(ApiKeys.METADATA.id, versionId, buffer),
                (short) versionId);
    }

    public static MetadataRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.METADATA.id));
    }
}
