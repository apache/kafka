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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class MetadataRequest extends AbstractRequest {

    private static final String TOPICS_KEY_NAME = "topics";
    private static final String ALLOW_AUTO_TOPIC_CREATION_KEY_NAME = "allow_auto_topic_creation";

    private static final Schema METADATA_REQUEST_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(STRING), "An array of topics to fetch metadata for. If no topics are specified fetch metadata for all topics."));

    private static final Schema METADATA_REQUEST_V1 = new Schema(
            new Field(TOPICS_KEY_NAME, ArrayOf.nullable(STRING), "An array of topics to fetch metadata for. If the topics array is null fetch metadata for all topics."));

    /* The v2 metadata request is the same as v1. An additional field for cluster id has been added to the v2 metadata response */
    private static final Schema METADATA_REQUEST_V2 = METADATA_REQUEST_V1;

    /* The v3 metadata request is the same as v1 and v2. An additional field for throttle time has been added to the v3 metadata response */
    private static final Schema METADATA_REQUEST_V3 = METADATA_REQUEST_V2;

    /* The v4 metadata request has an additional field for allowing auto topic creation. The response is the same as v3. */
    private static final Schema METADATA_REQUEST_V4 = new Schema(
            new Field(TOPICS_KEY_NAME, ArrayOf.nullable(STRING), "An array of topics to fetch metadata for. " +
                    "If the topics array is null fetch metadata for all topics."),
            new Field(ALLOW_AUTO_TOPIC_CREATION_KEY_NAME, BOOLEAN, "If this and the broker config " +
                    "'auto.create.topics.enable' are true, topics that don't exist will be created by the broker. " +
                    "Otherwise, no topics will be created by the broker."));

    /* The v5 metadata request is the same as v4. An additional field for offline_replicas has been added to the v5 metadata response */
    private static final Schema METADATA_REQUEST_V5 = METADATA_REQUEST_V4;

    public static Schema[] schemaVersions() {
        return new Schema[] {METADATA_REQUEST_V0, METADATA_REQUEST_V1, METADATA_REQUEST_V2, METADATA_REQUEST_V3,
            METADATA_REQUEST_V4, METADATA_REQUEST_V5};
    }

    public static class Builder extends AbstractRequest.Builder<MetadataRequest> {
        private static final List<String> ALL_TOPICS = null;

        // The list of topics, or null if we want to request metadata about all topics.
        private final List<String> topics;
        private final boolean allowAutoTopicCreation;

        public static Builder allTopics() {
            // This never causes auto-creation, but we set the boolean to true because that is the default value when
            // deserializing V2 and older. This way, the value is consistent after serialization and deserialization.
            return new Builder(ALL_TOPICS, true);
        }

        public Builder(List<String> topics, boolean allowAutoTopicCreation) {
            super(ApiKeys.METADATA);
            this.topics = topics;
            this.allowAutoTopicCreation = allowAutoTopicCreation;
        }

        public List<String> topics() {
            return this.topics;
        }

        public boolean isAllTopics() {
            return this.topics == ALL_TOPICS;
        }

        @Override
        public MetadataRequest build(short version) {
            if (version < 1)
                throw new UnsupportedVersionException("MetadataRequest versions older than 1 are not supported.");
            if (!allowAutoTopicCreation && version < 4)
                throw new UnsupportedVersionException("MetadataRequest versions older than 4 don't support the " +
                        "allowAutoTopicCreation field");
            return new MetadataRequest(this.topics, allowAutoTopicCreation, version);
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

    private final List<String> topics;
    private final boolean allowAutoTopicCreation;

    /**
     * In v0 null is not allowed and an empty list indicates requesting all topics.
     * Note: modern clients do not support sending v0 requests.
     * In v1 null indicates requesting all topics, and an empty list indicates requesting no topics.
     */
    public MetadataRequest(List<String> topics, boolean allowAutoTopicCreation, short version) {
        super(version);
        this.topics = topics;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
    }

    public MetadataRequest(Struct struct, short version) {
        super(version);
        Object[] topicArray = struct.getArray(TOPICS_KEY_NAME);
        if (topicArray != null) {
            topics = new ArrayList<>();
            for (Object topicObj: topicArray) {
                topics.add((String) topicObj);
            }
        } else {
            topics = null;
        }
        if (struct.hasField(ALLOW_AUTO_TOPIC_CREATION_KEY_NAME))
            allowAutoTopicCreation = struct.getBoolean(ALLOW_AUTO_TOPIC_CREATION_KEY_NAME);
        else
            allowAutoTopicCreation = true;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
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
                return new MetadataResponse(Collections.<Node>emptyList(), null, MetadataResponse.NO_CONTROLLER_ID, topicMetadatas);
            case 3:
            case 4:
            case 5:
                return new MetadataResponse(throttleTimeMs, Collections.<Node>emptyList(), null, MetadataResponse.NO_CONTROLLER_ID, topicMetadatas);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.METADATA.latestVersion()));
        }
    }

    public boolean isAllTopics() {
        return topics == null;
    }

    public List<String> topics() {
        return topics;
    }

    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    public static MetadataRequest parse(ByteBuffer buffer, short version) {
        return new MetadataRequest(ApiKeys.METADATA.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.METADATA.requestSchema(version()));
        if (topics == null)
            struct.set(TOPICS_KEY_NAME, null);
        else
            struct.set(TOPICS_KEY_NAME, topics.toArray());
        if (struct.hasField(ALLOW_AUTO_TOPIC_CREATION_KEY_NAME))
            struct.set(ALLOW_AUTO_TOPIC_CREATION_KEY_NAME, allowAutoTopicCreation);
        return struct;
    }
}
