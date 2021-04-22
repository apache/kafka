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
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

public class OffsetsForLeaderEpochRequest extends AbstractRequest {

    /**
     * Sentinel replica_id value to indicate a regular consumer rather than another broker
     */
    public static final int CONSUMER_REPLICA_ID = -1;

    /**
     * Sentinel replica_id which indicates either a debug consumer or a replica which is using
     * an old version of the protocol.
     */
    public static final int DEBUGGING_REPLICA_ID = -2;

    private final OffsetForLeaderEpochRequestData data;

    public static class Builder extends AbstractRequest.Builder<OffsetsForLeaderEpochRequest> {
        private final OffsetForLeaderEpochRequestData data;

        Builder(short oldestAllowedVersion, short latestAllowedVersion, OffsetForLeaderEpochRequestData data) {
            super(ApiKeys.OFFSET_FOR_LEADER_EPOCH, oldestAllowedVersion, latestAllowedVersion);
            this.data = data;
        }

        public static Builder forConsumer(OffsetForLeaderTopicCollection epochsByPartition) {
            // Old versions of this API require CLUSTER permission which is not typically granted
            // to clients. Beginning with version 3, the broker requires only TOPIC Describe
            // permission for the topic of each requested partition. In order to ensure client
            // compatibility, we only send this request when we can guarantee the relaxed permissions.
            OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData();
            data.setReplicaId(CONSUMER_REPLICA_ID);
            data.setTopics(epochsByPartition);
            return new Builder((short) 3, ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(), data);
        }

        public static Builder forFollower(short version, OffsetForLeaderTopicCollection epochsByPartition, int replicaId) {
            OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData();
            data.setReplicaId(replicaId);
            data.setTopics(epochsByPartition);
            return new Builder(version, version, data);
        }

        @Override
        public OffsetsForLeaderEpochRequest build(short version) {
            if (version < oldestAllowedVersion() || version > latestAllowedVersion())
                throw new UnsupportedVersionException("Cannot build " + this + " with version " + version);

            return new OffsetsForLeaderEpochRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public OffsetsForLeaderEpochRequest(OffsetForLeaderEpochRequestData data, short version) {
        super(ApiKeys.OFFSET_FOR_LEADER_EPOCH, version);
        this.data = data;
    }

    @Override
    public OffsetForLeaderEpochRequestData data() {
        return data;
    }

    public int replicaId() {
        return data.replicaId();
    }

    public static OffsetsForLeaderEpochRequest parse(ByteBuffer buffer, short version) {
        return new OffsetsForLeaderEpochRequest(new OffsetForLeaderEpochRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        OffsetForLeaderEpochResponseData responseData = new OffsetForLeaderEpochResponseData();
        data.topics().forEach(topic -> {
            OffsetForLeaderTopicResult topicData = new OffsetForLeaderTopicResult()
                .setTopic(topic.topic());
            topic.partitions().forEach(partition ->
                topicData.partitions().add(new EpochEndOffset()
                    .setPartition(partition.partition())
                    .setErrorCode(error.code())
                    .setLeaderEpoch(UNDEFINED_EPOCH)
                    .setEndOffset(UNDEFINED_EPOCH_OFFSET)));
            responseData.topics().add(topicData);
        });

        return new OffsetsForLeaderEpochResponse(responseData);
    }

    /**
     * Check whether a broker allows Topic-level permissions in order to use the
     * OffsetForLeaderEpoch API. Old versions require Cluster permission.
     */
    public static boolean supportsTopicPermission(short latestUsableVersion) {
        return latestUsableVersion >= 3;
    }
}
