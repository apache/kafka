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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;

public final class RemoteLogSegmentInfo {

    public final long baseOffset;
    public final long lastOffset;
    public final TopicPartition topicPartition;
    public final int leaderEpoch;
    public final Map<String, ?> props;

    /**
     * This class represents information about a remote log segment.
     *
     * @param baseOffset     baseOffset of this segment
     * @param lastOffset     last offset of this segment
     * @param topicPartition topic partition of this segment
     * @param leaderEpoch    leader epoch from which this segment is copied
     * @param props          any custom props to be stored by RemoteStorageManager, which can be used later when it
     *                       is passed through different methods in RemoteStorageManager.
     */
    public RemoteLogSegmentInfo(long baseOffset, long lastOffset, TopicPartition topicPartition, int leaderEpoch,
                                java.util.Map<String, ?> props) {
        this.baseOffset = baseOffset;
        this.lastOffset = lastOffset;
        this.topicPartition = topicPartition;
        this.leaderEpoch = leaderEpoch;
        this.props = props;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public Map<String, ?> getProps() {
        return props;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteLogSegmentInfo that = (RemoteLogSegmentInfo) o;
        return baseOffset == that.baseOffset &&
                lastOffset == that.lastOffset &&
                leaderEpoch == that.leaderEpoch &&
                Objects.equals(topicPartition, that.topicPartition) &&
                Objects.equals(props, that.props);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseOffset, lastOffset, topicPartition, leaderEpoch, props);
    }
}
