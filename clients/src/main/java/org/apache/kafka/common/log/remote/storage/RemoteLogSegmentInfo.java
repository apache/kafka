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
