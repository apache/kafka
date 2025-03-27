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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;

/**
 * Helper class for a log, its topic/partition, the first cleanable position, the first uncleanable dirty position,
 * and whether it needs compaction immediately.
 */
public final class LogToClean implements Comparable<LogToClean> {
    private final TopicPartition topicPartition;
    private final UnifiedLog log;
    private final long firstDirtyOffset;
    private final boolean needCompactionNow;
    private final long cleanBytes;
    private final long firstUncleanableOffset;
    private final long cleanableBytes;
    private final long totalBytes;
    private final double cleanableRatio;

    public LogToClean(UnifiedLog log, long firstDirtyOffset, long uncleanableOffset, boolean needCompactionNow) {
        this.log = log;
        this.topicPartition = log.topicPartition();
        this.firstDirtyOffset = firstDirtyOffset;
        this.needCompactionNow = needCompactionNow;

        this.cleanBytes = log.logSegments(-1, firstDirtyOffset).stream()
                .mapToLong(LogSegment::size)
                .sum();

        Map.Entry<Long, Long> cleanableBytesResult = LogCleanerManager.calculateCleanableBytes(log, firstDirtyOffset, uncleanableOffset);
        this.firstUncleanableOffset = cleanableBytesResult.getKey();
        this.cleanableBytes = cleanableBytesResult.getValue();

        this.totalBytes = cleanBytes + cleanableBytes;
        this.cleanableRatio = (double) cleanableBytes / totalBytes;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public UnifiedLog log() {
        return log;
    }

    public long firstDirtyOffset() {
        return firstDirtyOffset;
    }

    boolean needCompactionNow() {
        return needCompactionNow;
    }

    public long cleanBytes() {
        return cleanBytes;
    }

    public long firstUncleanableOffset() {
        return firstUncleanableOffset;
    }

    public long cleanableBytes() {
        return cleanableBytes;
    }

    public long totalBytes() {
        return totalBytes;
    }

    public double cleanableRatio() {
        return cleanableRatio;
    }

    @Override
    public int compareTo(LogToClean that) {
        return Double.compare(this.cleanableRatio, that.cleanableRatio);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogToClean that = (LogToClean) o;
        return firstDirtyOffset == that.firstDirtyOffset &&
                needCompactionNow == that.needCompactionNow &&
                cleanBytes == that.cleanBytes &&
                firstUncleanableOffset == that.firstUncleanableOffset &&
                cleanableBytes == that.cleanableBytes &&
                totalBytes == that.totalBytes &&
                Double.compare(that.cleanableRatio, cleanableRatio) == 0 &&
                topicPartition.equals(that.topicPartition) &&
                log.equals(that.log);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                topicPartition, log, firstDirtyOffset, needCompactionNow, cleanBytes,
                firstUncleanableOffset, cleanableBytes, totalBytes, cleanableRatio
        );
    }

    @Override
    public String toString() {
        return "LogToClean{" +
                "topicPartition=" + topicPartition +
                ", log=" + log +
                ", firstDirtyOffset=" + firstDirtyOffset +
                ", needCompactionNow=" + needCompactionNow +
                ", cleanBytes=" + cleanBytes +
                ", firstUncleanableOffset=" + firstUncleanableOffset +
                ", cleanableBytes=" + cleanableBytes +
                ", totalBytes=" + totalBytes +
                ", cleanableRatio=" + cleanableRatio +
                '}';
    }
}
