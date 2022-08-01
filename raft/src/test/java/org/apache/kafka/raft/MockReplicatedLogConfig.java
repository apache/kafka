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
package org.apache.kafka.raft;

public class MockReplicatedLogConfig {

    private final int logSegmentBytes;
    private final int logSegmentMinBytes;
    private final int logSegmentMillis;
    private final int retentionMaxBytes;
    private final int retentionMillis;
    private final int maxBatchSizeInBytes;
    private final int maxFetchSizeInBytes;
    private final int fileDeleteDelayMs;
    private final int nodeId;

    public MockReplicatedLogConfig(int logSegmentBytes, int logSegmentMinBytes, int logSegmentMillis,
                                   int retentionMaxBytes, int retentionMillis,
                                   int maxBatchSizeInBytes, int maxFetchSizeInBytes,
                                   int fileDeleteDelayMs, int nodeId) {
        this.logSegmentBytes = logSegmentBytes;
        this.logSegmentMinBytes = logSegmentMinBytes;
        this.logSegmentMillis = logSegmentMillis;
        this.retentionMaxBytes = retentionMaxBytes;
        this.retentionMillis = retentionMillis;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxFetchSizeInBytes = maxFetchSizeInBytes;
        this.fileDeleteDelayMs = fileDeleteDelayMs;
        this.nodeId = nodeId;
    }

    public int getLogSegmentBytes() {
        return logSegmentBytes;
    }

    public int getLogSegmentMinBytes() {
        return logSegmentMinBytes;
    }

    public int getLogSegmentMillis() {
        return logSegmentMillis;
    }

    public int getRetentionMaxBytes() {
        return retentionMaxBytes;
    }

    public int getRetentionMillis() {
        return retentionMillis;
    }

    public int getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public int getMaxFetchSizeInBytes() {
        return maxFetchSizeInBytes;
    }

    public int getFileDeleteDelayMs() {
        return fileDeleteDelayMs;
    }

    public int getNodeId() {
        return nodeId;
    }
}
