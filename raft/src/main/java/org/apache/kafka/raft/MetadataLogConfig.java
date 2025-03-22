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

/**
 * Configuration for the metadata log
 * @param logSegmentBytes The maximum size of a single metadata log file
 * @param logSegmentMinBytes The minimum size of a single metadata log file
 * @param logSegmentMillis The maximum time before a new metadata log file is rolled out
 * @param retentionMaxBytes The size of the metadata log and snapshots before deleting old snapshots and log files
 * @param retentionMillis The time to keep a metadata log file or snapshot before deleting it
 * @param maxBatchSizeInBytes The largest record batch size allowed in the metadata log
 * @param maxFetchSizeInBytes The maximum number of bytes to read when fetching from the metadata log
 * @param deleteDelayMillis The amount of time to wait before deleting a file from the filesystem
 * @param nodeId The node id
 */
public record MetadataLogConfig(int logSegmentBytes,
                                int logSegmentMinBytes,
                                long logSegmentMillis,
                                long retentionMaxBytes,
                                long retentionMillis,
                                int maxBatchSizeInBytes,
                                int maxFetchSizeInBytes,
                                long deleteDelayMillis,
                                int nodeId) {
}
