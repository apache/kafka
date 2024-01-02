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
import org.apache.kafka.server.util.Scheduler;
import org.slf4j.Logger;

import java.io.File;
import java.util.List;

/**
 * Holds the result of splitting a segment into one or more segments, see {@link LocalLog#splitOverflowedSegment(LogSegment, LogSegments, File, TopicPartition, LogConfig, Scheduler, LogDirFailureChannel, Logger)}.
 */
public class SplitSegmentResult {
    public final List<LogSegment> deletedSegments;
    public final List<LogSegment> newSegments;

    /**
     * Creates a new SplitSegmentResult instance.
     *
     * @param deletedSegments segments deleted when splitting a segment
     * @param newSegments new segments created when splitting a segment
     */
    SplitSegmentResult(List<LogSegment> deletedSegments, List<LogSegment> newSegments) {
        this.deletedSegments = deletedSegments;
        this.newSegments = newSegments;
    }

}
