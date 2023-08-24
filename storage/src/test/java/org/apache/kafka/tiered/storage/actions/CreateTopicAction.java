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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.tiered.storage.specs.TopicSpec;
import org.apache.kafka.common.config.TopicConfig;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public final class CreateTopicAction implements TieredStorageTestAction {

    private final TopicSpec spec;

    public CreateTopicAction(TopicSpec spec) {
        this.spec = spec;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws ExecutionException, InterruptedException {
        // Ensure offset and time indexes are generated for every record.
        spec.getProperties().put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "1");
        // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
        // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
        // time index. Hence, since the topic is configured to generate index entries for every record with, for
        // a "small" number of records (i.e. such that the average record size times the number of records is
        // much less than the segment size), the number of records which hold in a segment is the multiple of 12
        // defined below.
        if (spec.getMaxBatchCountPerSegment() != -1) {
            spec.getProperties().put(
                    TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, String.valueOf(12 * spec.getMaxBatchCountPerSegment()));
        }
        // To verify records physically absent from Kafka's storage can be consumed via the second tier storage, we
        // want to delete log segments as soon as possible. When tiered storage is active, an inactive log
        // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
        // should be offloaded before deletion, and their consumption is possible thereafter.
        spec.getProperties().put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "1");
        context.createTopic(spec);
    }

    @Override
    public void describe(PrintStream output) {
        output.println("create topic: " + spec);
    }
}
