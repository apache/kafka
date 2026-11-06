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
package org.apache.kafka.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class WallclockTimestampExtractorTest {

    @Test
    public void extractSystemTimestamp() {
        final TimestampExtractor extractor = new WallclockTimestampExtractor();

        final long before = System.currentTimeMillis();
        final long recordTimestamp = 41;
        final long partitionTime = 42;
        // The extractor should ignore the input timestamps and return the current wall-clock time.
        final long timestamp = extractor.extract(
            new ConsumerRecord<>(
                "anyTopic",
                0,
                0,
                recordTimestamp,
                TimestampType.CREATE_TIME,
                0,
                0,
                null,
                null,
                new RecordHeaders(),
                Optional.empty()),
            partitionTime);
        final long after = System.currentTimeMillis();

        assertTrue(before <= timestamp);
        assertTrue(timestamp <= after);
    }

}
