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

import org.apache.kafka.common.KafkaException;

/**
 * Indicates that the log segment contains one or more messages that overflow the offset (and / or time) index. This is
 * not a typical scenario, and could only happen when brokers have log segments that were created before the patch for
 * KAFKA-5413. With KAFKA-6264, we have the ability to split such log segments into multiple log segments such that we
 * do not have any segments with offset overflow.
 */
public class LogSegmentOffsetOverflowException extends KafkaException {
    public final LogSegment segment;
    public final long offset;

    public LogSegmentOffsetOverflowException(LogSegment segment, long offset) {
        super("Detected offset overflow at offset " + offset + " in segment " + segment);
        this.segment = segment;
        this.offset = offset;
    }
}
