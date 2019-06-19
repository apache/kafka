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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * In the even of unclean leader election, the log will be truncated,
 * previously committed data will be lost, and new data will be written
 * over these offsets. When this happens, the consumer will detect the
 * truncation and raise this exception (if no automatic reset policy
 * has been defined) with the first offset to diverge from what the
 * consumer read.
 */
public class LogTruncationException extends OffsetOutOfRangeException {

    private final Map<TopicPartition, OffsetAndMetadata> divergentOffsets;

    public LogTruncationException(Map<TopicPartition, OffsetAndMetadata> divergentOffsets) {
        super(Utils.transformMap(divergentOffsets, Function.identity(), OffsetAndMetadata::offset));
        this.divergentOffsets = Collections.unmodifiableMap(divergentOffsets);
    }

    /**
     * Get the offsets for the partitions which were truncated. This is the first offset which is known to diverge
     * from what the consumer read.
     */
    public Map<TopicPartition, OffsetAndMetadata> divergentOffsets() {
        return divergentOffsets;
    }
}
