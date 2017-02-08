/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import java.util.Map;

public class Checkpointer {

    private final Time time;
    private final Checkpointable checkpointable;
    private final long checkpointInterval;
    private long lastCheckpointMs;

    public Checkpointer(final Time time,
                        final Checkpointable checkpointable,
                        final long checkpointInterval) {
        this.time = time;
        this.checkpointable = checkpointable;
        this.checkpointInterval = checkpointInterval;
        lastCheckpointMs = time.milliseconds();
    }

    public void checkpoint(final Map<TopicPartition, Long> offsets) {
        if (time.milliseconds() >= lastCheckpointMs + checkpointInterval) {
            checkpointable.checkpoint(offsets);
            lastCheckpointMs = time.milliseconds();
        }
    }
}
