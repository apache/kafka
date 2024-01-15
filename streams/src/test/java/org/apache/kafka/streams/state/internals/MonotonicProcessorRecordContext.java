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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

public class MonotonicProcessorRecordContext extends ProcessorRecordContext {
    private long counter;
    private final boolean automatic;

    public MonotonicProcessorRecordContext(final String topic, final int partition) {
        this(topic, partition, true);
    }

    public MonotonicProcessorRecordContext(final String topic, final int partition, final boolean automatic) {
        super(0, 0, partition, topic, new RecordHeaders());
        this.counter = 0;
        this.automatic = automatic;
    }

    @Override
    public long offset() {
        final long ret = counter;
        if (automatic) {
            counter++;
        }
        return ret;
    }

    public void kick() {
        if (!automatic) {
            counter++;
        }
    }
}
