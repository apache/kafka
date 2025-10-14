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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;


public class InitProcessorRecordContext extends ProcessorRecordContext {

    private final long initTime;
    private static final long NO_OFFSET = -1;
    private static final int NO_PARTITION = -1;

    public InitProcessorRecordContext(final long currentTimestamp) {
        super(ConsumerRecord.NO_TIMESTAMP, NO_OFFSET, NO_PARTITION, null, new RecordHeaders());
        this.initTime = currentTimestamp;
    }

    @Override
    public long timestamp() {
        return initTime;
    }

    @Override
    @Deprecated
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    @Override
    @Deprecated
    public int hashCode() {
        return super.hashCode();
    }

}
