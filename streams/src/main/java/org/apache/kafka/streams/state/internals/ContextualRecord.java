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

import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.kafka.common.utils.Utils.getNullableSizePrefixedArray;

public class ContextualRecord {
    private final byte[] value;
    private final ProcessorRecordContext recordContext;

    public ContextualRecord(final byte[] value, final ProcessorRecordContext recordContext) {
        this.value = value;
        this.recordContext = Objects.requireNonNull(recordContext);
    }

    public ProcessorRecordContext recordContext() {
        return recordContext;
    }

    public byte[] value() {
        return value;
    }

    long residentMemorySizeEstimate() {
        return (value == null ? 0 : value.length) + recordContext.residentMemorySizeEstimate();
    }

    static ContextualRecord deserialize(final ByteBuffer buffer) {
        final ProcessorRecordContext context = ProcessorRecordContext.deserialize(buffer);
        final byte[] value = getNullableSizePrefixedArray(buffer);
        return new ContextualRecord(value, context);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ContextualRecord that = (ContextualRecord) o;
        return Arrays.equals(value, that.value) &&
            Objects.equals(recordContext, that.recordContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, recordContext);
    }

    @Override
    public String toString() {
        return "ContextualRecord{" +
            "recordContext=" + recordContext +
            ", value=" + Arrays.toString(value) +
            '}';
    }
}
