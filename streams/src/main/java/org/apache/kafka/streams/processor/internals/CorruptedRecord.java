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

import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * This class represents a version of a {@link StampedRecord} that failed to deserialize. We need
 * a special record type so that {@link StreamTask} can update consumed offsets. See KAFKA-6502
 * for more details.
 */
public class CorruptedRecord extends StampedRecord {

    CorruptedRecord(final ConsumerRecord<byte[], byte[]> rawRecord) {
        super(rawRecord, ConsumerRecord.NO_TIMESTAMP);
    }

    @Override
    public String toString() {
        return "CorruptedRecord(" +
            "value = " + value +
            ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final CorruptedRecord that = (CorruptedRecord) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
