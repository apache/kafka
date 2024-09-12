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

import java.util.Objects;
import java.util.OptionalLong;

/**
 * The last written record for a given producer. The last data offset may be undefined
 * if the only log entry for a producer is a transaction marker.
 */
public final class LastRecord {
    public final OptionalLong lastDataOffset;
    public final short producerEpoch;

    public LastRecord(OptionalLong lastDataOffset, short producerEpoch) {
        Objects.requireNonNull(lastDataOffset, "lastDataOffset must be non null");
        this.lastDataOffset = lastDataOffset;
        this.producerEpoch = producerEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LastRecord that = (LastRecord) o;

        return producerEpoch == that.producerEpoch &&
                lastDataOffset.equals(that.lastDataOffset);
    }

    @Override
    public int hashCode() {
        return 31 * lastDataOffset.hashCode() + producerEpoch;
    }

    @Override
    public String toString() {
        return "LastRecord(" +
                "lastDataOffset=" + lastDataOffset +
                ", producerEpoch=" + producerEpoch +
                ')';
    }
}
