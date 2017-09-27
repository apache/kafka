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
package org.apache.kafka.common.record;

public class RecordsProcessingStats {

    public static final RecordsProcessingStats EMPTY = new RecordsProcessingStats(0L, 0L);

    private final long temporaryMemoryBytes;
    private final long conversionCount;
    private long conversionTimeNanos;

    public RecordsProcessingStats(long temporaryMemoryBytes, long conversionCount) {
        this(temporaryMemoryBytes, conversionCount, -1);
    }

    public RecordsProcessingStats(long temporaryMemoryBytes, long conversionCount, long conversionTimeNanos) {
        this.temporaryMemoryBytes = temporaryMemoryBytes;
        this.conversionCount = conversionCount;
        this.conversionTimeNanos = conversionTimeNanos;
    }

    /**
     * Return uncompressed memory.
     */
    public long temporaryMemoryBytes() {
        return temporaryMemoryBytes;
    }

    public long conversionCount() {
        return conversionCount;
    }

    public long conversionTimeNanos() {
        return conversionTimeNanos;
    }

    public void conversionTimeNanos(long nanos) {
        this.conversionTimeNanos = nanos;
    }

    @Override
    public String toString() {
        return String.format("RecordsProcessingStats(temporaryMemoryBytes=%d, conversionCount=%d, conversionTimeNanos=%d)",
                temporaryMemoryBytes, conversionCount, conversionTimeNanos);
    }
}
