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

/**
 * This class tracks resource usage during broker record validation for eventual reporting in metrics.
 * Record validation covers integrity checks on inbound data (e.g. checksum verification), structural
 * validation to make sure that records are well-formed, and conversion between record formats if needed.
 */
public class RecordValidationStats {

    public static final RecordValidationStats EMPTY = new RecordValidationStats();

    private long temporaryMemoryBytes;
    private int numRecordsConverted;
    private long conversionTimeNanos;

    public RecordValidationStats(long temporaryMemoryBytes, int numRecordsConverted, long conversionTimeNanos) {
        this.temporaryMemoryBytes = temporaryMemoryBytes;
        this.numRecordsConverted = numRecordsConverted;
        this.conversionTimeNanos = conversionTimeNanos;
    }

    public RecordValidationStats() {
        this(0, 0, 0);
    }

    public void add(RecordValidationStats stats) {
        temporaryMemoryBytes += stats.temporaryMemoryBytes;
        numRecordsConverted += stats.numRecordsConverted;
        conversionTimeNanos += stats.conversionTimeNanos;
    }

    /**
     * Returns the number of temporary memory bytes allocated to process the records.
     * This size depends on whether the records need decompression and/or conversion:
     * <ul>
     *   <li>Non compressed, no conversion: zero</li>
     *   <li>Non compressed, with conversion: size of the converted buffer</li>
     *   <li>Compressed, no conversion: size of the original buffer after decompression</li>
     *   <li>Compressed, with conversion: size of the original buffer after decompression + size of the converted buffer uncompressed</li>
     * </ul>
     */
    public long temporaryMemoryBytes() {
        return temporaryMemoryBytes;
    }

    public int numRecordsConverted() {
        return numRecordsConverted;
    }

    public long conversionTimeNanos() {
        return conversionTimeNanos;
    }

    @Override
    public String toString() {
        return String.format("RecordValidationStats(temporaryMemoryBytes=%d, numRecordsConverted=%d, conversionTimeNanos=%d)",
                temporaryMemoryBytes, numRecordsConverted, conversionTimeNanos);
    }
}
