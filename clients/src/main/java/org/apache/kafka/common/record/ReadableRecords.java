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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;

import java.util.Iterator;

public interface ReadableRecords extends BaseRecords {
    /**
     * Get the record batches. Note that the signature allows subclasses
     * to return a more specific batch type. This enables optimizations such as in-place offset
     * assignment (see for example {@link DefaultRecordBatch}), and partial reading of
     * record data (see {@link FileLogInputStream.FileChannelRecordBatch#magic()}.
     * @return An iterator over the record batches of the log
     */
    Iterable<? extends RecordBatch> batches();

    /**
     * Get an iterator over the record batches. This is similar to {@link #batches()} but returns an {@link AbstractIterator}
     * instead of {@link Iterator}, so that clients can use methods like {@link AbstractIterator#peek() peek}.
     * @return An iterator over the record batches of the log
     */
    AbstractIterator<? extends RecordBatch> batchIterator();

    /**
     * Check whether all batches in this buffer have a certain magic value.
     * @param magic The magic value to check
     * @return true if all record batches have a matching magic value, false otherwise
     */
    boolean hasMatchingMagic(byte magic);

    /**
     * Check whether this log buffer has a magic value compatible with a particular value
     * (i.e. whether all message sets contained in the buffer have a matching or lower magic).
     * @param magic The magic version to ensure compatibility with
     * @return true if all batches have compatible magic, false otherwise
     */
    boolean hasCompatibleMagic(byte magic);

    /**
     * Convert all batches in this buffer to the format passed as a parameter. Note that this requires
     * deep iteration since all of the deep records must also be converted to the desired format.
     * @param toMagic The magic value to convert to
     * @param firstOffset The starting offset for returned records. This only impacts some cases. See
     *                    {@link RecordsUtil#downConvert(Iterable, byte, long, Time)} for an explanation.
     * @param time instance used for reporting stats
     * @return A ConvertedRecords instance which may or may not contain the same instance in its records field.
     */
    ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time);

    /**
     * Get an iterator over the records in this log. Note that this generally requires decompression,
     * and should therefore be used with care.
     * @return The record iterator
     */
    Iterable<Record> records();
}
