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

import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * Records implementation which enables lazy down-conversion to avoid unnecessary memory utilization before
 * a fetch response is ready to be sent.
 */
public class LazyDownConvertingRecords implements Records {
    private final Records records;
    private final long firstOffset;
    private final byte toMagic;
    private final Time time;
    private ConvertedRecords<? extends Records> convertedRecords = null;

    public LazyDownConvertingRecords(Records records,
                                     byte toMagic,
                                     long firstOffset,
                                     Time time) {
        this.records = records;
        this.toMagic = toMagic;
        this.firstOffset = firstOffset;
        this.time = time;
    }

    @Override
    public int sizeInBytes() {
        return convertIfNeeded().records().sizeInBytes();
    }

    @Override
    public RecordsSend toSend(String destination) {
        ConvertedRecords<? extends Records> convertedRecords = convertIfNeeded();
        return new RecordsSend(destination, convertedRecords.records(), convertedRecords.recordsProcessingStats());
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) throws IOException {
        return convertIfNeeded().records().writeTo(channel, position, length);
    }

    @Override
    public Iterable<? extends RecordBatch> batches() {
        return convertIfNeeded().records().batches();
    }

    @Override
    public boolean hasMatchingMagic(byte magic) {
        return convertIfNeeded().records().hasMatchingMagic(magic);
    }

    @Override
    public boolean hasCompatibleMagic(byte magic) {
        return toMagic <= magic;
    }

    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        return records.downConvert(toMagic, firstOffset, time);
    }

    @Override
    public Iterable<Record> records() {
        return convertIfNeeded().records().records();
    }

    private ConvertedRecords<? extends Records> convertIfNeeded() {
        if (convertedRecords == null)
            convertedRecords = records.downConvert(toMagic, firstOffset, time);
        return convertedRecords;
    }

}
