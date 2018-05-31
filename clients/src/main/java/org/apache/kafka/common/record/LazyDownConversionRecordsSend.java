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

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

/**
 * Encapsulation for {@link RecordsSend} for {@link LazyDownConversionRecords}. Records are down-converted in batches and
 * on-demand when {@link #writeTo} method is called.
 */
public final class LazyDownConversionRecordsSend extends RecordsSend<LazyDownConversionRecords> {
    private static final Logger log = LoggerFactory.getLogger(LazyDownConversionRecordsSend.class);
    private static final int MAX_READ_SIZE = 128 * 1024;

    private RecordConversionStats recordConversionStats;
    private RecordsSend convertedRecordsWriter;
    private Iterator<ConvertedRecords> convertedRecordsIterator;

    public LazyDownConversionRecordsSend(String destination, LazyDownConversionRecords records) {
        super(destination, records, records.sizeInBytes());
        convertedRecordsWriter = null;
        recordConversionStats = new RecordConversionStats();
        convertedRecordsIterator = records().iterator(MAX_READ_SIZE);
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long previouslyWritten, int remaining) throws IOException {
        if (convertedRecordsWriter == null || convertedRecordsWriter.completed()) {
            MemoryRecords convertedRecords;

            // Check if we have more chunks left to down-convert
            if (convertedRecordsIterator.hasNext()) {
                // Get next chunk of down-converted messages
                ConvertedRecords<MemoryRecords> recordsAndStats = convertedRecordsIterator.next();
                convertedRecords = recordsAndStats.records();

                int sizeOfFirstConvertedBatch = convertedRecords.batchIterator().next().sizeInBytes();
                if (previouslyWritten == 0 && sizeOfFirstConvertedBatch > size())
                    throw new EOFException("Unable to send first batch completely." +
                            " maximum_size: " + size() +
                            " converted_records_size: " + sizeOfFirstConvertedBatch);

                recordConversionStats.add(recordsAndStats.recordConversionStats());
                log.debug("Got lazy converted records for {" + topicPartition() + "} with length=" + convertedRecords.sizeInBytes());
            } else {
                if (previouslyWritten == 0)
                    throw new EOFException("Unable to get the first batch of down-converted records");

                // We do not have any records left to down-convert. Construct a "fake" message for the length remaining.
                // This message will be ignored by the consumer because its length will be past the length of maximum
                // possible response size.
                // DefaultRecordBatch =>
                //      BaseOffset => Int64
                //      Length => Int32
                //      ...
                // TODO: check if there is a better way to encapsulate this logic, perhaps in DefaultRecordBatch
                log.debug("Constructing fake message batch for topic-partition {" + topicPartition() + "} for remaining length " + remaining);
                int minLength = (Long.SIZE / Byte.SIZE) + (Integer.SIZE / Byte.SIZE);
                ByteBuffer fakeMessageBatch = ByteBuffer.allocate(Math.max(minLength, Math.min(remaining + 1, MAX_READ_SIZE)));
                fakeMessageBatch.putLong(-1L);
                fakeMessageBatch.putInt(remaining + 1);
                convertedRecords = MemoryRecords.readableRecords(fakeMessageBatch);
            }

            convertedRecordsWriter = new DefaultRecordsSend(destination(), convertedRecords, Math.min(convertedRecords.sizeInBytes(), remaining));
        }
        return convertedRecordsWriter.writeTo(channel);
    }

    public RecordConversionStats recordConversionStats() {
        return recordConversionStats;
    }

    public TopicPartition topicPartition() {
        return records().topicPartition();
    }
}
