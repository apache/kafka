/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.AbstractIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractRecords implements Records {

    @Override
    public boolean hasMatchingShallowMagic(byte magic) {
        Iterator<? extends LogEntry> iterator = shallowIterator();
        while (iterator.hasNext())
            if (iterator.next().magic() != magic)
                return false;
        return true;
    }

    /**
     * Convert this message set to use the specified message format.
     */
    @Override
    public Records toMessageFormat(byte toMagic) {
        List<LogEntry> converted = new ArrayList<>();
        Iterator<LogEntry> deepIterator = deepIterator();
        while (deepIterator.hasNext()) {
            LogEntry entry = deepIterator.next();
            converted.add(LogEntry.create(entry.offset(), entry.record().convert(toMagic)));
        }

        if (converted.isEmpty()) {
            // This indicates that the message is too large, which indicates that the buffer is not large
            // enough to hold a full log entry. We just return all the bytes in the file message set.
            // Even though the message set does not have the right format version, we expect old clients
            // to raise an error to the user after reading the message size and seeing that there
            // are not enough available bytes in the response to read the full message.
            return this;
        } else {
            // We use the first message to determine the compression type for the resulting message set.
            // This could result in message sets which are either larger or smaller than the original size.
            // For example, it could end up larger if most messages were previously compressed, but
            // it just so happens that the first one is not. There is also some risk that this can
            // cause some timestamp information to be lost (e.g. if the timestamp type was changed) since
            // we are essentially merging multiple message sets. However, currently this method is only
            // used for down-conversion, so we've ignored the problem.
            CompressionType compressionType = shallowIterator().next().record().compressionType();
            return MemoryRecords.withLogEntries(compressionType, converted);
        }
    }

    public static int estimatedSize(CompressionType compressionType, Iterable<LogEntry> entries) {
        int size = 0;
        for (LogEntry entry : entries)
            size += entry.sizeInBytes();
        // NOTE: 1024 is the minimum block size for snappy encoding
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    /**
     * Get an iterator over the deep records.
     * @return An iterator over the records
     */
    public Iterator<Record> records() {
        return new AbstractIterator<Record>() {
            private final Iterator<? extends LogEntry> deepEntries = deepIterator();
            @Override
            protected Record makeNext() {
                if (deepEntries.hasNext())
                    return deepEntries.next().record();
                return allDone();
            }
        };
    }

}
