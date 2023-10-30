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

import java.util.List;
import java.util.NoSuchElementException;
import org.apache.kafka.streams.state.ValueIterator;
import org.apache.kafka.streams.state.VersionedRecord;

public class VersionedRecordIterator<V> implements ValueIterator<VersionedRecord<V>> {

    protected final List<VersionedRecord<V>> records;
    protected int currentIndex;

    public VersionedRecordIterator(final List<VersionedRecord<V>> records) {
        this.records = records;
        this.currentIndex = 0;
    }

    @Override
    public void close() {

    }

    @Override
    public VersionedRecord<V> peek() {
        return next();
    }

    @Override
    public boolean hasNext() {
        return currentIndex < records.size();
    }

    @Override
    public VersionedRecord<V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return records.get(currentIndex++);
    }
}
