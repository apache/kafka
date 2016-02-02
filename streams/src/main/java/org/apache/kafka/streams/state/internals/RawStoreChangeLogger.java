/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStoreUtils;

import java.util.Comparator;
import java.util.TreeSet;

public class RawStoreChangeLogger extends StoreChangeLogger<byte[], byte[]> {

    private class ByteArrayComparator implements Comparator<byte[]> {
        @Override
        public int compare(byte[] left, byte[] right) {
            for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
                int a = left[i] & 0xff;
                int b = right[j] & 0xff;

                if (a != b)
                    return a - b;
            }
            return left.length - right.length;
        }
    }

    public RawStoreChangeLogger(String topic, ProcessorContext context) {
        this(topic, context, DEFAULT_WRITE_BATCH_SIZE, DEFAULT_WRITE_BATCH_SIZE);
    }

    public RawStoreChangeLogger(String topic, ProcessorContext context, int maxDirty, int maxRemoved) {
        super(topic, context, context.id().partition, WindowStoreUtils.INNER_SERDES, maxDirty, maxRemoved);
        init();
    }

    @Override
    public void init() {
        this.dirty = new TreeSet<>(new ByteArrayComparator());
        this.removed = new TreeSet<>(new ByteArrayComparator());
    }
}
