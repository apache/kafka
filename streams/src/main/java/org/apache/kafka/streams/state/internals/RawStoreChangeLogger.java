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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.TreeSet;

public class RawStoreChangeLogger extends StoreChangeLogger<byte[], byte[]> {

    public static final StateSerdes<byte[], byte[]> RAW_SERDES = new StateSerdes<>("", Serdes.ByteArray(), Serdes.ByteArray());

    public RawStoreChangeLogger(String storeName, ProcessorContext context) {
        this(storeName, context, DEFAULT_WRITE_BATCH_SIZE, DEFAULT_WRITE_BATCH_SIZE);
    }

    public RawStoreChangeLogger(String storeName, ProcessorContext context, int maxDirty, int maxRemoved) {
        super(storeName, context, context.taskId().partition, RAW_SERDES, maxDirty, maxRemoved);
        init();
    }

    @Override
    public void init() {
        this.dirty = new TreeSet<>(Bytes.BYTES_LEXICO_COMPARATOR);
        this.removed = new TreeSet<>(Bytes.BYTES_LEXICO_COMPARATOR);
    }
}
