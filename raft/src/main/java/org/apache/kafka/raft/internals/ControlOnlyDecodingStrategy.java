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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.raft.Batch;

import java.io.InputStream;

/**
 * Decodes only the control records of a batch and skips the data records.
 */
public final class ControlOnlyDecodingStrategy<T> implements DecodingStrategy<T> {

    @Override
    public Batch<T> readBatch(DefaultRecordBatch batch, InputStream input, BufferSupplier bufferSupplier, int numRecords) {
        if (batch.isControlBatch()) {
            return DecodingStrategy.readControlBatch(batch, input, bufferSupplier, numRecords);
        } else {
            return DecodingStrategy.notDecodedBatch(batch, numRecords);
        }
    }
}
