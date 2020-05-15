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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class StateRestoreCallbackAdapter {
    private StateRestoreCallbackAdapter() {}

    public static RecordBatchingStateRestoreCallback adapt(final StateRestoreCallback restoreCallback) {
        Objects.requireNonNull(restoreCallback, "stateRestoreCallback must not be null");
        if (restoreCallback instanceof RecordBatchingStateRestoreCallback) {
            return (RecordBatchingStateRestoreCallback) restoreCallback;
        } else if (restoreCallback instanceof BatchingStateRestoreCallback) {
            return records -> {
                final List<KeyValue<byte[], byte[]>> keyValues = new ArrayList<>();
                for (final ConsumerRecord<byte[], byte[]> record : records) {
                    keyValues.add(new KeyValue<>(record.key(), record.value()));
                }
                ((BatchingStateRestoreCallback) restoreCallback).restoreAll(keyValues);
            };
        } else {
            return records -> {
                for (final ConsumerRecord<byte[], byte[]> record : records) {
                    restoreCallback.restore(record.key(), record.value());
                }
            };
        }
    }
}