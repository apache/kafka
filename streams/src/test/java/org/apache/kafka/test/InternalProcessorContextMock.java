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
package org.apache.kafka.test;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;

import java.util.List;

public interface InternalProcessorContextMock extends InternalProcessorContext, RecordCollector.Supplier {

    void setRecordCollector(final RecordCollector recordCollector);

    StateRestoreCallback stateRestoreCallback(final String storeName);

    List<MockProcessorContext.CapturedForward> forwarded();

    void setTimestamp(final long timestamp);

    void setKeySerde(final Serde<?> keySerde);

    void setValueSerde(final Serde<?> valSerde);

    StateRestoreListener getRestoreListener(final String storeName);

    void restore(final String storeName, final Iterable<KeyValue<byte[], byte[]>> changeLog);

}
