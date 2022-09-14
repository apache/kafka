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

import static org.easymock.EasyMock.mock;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

public class TransactionalKeyValueStoreTest extends AbstractTransactionalStoreTest<KeyValueStore<Bytes, byte[]>> {

    @Override
    TransactionalKeyValueStore getTxnStore() {
        final TransactionalKeyValueStore txnStore = new TransactionalKeyValueStore(
            Stores.inMemoryKeyValueStore("main").get(),
            (RocksDBMetricsRecorder) mock(RocksDBMetricsRecorder.class)
        );
        txnStore.init((StateStoreContext) context, txnStore);
        return txnStore;
    }
}