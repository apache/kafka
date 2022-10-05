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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

public class TransactionalKeyValueStore extends AbstractTransactionalStore<KeyValueStore<Bytes, byte[]>> {
    private final RocksDBMetricsRecorder metricsRecorder;

    private final KeyValueSegment tmpStore;
    private final KeyValueStore<Bytes, byte[]> mainStore;

    //VisibleForTesting
    TransactionalKeyValueStore(final KeyValueStore<Bytes, byte[]> mainStore, final RocksDBMetricsRecorder metricsRecorder) {
        this.metricsRecorder = metricsRecorder;
        this.mainStore = mainStore;
        this.tmpStore = createTmpStore(mainStore.name() + TMP_SUFFIX,
                                       mainStore.name(),
                                       0,
                                       metricsRecorder);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        metricsRecorder.init(ProcessorContextUtils.getMetricsImpl(context), context.taskId());
        super.init(context, root);
    }

    @Override
    public KeyValueStore<Bytes, byte[]> mainStore() {
        return mainStore;
    }

    @Override
    public KeyValueSegment tmpStore() {
        return tmpStore;
    }

    @Override
    public String name() {
        return mainStore.name();
    }
}
