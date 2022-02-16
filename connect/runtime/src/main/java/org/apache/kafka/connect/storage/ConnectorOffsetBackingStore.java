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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

public class ConnectorOffsetBackingStore implements OffsetBackingStore {

    private final OffsetBackingStore workerStore;
    private final String primaryOffsetsTopic;

    public ConnectorOffsetBackingStore(
            OffsetBackingStore workerStore,
            String primaryOffsetsTopic
    ) {
        this.workerStore = workerStore;
        this.primaryOffsetsTopic = primaryOffsetsTopic;
    }

    public String primaryOffsetsTopic() {
        return primaryOffsetsTopic;
    }

    @Override
    public void start() {
        // TODO
    }

    @Override
    public void stop() {
        // TODO
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        // TODO
        return workerStore.get(keys);
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        // TODO
        return workerStore.set(values, callback);
    }

    @Override
    public void configure(WorkerConfig config) {
        // TODO
    }

}
