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

package org.apache.kafka.metadata.migration;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A record batch consumer that merges incoming batches into batches of a minimum a given size. It does so
 * by buffering the records into an array that is later flushed to a downstream consumer. Batches consumed
 * by this class will not be broken apart, only combined with other batches to reach the minimum batch size.
 * </p>
 * Note that {@link #flush()} must be called after the last batch has been accepted in order to flush any
 * buffered records.
 */
public class BufferingBatchConsumer<T> implements Consumer<List<T>> {

    private final Consumer<List<T>> delegateConsumer;
    private final int minBatchSize;
    private List<T> bufferedBatch;

    BufferingBatchConsumer(Consumer<List<T>> delegateConsumer, int minBatchSize) {
        this.delegateConsumer = delegateConsumer;
        this.minBatchSize = minBatchSize;
        this.bufferedBatch = new ArrayList<>(minBatchSize);
    }

    @Override
    public void accept(List<T> batch) {
        bufferedBatch.addAll(batch);
        if (bufferedBatch.size() >= minBatchSize) {
            flush();
        }
    }

    public void flush() {
        if (!bufferedBatch.isEmpty()) {
            delegateConsumer.accept(bufferedBatch);
            bufferedBatch = new ArrayList<>(minBatchSize);
        }
    }
}
