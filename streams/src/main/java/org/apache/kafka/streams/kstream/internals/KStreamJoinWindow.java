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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.WindowStore;

class KStreamJoinWindow<K, V> implements ProcessorSupplier<K, V> {

    private final String windowName;

    /**
     * @throws TopologyException if retention period of the join window is less than expected
     */
    KStreamJoinWindow(String windowName, long windowSizeMs, long retentionPeriodMs) {
        this.windowName = windowName;

        if (windowSizeMs > retentionPeriodMs)
            throw new TopologyException("The retention period of the join window "
                    + windowName + " must be no smaller than its window size.");
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamJoinWindowProcessor();
    }

    private class KStreamJoinWindowProcessor extends AbstractProcessor<K, V> {

        private WindowStore<K, V> window;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);

            window = (WindowStore<K, V>) context.getStateStore(windowName);
        }

        @Override
        public void process(K key, V value) {
            // if the key is null, we do not need to put the record into window store
            // since it will never be considered for join operations
            if (key != null) {
                context().forward(key, value);
                window.put(key, value);
            }
        }
    }

}
