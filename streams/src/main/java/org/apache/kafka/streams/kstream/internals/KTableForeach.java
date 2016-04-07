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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;

class KTableForeach<K, V> implements KTableProcessorSupplier<K, V, V> {

    private final KTableImpl<K, ?, V> parent;
    private final ForeachAction<K, V> action;
    private boolean sendOldValues = false;

    public KTableForeach(KTableImpl<K, ?, V> parent, ForeachAction<K, V> action) {
        this.parent = parent;
        this.action = action;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableForeachProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return null;
    }

    @Override
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private class KTableForeachProcessor extends AbstractProcessor<K, Change<V>> {
        @Override
        public void process(K key, Change<V> change) {
            action.apply(key, change.newValue);
        }
    }

}

