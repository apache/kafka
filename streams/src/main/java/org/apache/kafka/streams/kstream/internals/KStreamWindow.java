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

import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.WindowSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KStreamWindow<K, V> implements ProcessorSupplier<K, V> {

    private final WindowSupplier<K, V> windowSupplier;

    KStreamWindow(WindowSupplier<K, V> windowSupplier) {
        this.windowSupplier = windowSupplier;
    }

    public WindowSupplier<K, V> window() {
        return windowSupplier;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamWindowProcessor();
    }

    private class KStreamWindowProcessor extends AbstractProcessor<K, V> {

        private Window<K, V> window;

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            this.window = windowSupplier.get();
            this.window.init(context);
        }

        @Override
        public void process(K key, V value) {
            synchronized (this) {
                window.put(key, value, context().timestamp());
                context().forward(key, value);
            }
        }

        @Override
        public void close() {
            window.close();
        }
    }
}
