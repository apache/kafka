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

package org.apache.kafka.streaming.kstream.internals;

import org.apache.kafka.streaming.processor.Processor;
import org.apache.kafka.streaming.processor.ProcessorFactory;
import org.apache.kafka.streaming.processor.TopologyBuilder;
import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.kstream.KStream;
import org.apache.kafka.streaming.kstream.KStreamWindowed;
import org.apache.kafka.streaming.kstream.ValueJoiner;
import org.apache.kafka.streaming.kstream.Window;

public class KStreamWindow<K, V> implements ProcessorFactory {

    private final Window<K, V> window;

    KStreamWindow(Window<K, V> window) {
        this.window = window;
    }

    public Window<K, V> window() {
        return window;
    }

    @Override
    public Processor build() {
        return new KStreamWindowProcessor();
    }

    private class KStreamWindowProcessor extends KStreamProcessor<K, V> {

        private Window.WindowInstance<K, V> windowInstance;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.windowInstance = window.build();
            this.windowInstance.init(context);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(K key, V value) {
            synchronized (this) {
                windowInstance.put(key, value, context.timestamp());
                context.forward(key, value);
            }
        }

        @Override
        public void close() {
            windowInstance.close();
        }
    }
}
