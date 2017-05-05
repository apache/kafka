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

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.PrintForeachAction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class KStreamPeek<K, V> implements ProcessorSupplier<K, V> {

    private final boolean print;
    private final boolean downStream;
    private ForeachAction<K, V> action;
    private PrintForeachAction<K, V> printAction;

    public KStreamPeek(final ForeachAction<K, V> action, final boolean downStream) {
        this.action = action;
        this.downStream = downStream;
        this.print = false;
    }

    public KStreamPeek(final PrintForeachAction<K, V> printAction, final boolean downStream) {
        this.printAction = printAction;
        this.downStream = downStream;
        this.print = true;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamPeekProcessor();
    }

    private class KStreamPeekProcessor extends AbstractProcessor<K, V> {
        
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            if (print) {
                printAction.setContext(context);
                if (printAction.keySerde() == null) {
                    printAction.useDefaultKeySerde();
                }
                if (printAction.valueSerde() == null) {
                    printAction.useDefaultValueSerde();
                }
            }
        }

        @Override
        public void process(final K key, final V value) {
            if (print) {
                printAction.apply(key, value);
            } else {
                action.apply(key, value);
            }
            if (downStream) {
                context().forward(key, value);
            }
        }

        @Override
        public void close() {
            if (print) {
                printAction.close();
            }
        }
    }

}
