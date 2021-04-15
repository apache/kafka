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

import java.util.List;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

class KStreamBranch<K, V> implements ProcessorSupplier<K, V, K, V> {

    private final List<Predicate<? super K, ? super V>> predicates;
    private final List<String> childNodes;

    KStreamBranch(final List<Predicate<? super K, ? super V>> predicates,
        final List<String> childNodes) {
        this.predicates = predicates;
        this.childNodes = childNodes;
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new KStreamBranchProcessor();
    }

    private class KStreamBranchProcessor extends ContextualProcessor<K, V, K, V> {

        @Override
        public void process(final Record<K, V> record) {
            for (int i = 0; i < predicates.size(); i++) {
                if (predicates.get(i).test(record.key(), record.value())) {
                    // use forward with child here and then break the loop
                    // so that no record is going to be piped to multiple streams
                    context().forward(record, childNodes.get(i));
                    return;
                }
            }
            // using default child node if supplied
            if (childNodes.size() > predicates.size()) {
                context().forward(record, childNodes.get(predicates.size()));
            }
        }
    }
}
