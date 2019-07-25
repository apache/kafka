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

package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Murmur3;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.function.Function;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE;

public class KTableRepartitionerProcessorSupplier<K, KO, V> implements ProcessorSupplier<K, Change<V>> {

    private final Function<V, KO> foreignKeyExtractor;
    private final Serializer<V> valueSerializer;
    private final boolean leftJoin;

    public KTableRepartitionerProcessorSupplier(final Function<V, KO> foreignKeyExtractor,
                                                final Serializer<V> valueSerializer,
                                                final boolean leftJoin) {
        this.foreignKeyExtractor = foreignKeyExtractor;
        this.valueSerializer = valueSerializer;
        this.leftJoin = leftJoin;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new UnbindChangeProcessor();
    }

    private class UnbindChangeProcessor extends AbstractProcessor<K, Change<V>> {
        
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
        }

        @Override
        public void process(final K key, final Change<V> change) {
            final long[] currentHash = change.newValue == null ?
                    null :
                    Murmur3.hash128(valueSerializer.serialize(context().topic(), change.newValue));

            if (change.oldValue != null) {
                final KO oldForeignKey = foreignKeyExtractor.apply(change.oldValue);
                if (change.newValue != null) {
                    final KO newForeignKey = foreignKeyExtractor.apply(change.newValue);

                    //Requires equal to be defined...
                    if (oldForeignKey.equals(newForeignKey)) {
                        //Same foreign key. Just propagate onwards.
                        context().forward(newForeignKey, new SubscriptionWrapper<>(currentHash, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE, key));
                    } else {
                        //Different Foreign Key - delete the old key value and propagate the new one.
                        //Delete it from the oldKey's state store
                        context().forward(oldForeignKey, new SubscriptionWrapper<>(currentHash, DELETE_KEY_NO_PROPAGATE, key));
                        //Add to the newKey's state store. Additionally, propagate null if no FK is found there,
                        //since we must "unset" any output set by the previous FK-join. This is true for both INNER
                        //and LEFT join.
                        context().forward(newForeignKey, new SubscriptionWrapper<>(currentHash, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE, key));
                    }
                } else {
                    //A simple propagatable delete. Delete from the state store and propagate the delete onwards.
                    context().forward(oldForeignKey, new SubscriptionWrapper<>(currentHash, DELETE_KEY_AND_PROPAGATE, key));
                }
            } else if (change.newValue != null) {
                //change.oldValue is null, which means it was deleted at least once before, or it is brand new.
                //In either case, we only need to propagate if the FK_VAL is available, as the null from the delete would
                //have been propagated otherwise.

                final SubscriptionWrapper.Instruction instruction;
                if (leftJoin) {
                    //Want to send info even if RHS is null.
                    instruction = PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
                } else {
                    instruction = PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;
                }
                context().forward(foreignKeyExtractor.apply(change.newValue), new SubscriptionWrapper<>(currentHash, instruction, key));
            }
        }

        @Override
        public void close() {}
    }
}
