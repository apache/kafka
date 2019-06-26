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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.*;

public class KTableRepartitionerProcessorSupplier<K, KO, V> implements ProcessorSupplier<K, Change<V>> {

    private final ValueMapper<V, KO> mapper;
    private final Serializer<V> valueSerializer;

    public KTableRepartitionerProcessorSupplier(final ValueMapper<V, KO> extractor,
                                                final Serializer<V> valueSerializer) {
        this.mapper = extractor;
        this.valueSerializer = valueSerializer;
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
            long[] nullHash = Murmur3.hash128(new byte[]{});
            long[] currentHash = (change.newValue == null ?
                    Murmur3.hash128(new byte[]{}):
                    Murmur3.hash128(valueSerializer.serialize(context().topic(), change.newValue)));

            //DELETE_KEY_NO_PROPAGATE
              //Send nothing. Do not propagate.
            //DELETE_KEY_AND_PROPAGATE
              //Send (k, null)
            //PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE (changing foreign key, but FK+Val may not exist)
              //Send (k, fk-val) OR
              //Send (k, null) if fk-val does not exist
            //PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE (first time ever sending key)
              //Send (k, fk-val) only if fk-val exists.

            if (change.oldValue != null) {
                final KO oldForeignKey = mapper.apply(change.oldValue);
                final CombinedKey<KO, K> combinedOldKey = new CombinedKey<>(oldForeignKey, key);
                if (change.newValue != null) {
                    final KO extractedNewForeignKey = mapper.apply(change.newValue);
                    final CombinedKey<KO, K> combinedNewKey = new CombinedKey<>(extractedNewForeignKey, key);

                    //Requires equal to be defined...
                    if (oldForeignKey.equals(extractedNewForeignKey)) {
                        //Same foreign key. Just propagate onwards.
                        context().forward(combinedNewKey, new SubscriptionWrapper(currentHash, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE));
                    } else {
                        //Different Foreign Key - delete the old key value and propagate the new one.

                        //Delete it from the oldKey's state store
                        context().forward(combinedOldKey, new SubscriptionWrapper(nullHash, DELETE_KEY_NO_PROPAGATE));
                        // Add to the newKey's state store. Additionally, propagate null if no FK is found there,
                        // since we must "unset" any output set by the previous FK-join. This is true for both INNER
                        // and LEFT join.
                        context().forward(combinedNewKey, new SubscriptionWrapper(currentHash, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE));
                    }
                } else {
                    //A simple propagatable delete. Delete from the state store and propagate the delete onwards.
                    context().forward(combinedOldKey, new SubscriptionWrapper(nullHash, DELETE_KEY_AND_PROPAGATE));
                }
            } else if (change.newValue != null) {
                //change.oldValue is null, which means it was deleted at least once before, or it is brand new.
                //In either case, we only need to propagate if the FK_VAL is available, as the null from the delete would
                //have been propagated otherwise.
                final KO extractedForeignKeyValue = mapper.apply(change.newValue);
                final CombinedKey<KO, K> newCombinedKeyValue = new CombinedKey<>(extractedForeignKeyValue, key);
                context().forward(newCombinedKeyValue, new SubscriptionWrapper(currentHash, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE));
            }
        }

        @Override
        public void close() {}
    }
}
