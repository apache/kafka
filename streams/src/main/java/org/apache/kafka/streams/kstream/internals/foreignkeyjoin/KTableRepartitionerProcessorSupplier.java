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

import java.security.NoSuchAlgorithmException;
import java.util.Random;

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

            if (change.oldValue != null) {
                final KO oldForeignKey = mapper.apply(change.oldValue);
                final CombinedKey<KO, K> combinedOldKey = new CombinedKey<>(oldForeignKey, key);
                if (change.newValue != null) {
                    final KO extractedNewForeignKey = mapper.apply(change.newValue);
                    final CombinedKey<KO, K> combinedNewKey = new CombinedKey<>(extractedNewForeignKey, key);

                    //Requires equal to be defined...
                    if (oldForeignKey.equals(extractedNewForeignKey)) {
                        //Same foreign key. Just propagate onwards.
                        context().forward(combinedNewKey, new SubscriptionWrapper(currentHash, true));
                    } else {
                        //Different Foreign Key - delete the old key value and propagate the new one.
                        //Note that we indicate that we don't want to propagate the delete to the join output.
                        //The downstream processor to delete it from the local state store, but not propagate it.
                        context().forward(combinedOldKey, new SubscriptionWrapper(nullHash, false));
                        context().forward(combinedNewKey, new SubscriptionWrapper(currentHash, true));
                    }
                } else {
                    //A propagatable delete. Set hash to null instead of using the null hash code.
                    context().forward(combinedOldKey, new SubscriptionWrapper(nullHash, true));
                }
            } else if (change.newValue != null) {
                final KO extractedForeignKeyValue = mapper.apply(change.newValue);
                final CombinedKey<KO, K> newCombinedKeyValue = new CombinedKey<>(extractedForeignKeyValue, key);
                context().forward(newCombinedKeyValue, new SubscriptionWrapper(currentHash, true));
            }
        }

        @Override
        public void close() {}
    }
}
