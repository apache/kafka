/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.KTableValueGetterStub;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.NoOpReadOnlyStore;
import org.junit.Test;

public class GlobalKTableImplTest {

    private final GlobalKTable<String, String> table = new GlobalKTableImpl<>(new ValueGetterSupplier<String, String>(),
                                                                        new NoOpReadOnlyStore<String, String>(),
                                                                        new KStreamBuilder());

    private final GlobalKTable<String, String> other = new GlobalKTableImpl<>(new ValueGetterSupplier<String, String>(),
                                                                        new NoOpReadOnlyStore<String, String>(),
                                                                        new KStreamBuilder());


    @Test(expected = NullPointerException.class)
    public void shouldThrowNpeOnJoinWhenOtherTableIsNull() throws Exception {
        table.join(null, MockKeyValueMapper.<String, String>SelectValueMapper(), MockValueJoiner.STRING_JOINER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNpeOnLeftJoinWhenOtherTableIsNull() throws Exception {
        table.leftJoin(null, MockKeyValueMapper.<String, String>SelectValueMapper(), MockValueJoiner.STRING_JOINER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNpeOnJoinWhenKeyMapperIsNull() throws Exception {
        table.join(other, null, MockValueJoiner.STRING_JOINER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNpeOnLeftJoinWhenKeyMapperIsNull() throws Exception {
        table.leftJoin(other, null, MockValueJoiner.STRING_JOINER, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNpeOnJoinWhenJoinerIsNull() throws Exception {
        table.join(other, MockKeyValueMapper.<String, String>SelectValueMapper(), null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNpeOnLeftJoinWhenJoinerIsNull() throws Exception {
        table.leftJoin(other, MockKeyValueMapper.<String, String>SelectValueMapper(), null, "store");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNpeOnJoinWhenStoreNameIsNull() throws Exception {
        table.join(other, MockKeyValueMapper.<String, String>SelectValueMapper(), MockValueJoiner.STRING_JOINER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNpeOnLeftJoinWhenStoreNameIsNull() throws Exception {
        table.leftJoin(other, MockKeyValueMapper.<String, String>SelectValueMapper(), MockValueJoiner.STRING_JOINER, null);
    }

    private class ValueGetterSupplier<K, V> implements KTableValueGetterSupplier<K, V> {

        @Override
        public KTableValueGetter<K, V> get() {
            return new KTableValueGetterStub<>();
        }

        @Override
        public String[] storeNames() {
            return new String[0];
        }
    }
}