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

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.test.KTableValueGetterStub;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@SuppressWarnings("unchecked")
public class KTableGlobalKTableLeftJoinTest {

    private final KTableValueGetterStub<String, String> global = new KTableValueGetterStub<>();
    private final KeyValueMapper<String, String, String> keyValueMapper = new KeyValueMapper<String, String, String>() {
        @Override
        public String apply(final String key, final String value) {
            return value;
        }
    };
    private final KTableGlobalKTableLeftJoin<String, String, String, String, String> join
            = new KTableGlobalKTableLeftJoin<>(new ValueGetterSupplier<>(new KTableValueGetterStub<String, String>()),
                                               new ValueGetterSupplier<>(global),
                                               MockValueJoiner.STRING_JOINER,
                                               keyValueMapper);

    private NoOpProcessorContext context;

    @Before
    public void before() {
        context = new NoOpProcessorContext();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldJoinWithNullAndForwardIfKeyNotInGlobalStore() throws Exception {
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        processor.process("A", new Change<>("1", null));
        final Change<String> a = (Change<String>) context.forwardedValues.get("A");
        assertThat(a.newValue, equalTo("1+null"));
        assertThat(a.oldValue, is(nullValue()));
    }


    @Test
    public void shouldJoinAndForwardIfKeyFromNewValueInOtherStore() throws Exception {
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        global.put("1", "B");
        processor.process("A", new Change<>("1", null));
        final Change<String> a = (Change<String>) context.forwardedValues.get("A");
        assertThat(a.newValue, equalTo("1+B"));
        assertThat(a.oldValue, is(nullValue()));
    }

    @Test
    public void shouldSendNewAndOldValuesCorrectly() throws Exception {
        join.enableSendingOldValues();
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        global.put("1", "B");
        global.put("2", "C");
        processor.process("A", new Change<>("2", "1"));
        final Change<String> a = (Change<String>) context.forwardedValues.get("A");
        assertThat(a.newValue, equalTo("2+C"));
        assertThat(a.oldValue, equalTo("1+B"));
    }

    @Test
    public void shouldSendOldValueIfNewValueIsNullOldValueIsntNull() throws Exception {
        join.enableSendingOldValues();
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        global.put("1", "B");
        processor.process("A", new Change<>(null, "1"));
        final Change<String> a = (Change<String>) context.forwardedValues.get("A");
        assertThat(a.newValue, is(nullValue()));
        assertThat(a.oldValue, equalTo("1+B"));
    }

    @Test
    public void shouldJoinAndForwardIfOldKeyNotInOtherStoreAndSendOldValues() throws Exception {
        join.enableSendingOldValues();
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        processor.process("A", new Change<>(null, "1"));
        final Change<String> a = (Change<String>) context.forwardedValues.get("A");
        assertThat(a.newValue, is(nullValue()));
        assertThat(a.oldValue, is("1+null"));
    }

    @Test
    public void shouldNotForwardIfOtherStoreValueIsNullAndNewAndOldValuesAreNull() throws Exception {
        join.enableSendingOldValues();
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        processor.process("A", new Change<String>(null, null));
        assertThat(context.forwardedValues.get("A"), is(nullValue()));
    }

    @Test
    public void shouldSendDeletesIfChangeHasOldValue() throws Exception {
        global.put("1", "A");
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        processor.process("A", new Change<>(null, "1"));
        final Change<String> a = (Change<String>) context.forwardedValues.get("A");
        assertThat(a.newValue, is(nullValue()));
    }

    @Test
    public void shouldNotSendAnythingIfChangeIsNullNull() throws Exception {
        global.put("1", "A");
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        processor.process("A", new Change<String>(null, null));
        assertThat(context.forwardedValues.get("A"), is(nullValue()));
    }

    static class ValueGetterSupplier<K, V> implements KTableValueGetterSupplier<K, V> {

        private final KTableValueGetterStub<K, V> valueGetter;

        ValueGetterSupplier(final KTableValueGetterStub<K, V> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @Override
        public KTableValueGetter<K, V> get() {
            return valueGetter;
        }

        @Override
        public String[] storeNames() {
            return new String[0];
        }
    }

}