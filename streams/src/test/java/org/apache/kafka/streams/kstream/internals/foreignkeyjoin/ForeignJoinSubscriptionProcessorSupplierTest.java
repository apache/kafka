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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.Assert;
import org.junit.Test;

public class ForeignJoinSubscriptionProcessorSupplierTest {
    final Map<String, ValueAndTimestamp<String>> fks = Collections.singletonMap(
        "fk1", ValueAndTimestamp.make("foo", 1L)
    );
    final KTableValueGetterSupplier<String, String> valueGetterSupplier = valueGetterSupplier(fks);
    final Processor<CombinedKey<String, String>,
                    Change<ValueAndTimestamp<SubscriptionWrapper<String>>>,
                    String,
                    SubscriptionResponseWrapper<String>>
        processor = processor(valueGetterSupplier);

    @Test
    public void shouldDetectVersionChange() {
        // This test serves as a reminder to add new tests once we bump SubscriptionWrapper version.
        Assert.assertEquals(SubscriptionWrapper.VERSION_1, SubscriptionWrapper.CURRENT_VERSION);
    }

    @Test
    public void shouldDeleteKeyAndPropagateFKV0() {
        final MockProcessorContext<String, SubscriptionResponseWrapper<String>> context = new MockProcessorContext<>();
        processor.init(context);

        final SubscriptionWrapper<String> newValue = new SubscriptionWrapper<>(
            new long[]{1L},
            Instruction.DELETE_KEY_AND_PROPAGATE,
            "pk1",
            SubscriptionWrapper.VERSION_0,
            null
        );
        final Record<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> record =
            new Record<>(
                new CombinedKey<>("fk1", "pk1"),
                new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                1L
            );
        processor.process(record);
        final List<CapturedForward<? extends String, ? extends SubscriptionResponseWrapper<String>>> forwarded = context.forwarded();
        Assert.assertEquals(1, forwarded.size());
        Assert.assertEquals(
            new Record<>(
                "pk1",
                new SubscriptionResponseWrapper<>(
                    newValue.getHash(),
                    null,
                    null),
                1L
            ),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldDeleteKeyAndPropagateFKV1() {
        final MockProcessorContext<String, SubscriptionResponseWrapper<String>> context = new MockProcessorContext<>();
        processor.init(context);

        final SubscriptionWrapper<String> newValue = new SubscriptionWrapper<>(
            new long[]{1L},
            Instruction.DELETE_KEY_AND_PROPAGATE,
            "pk1",
            SubscriptionWrapper.VERSION_1,
            12
        );
        final Record<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> record =
            new Record<>(
                new CombinedKey<>("fk1", "pk1"),
                new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                1L
            );
        processor.process(record);
        final List<CapturedForward<? extends String, ? extends SubscriptionResponseWrapper<String>>> forwarded = context.forwarded();
        Assert.assertEquals(1, forwarded.size());
        Assert.assertEquals(
            new Record<>(
                "pk1",
                new SubscriptionResponseWrapper<>(
                    newValue.getHash(),
                    null,
                    12
                ),
                1L
            ),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldPropagateOnlyIfFKAvailableV0() {
        final MockProcessorContext<String, SubscriptionResponseWrapper<String>> context = new MockProcessorContext<>();
        processor.init(context);

        final SubscriptionWrapper<String> newValue = new SubscriptionWrapper<>(
            new long[]{1L},
            Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            "pk1",
            SubscriptionWrapper.VERSION_0,
            null
        );
        final Record<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> record =
            new Record<>(
                new CombinedKey<>("fk1", "pk1"),
                new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                1L
        );
        processor.process(record);
        final List<CapturedForward<? extends String, ? extends SubscriptionResponseWrapper<String>>> forwarded = context.forwarded();
        Assert.assertEquals(1, forwarded.size());
        Assert.assertEquals(
            new Record<>(
                "pk1",
                new SubscriptionResponseWrapper<>(
                    newValue.getHash(),
                    "foo",
                    null
                ),
                1L
            ),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldPropagateOnlyIfFKAvailableV1() {
        final MockProcessorContext<String, SubscriptionResponseWrapper<String>> context = new MockProcessorContext<>();
        processor.init(context);

        final SubscriptionWrapper<String> newValue = new SubscriptionWrapper<>(
            new long[]{1L},
            Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            "pk1",
            SubscriptionWrapper.VERSION_1,
            12
        );
        final Record<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> record =
            new Record<>(
                new CombinedKey<>("fk1", "pk1"),
                new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                1L
            );
        processor.process(record);
        final List<CapturedForward<? extends String, ? extends SubscriptionResponseWrapper<String>>> forwarded = context.forwarded();
        Assert.assertEquals(1, forwarded.size());
        Assert.assertEquals(
            new Record<>(
                "pk1",
                new SubscriptionResponseWrapper<>(
                    newValue.getHash(),
                    "foo",
                     12
                ),
                1L
            ),
            forwarded.get(0).record());
    }

    @Test
    public void shouldPropagateNullIfNoFKAvailableV0() {
        final MockProcessorContext<String, SubscriptionResponseWrapper<String>> context = new MockProcessorContext<>();
        processor.init(context);

        final SubscriptionWrapper<String> newValue = new SubscriptionWrapper<>(
            new long[]{1L},
            Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE,
            "pk1",
            SubscriptionWrapper.VERSION_0,
            null
        );
        Record<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> record =
            new Record<>(
                new CombinedKey<>("fk1", "pk1"),
                new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                1L
            );
        processor.process(record);
        // propagate matched FK
        List<CapturedForward<? extends String, ? extends SubscriptionResponseWrapper<String>>> forwarded = context.forwarded();
        Assert.assertEquals(1, forwarded.size());
        Assert.assertEquals(
            new Record<>(
                "pk1",
                new SubscriptionResponseWrapper<>(
                    newValue.getHash(),
                    "foo",
                    null
                ),
                1L
            ),
            forwarded.get(0).record());

        record = new Record<>(
                new CombinedKey<>("fk9000", "pk1"),
                new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                1L
            );
        processor.process(record);
        // propagate null if there is no match
        forwarded = context.forwarded();
        Assert.assertEquals(2, forwarded.size());
        Assert.assertEquals(
            new Record<>(
                "pk1",
                new SubscriptionResponseWrapper<>(
                    newValue.getHash(),
                    null,
                    null
                ),
                1L
            ),
            forwarded.get(1).record());
    }

    @Test
    public void shouldPropagateNullIfNoFKAvailableV1() {
        final MockProcessorContext<String, SubscriptionResponseWrapper<String>> context = new MockProcessorContext<>();
        processor.init(context);

        final SubscriptionWrapper<String> newValue = new SubscriptionWrapper<>(
            new long[]{1L},
            Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE,
            "pk1",
            SubscriptionWrapper.VERSION_1,
            12);
        Record<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> record =
            new Record<>(
                new CombinedKey<>("fk1", "pk1"),
                new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                1L
            );
        processor.process(record);
        List<CapturedForward<? extends String, ? extends SubscriptionResponseWrapper<String>>> forwarded = context.forwarded();
        Assert.assertEquals(1, forwarded.size());
        Assert.assertEquals(
            new Record<>(
                "pk1",
                new SubscriptionResponseWrapper<>(
                    newValue.getHash(),
                    "foo",
                    12
                ),
                1L
            ),
            forwarded.get(0).record());

        record = new Record<>(
            new CombinedKey<>("fk9000", "pk1"),
            new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
            1L
        );
        processor.process(record);
        // propagate null if there is no match
        forwarded = context.forwarded();
        Assert.assertEquals(2, forwarded.size());
        Assert.assertEquals(
            new Record<>(
                "pk1",
                new SubscriptionResponseWrapper<>(
                    newValue.getHash(),
                    null,
                    12
                ),
                1L
            ),
            forwarded.get(1).record());
    }

    @Test
    public void shouldDeleteKeyNoPropagateV0() {
        final MockProcessorContext<String, SubscriptionResponseWrapper<String>> context = new MockProcessorContext<>();
        processor.init(context);

        final SubscriptionWrapper<String> newValue = new SubscriptionWrapper<>(
            new long[]{1L},
            Instruction.DELETE_KEY_NO_PROPAGATE,
            "pk1",
            SubscriptionWrapper.VERSION_0,
            null);
        final Record<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> record =
            new Record<>(
                new CombinedKey<>("fk1", "pk1"),
                new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                1L
            );
        processor.process(record);
        final List<CapturedForward<? extends String, ? extends SubscriptionResponseWrapper<String>>> forwarded = context.forwarded();
        Assert.assertEquals(0, forwarded.size());
    }

    @Test
    public void shouldDeleteKeyNoPropagateV1() {
        final MockProcessorContext<String, SubscriptionResponseWrapper<String>> context = new MockProcessorContext<>();
        processor.init(context);

        final SubscriptionWrapper<String> newValue = new SubscriptionWrapper<>(
            new long[]{1L},
            Instruction.DELETE_KEY_NO_PROPAGATE,
            "pk1",
            SubscriptionWrapper.VERSION_1,
            12);
        final Record<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> record =
            new Record<>(new CombinedKey<>("fk1", "pk1"),
                         new Change<>(ValueAndTimestamp.make(newValue, 1L), null),
                        1L
            );
        processor.process(record);
        final List<CapturedForward<? extends String, ? extends SubscriptionResponseWrapper<String>>> forwarded = context.forwarded();
        Assert.assertEquals(0, forwarded.size());
    }

    private KTableValueGetterSupplier<String, String> valueGetterSupplier(final Map<String, ValueAndTimestamp<String>> map) {
        final KTableValueGetter<String, String> valueGetter = new KTableValueGetter<String, String>() {

            @Override
            public ValueAndTimestamp<String> get(final String key) {
                return map.get(key);
            }

            @Override
            public void init(final ProcessorContext context) {

            }
        };
        return new KTableValueGetterSupplier<String, String>() {
            @Override
            public KTableValueGetter<String, String> get() {
                return valueGetter;
            }

            @Override
            public String[] storeNames() {
                return new String[0];
            }
        };
    }

    private Processor<CombinedKey<String, String>,
                      Change<ValueAndTimestamp<SubscriptionWrapper<String>>>,
                      String,
                      SubscriptionResponseWrapper<String>> processor(final KTableValueGetterSupplier<String, String> valueGetterSupplier) {
        final SubscriptionJoinForeignProcessorSupplier<String, String, String> supplier =
            new SubscriptionJoinForeignProcessorSupplier<>(valueGetterSupplier);
        return supplier.get();
    }
}