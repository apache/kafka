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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StoreBuilderWrapper;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SubscriptionReceiveProcessorSupplierTest {

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private File stateDir;
    private MockInternalProcessorContext<CombinedKey<String, String>, Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> context;
    private TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>> stateStore = null;

    private static final String FK = "fk1";
    private static final String PK1 = "pk1";
    private static final String PK2 = "pk2";

    private static final Supplier<String> PK_SERDE_TOPIC_SUPPLIER = () -> "pk-topic";
    private static final CombinedKeySchema<String, String> COMBINED_KEY_SCHEMA = new CombinedKeySchema<>(
        () -> "fk-topic",
        Serdes.String(),
        PK_SERDE_TOPIC_SUPPLIER,
        Serdes.String()
    );

    @BeforeEach
    public void before() {
        stateDir = TestUtils.tempDirectory();
        context = new MockInternalProcessorContext<>(props, new TaskId(0, 0), stateDir);
    }

    @AfterEach
    public void after() throws IOException {
        if (stateStore != null) {
            stateStore.close();
        }
        Utils.delete(stateDir);
    }

    @Test
    public void shouldDetectVersionChange() {
        // This test serves as a reminder to add new tests once we bump SubscriptionWrapper version.
        assertEquals(SubscriptionWrapper.VERSION_1, SubscriptionWrapper.CURRENT_VERSION);
    }

    @Test
    public void shouldDeleteKeyAndPropagateV0() {
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        final SubscriptionReceiveProcessorSupplier<String, String> supplier = supplier(storeBuilder);
        final Processor<String,
                        SubscriptionWrapper<String>,
                        CombinedKey<String, String>,
                        Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> processor = supplier.get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);

        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.DELETE_KEY_AND_PROPAGATE,
            PK2,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(FK, PK1);
        stateStore.put(key, oldValue);
        processor.init(context);

        final SubscriptionWrapper<String> newWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.DELETE_KEY_AND_PROPAGATE,
            PK1,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> newValue = ValueAndTimestamp.make(
            newWrapper, 1L);
        final Record<String, SubscriptionWrapper<String>> record = new Record<>(
            FK,
            newWrapper,
            1L
        );
        processor.process(record);

        final List<CapturedForward<? extends CombinedKey<String, String>,
                                   ? extends Change<ValueAndTimestamp<SubscriptionWrapper<String>>>>> forwarded = context.forwarded();
        assertNull(stateStore.get(key));
        assertEquals(1, forwarded.size());
        assertEquals(
            record.withKey(new CombinedKey<>(FK, PK1))
                  .withValue(new Change<>(newValue, oldValue)),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldDeleteKeyAndPropagateV1() {
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        final SubscriptionReceiveProcessorSupplier<String, String> supplier = supplier(storeBuilder);
        final Processor<String,
            SubscriptionWrapper<String>,
            CombinedKey<String, String>,
            Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> processor = supplier.get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);
        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.DELETE_KEY_AND_PROPAGATE,
            PK2,
            SubscriptionWrapper.VERSION_1,
            1
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(FK, PK1);
        stateStore.put(key, oldValue);
        processor.init(context);

        final SubscriptionWrapper<String> newWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.DELETE_KEY_AND_PROPAGATE,
            PK1,
            SubscriptionWrapper.VERSION_1,
            1
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> newValue = ValueAndTimestamp.make(
            newWrapper, 1L);
        final Record<String, SubscriptionWrapper<String>> record = new Record<>(
            FK,
            newWrapper,
            1L
        );
        processor.process(record);

        final List<CapturedForward<? extends CombinedKey<String, String>,
            ? extends Change<ValueAndTimestamp<SubscriptionWrapper<String>>>>> forwarded = context.forwarded();
        assertNull(stateStore.get(key));
        assertEquals(1, forwarded.size());
        assertEquals(
            record.withKey(new CombinedKey<>(FK, PK1))
                .withValue(new Change<>(newValue, oldValue)),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldDeleteKeyNoPropagateV0() {
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        final SubscriptionReceiveProcessorSupplier<String, String> supplier = supplier(storeBuilder);
        final Processor<String,
            SubscriptionWrapper<String>,
            CombinedKey<String, String>,
            Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> processor = supplier.get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);

        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.DELETE_KEY_NO_PROPAGATE,
            PK2,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(FK, PK1);
        stateStore.put(key, oldValue);
        processor.init(context);

        final SubscriptionWrapper<String> newWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.DELETE_KEY_NO_PROPAGATE,
            PK1,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> newValue = ValueAndTimestamp.make(
            newWrapper, 1L);
        final Record<String, SubscriptionWrapper<String>> record = new Record<>(
            FK,
            newWrapper,
            1L
        );
        processor.process(record);

        final List<CapturedForward<? extends CombinedKey<String, String>,
            ? extends Change<ValueAndTimestamp<SubscriptionWrapper<String>>>>> forwarded = context.forwarded();
        assertNull(stateStore.get(key));
        assertEquals(1, forwarded.size());
        assertEquals(
            record.withKey(new CombinedKey<>(FK, PK1))
                .withValue(new Change<>(newValue, oldValue)),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldDeleteKeyNoPropagateV1() {
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        final SubscriptionReceiveProcessorSupplier<String, String> supplier = supplier(storeBuilder);
        final Processor<String,
            SubscriptionWrapper<String>,
            CombinedKey<String, String>,
            Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> processor = supplier.get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);

        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.DELETE_KEY_NO_PROPAGATE,
            PK2,
            SubscriptionWrapper.VERSION_1,
            1
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(FK, PK1);
        stateStore.put(key, oldValue);
        processor.init(context);

        final SubscriptionWrapper<String> newWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.DELETE_KEY_NO_PROPAGATE,
            PK1,
            SubscriptionWrapper.VERSION_1,
            1
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> newValue = ValueAndTimestamp.make(
            newWrapper, 1L);
        final Record<String, SubscriptionWrapper<String>> record = new Record<>(
            FK,
            newWrapper,
            1L
        );
        processor.process(record);

        final List<CapturedForward<? extends CombinedKey<String, String>,
            ? extends Change<ValueAndTimestamp<SubscriptionWrapper<String>>>>> forwarded = context.forwarded();
        assertNull(stateStore.get(key));
        assertEquals(1, forwarded.size());
        assertEquals(
            record.withKey(new CombinedKey<>(FK, PK1))
                .withValue(new Change<>(newValue, oldValue)),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldPropagateOnlyIfFKValAvailableV0() {
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        final SubscriptionReceiveProcessorSupplier<String, String> supplier = supplier(storeBuilder);
        final Processor<String,
            SubscriptionWrapper<String>,
            CombinedKey<String, String>,
            Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> processor = supplier.get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);

        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            PK2,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(FK, PK1);
        stateStore.put(key, oldValue);
        processor.init(context);

        final SubscriptionWrapper<String> newWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            PK1,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> newValue = ValueAndTimestamp.make(
            newWrapper, 1L);
        final Record<String, SubscriptionWrapper<String>> record = new Record<>(
            FK,
            newWrapper,
            1L
        );
        processor.process(record);
        final List<CapturedForward<? extends CombinedKey<String, String>,
            ? extends Change<ValueAndTimestamp<SubscriptionWrapper<String>>>>> forwarded = context.forwarded();

        assertEquals(newValue, stateStore.get(key));
        assertEquals(1, forwarded.size());
        assertEquals(
            record.withKey(new CombinedKey<>(FK, PK1))
                .withValue(new Change<>(newValue, oldValue)),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldPropagateOnlyIfFKValAvailableV1() {
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        final SubscriptionReceiveProcessorSupplier<String, String> supplier = supplier(storeBuilder);
        final Processor<String,
            SubscriptionWrapper<String>,
            CombinedKey<String, String>,
            Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> processor = supplier.get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);

        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            PK2,
            SubscriptionWrapper.VERSION_1,
            1
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(FK, PK1);
        stateStore.put(key, oldValue);
        processor.init(context);

        final SubscriptionWrapper<String> newWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            PK1,
            SubscriptionWrapper.VERSION_1,
            1
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> newValue = ValueAndTimestamp.make(
            newWrapper, 1L);
        final Record<String, SubscriptionWrapper<String>> record = new Record<>(
            FK,
            newWrapper,
            1L
        );
        processor.process(record);
        final List<CapturedForward<? extends CombinedKey<String, String>,
            ? extends Change<ValueAndTimestamp<SubscriptionWrapper<String>>>>> forwarded = context.forwarded();

        assertEquals(newValue, stateStore.get(key));
        assertEquals(1, forwarded.size());
        assertEquals(
            record.withKey(new CombinedKey<>(FK, PK1))
                .withValue(new Change<>(newValue, oldValue)),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldPropagateNullIfNoFKValAvailableV0() {
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        final SubscriptionReceiveProcessorSupplier<String, String> supplier = supplier(storeBuilder);
        final Processor<String,
            SubscriptionWrapper<String>,
            CombinedKey<String, String>,
            Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> processor = supplier.get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);

        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE,
            PK2,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(FK, PK1);
        stateStore.put(key, oldValue);
        processor.init(context);

        final SubscriptionWrapper<String> newWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE,
            PK1,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> newValue = ValueAndTimestamp.make(
            newWrapper, 1L);
        final Record<String, SubscriptionWrapper<String>> record = new Record<>(
            FK,
            newWrapper,
            1L
        );
        processor.process(record);
        final List<CapturedForward<? extends CombinedKey<String, String>,
            ? extends Change<ValueAndTimestamp<SubscriptionWrapper<String>>>>> forwarded = context.forwarded();

        assertEquals(newValue, stateStore.get(key));
        assertEquals(1, forwarded.size());
        assertEquals(
            record.withKey(new CombinedKey<>(FK, PK1))
                .withValue(new Change<>(newValue, oldValue)),
            forwarded.get(0).record()
        );
    }

    @Test
    public void shouldPropagateNullIfNoFKValAvailableV1() {
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        final SubscriptionReceiveProcessorSupplier<String, String> supplier = supplier(storeBuilder);
        final Processor<String,
            SubscriptionWrapper<String>,
            CombinedKey<String, String>,
            Change<ValueAndTimestamp<SubscriptionWrapper<String>>>> processor = supplier.get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);

        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE,
            PK2,
            SubscriptionWrapper.VERSION_1,
            1
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(FK, PK1);
        stateStore.put(key, oldValue);
        processor.init(context);

        final SubscriptionWrapper<String> newWrapper = new SubscriptionWrapper<>(
            new long[]{1L, 2L},
            Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE,
            PK1,
            SubscriptionWrapper.VERSION_1,
            1
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> newValue = ValueAndTimestamp.make(
            newWrapper, 1L);
        final Record<String, SubscriptionWrapper<String>> record = new Record<>(
            FK,
            newWrapper,
            1L
        );
        processor.process(record);
        final List<CapturedForward<? extends CombinedKey<String, String>,
            ? extends Change<ValueAndTimestamp<SubscriptionWrapper<String>>>>> forwarded = context.forwarded();

        assertEquals(newValue, stateStore.get(key));
        assertEquals(1, forwarded.size());
        assertEquals(
            record.withKey(new CombinedKey<>(FK, PK1))
                .withValue(new Change<>(newValue, oldValue)),
            forwarded.get(0).record()
        );
    }


    private SubscriptionReceiveProcessorSupplier<String, String> supplier(
        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder) {

        return new SubscriptionReceiveProcessorSupplier<>(
            StoreBuilderWrapper.wrapStoreBuilder(storeBuilder),
            COMBINED_KEY_SCHEMA
        );
    }

    private StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder() {
        final Serde<SubscriptionWrapper<String>> subscriptionWrapperSerde = new SubscriptionWrapperSerde<>(
            PK_SERDE_TOPIC_SUPPLIER, Serdes.String());
        return Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore(
                "Store"
            ),
            new Serdes.BytesSerde(),
            subscriptionWrapperSerde
        );
    }
}