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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class KTableProcessFixedKeyTest {

    private static final String QUERYABLE_NAME = "queryable-store";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String STORE_NAME = "someStore";
    private static final String OTHER_STORE_NAME = "otherStore";

    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());

    private TopologyTestDriver driver;
    private MockProcessorSupplier<String, String, Void, Void> capture;
    private StreamsBuilder builder;

    @Mock
    private KTableImpl<String, String, String> parent;
    @Mock
    private InternalProcessorContext<String, Change<String>> context;
    @Mock
    private KTableValueGetterSupplier<String, String> parentGetterSupplier;
    @Mock
    private KTableValueGetter<String, String> parentGetter;
    @Mock
    private TimestampedKeyValueStoreWithHeaders<String, String> stateStore;
    @Mock
    private FixedKeyProcessorSupplier<String, String, String> mockSupplier;
    @Mock
    private FixedKeyProcessor<String, String, String> fixedKeyProcessor;

    @AfterEach
    public void cleanup() {
        if (driver != null) {
            driver.close();
            driver = null;
        }
    }

    @BeforeEach
    public void setUp() {
        capture = new MockProcessorSupplier<>();
        builder = new StreamsBuilder();
    }

    @Test
    public void shouldThrowOnGetIfSupplierReturnsNull() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, new NullFixedKeyProcessorSupplier(), QUERYABLE_NAME);

        try {
            processFixedKey.get();
            fail("NPE expected");
        } catch (final NullPointerException expected) {
            // expected
        }
    }

    @Test
    public void shouldThrowOnViewGetIfSupplierReturnsNull() {
        final KTableValueGetterSupplier<String, String> view =
            new KTableProcessFixedKey<>(parent, new NullFixedKeyProcessorSupplier(), null).view();

        try {
            view.get();
            fail("NPE expected");
        } catch (final NullPointerException expected) {
            // expected
        }
    }

    @Test
    public void shouldInitializeProcessorWithCapturingContext() {
        // Capture the context given to the processor's init()
        final FixedKeyProcessorContext<?, ?>[] capturedCtx = new FixedKeyProcessorContext[1];
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, () -> new FixedKeyProcessor<>() {
                @Override
                public void init(final FixedKeyProcessorContext<String, String> ctx) {
                    capturedCtx[0] = ctx;
                }

                @Override
                public void process(final FixedKeyRecord<String, String> record) {
                }
            }, null);

        final Processor<String, Change<String>, String, Change<String>> processor = processFixedKey.get();
        processor.init(context);

        // Should be wrapped in CapturingFixedKeyProcessorContext, not the raw InternalProcessorContext
        assertFalse(capturedCtx[0] instanceof InternalProcessorContext,
            "Processor should receive a capturing context wrapper, not the raw InternalProcessorContext");
    }

    @Test
    public void shouldNotSendOldValuesByDefault() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, new ExclamationFixedKeyProcessorSupplier(), null);

        final Processor<String, Change<String>, String, Change<String>> processor = processFixedKey.get();
        processor.init(context);

        doNothing().when(context).forward(new Record<>("Key", new Change<>("Key->newValue!", null), 0));

        processor.process(new Record<>("Key", new Change<>("newValue", "oldValue"), 0));
    }

    @Test
    public void shouldSendOldValuesIfConfigured() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, new ExclamationFixedKeyProcessorSupplier(), null);

        when(parent.enableSendingOldValues(true)).thenReturn(true);

        processFixedKey.enableSendingOldValues(true);
        final Processor<String, Change<String>, String, Change<String>> processor = processFixedKey.get();
        processor.init(context);

        doNothing().when(context).forward(new Record<>("Key", new Change<>("Key->newValue!", "Key->oldValue!"), 0));

        processor.process(new Record<>("Key", new Change<>("newValue", "oldValue"), 0));
    }

    @Test
    public void shouldNotSetSendOldValuesOnParentIfMaterialized() {
        new KTableProcessFixedKey<>(parent, () -> record -> { }, QUERYABLE_NAME).enableSendingOldValues(true);

        verify(parent, never()).enableSendingOldValues(anyBoolean());
    }

    @Test
    public void shouldSetSendOldValuesOnParentIfNotMaterialized() {
        when(parent.enableSendingOldValues(true)).thenReturn(true);

        new KTableProcessFixedKey<>(parent, () -> record -> { }, null).enableSendingOldValues(true);
    }

    @Test
    public void shouldTransformOnGetIfNotMaterialized() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, new ExclamationFixedKeyProcessorSupplier(), null);

        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);
        when(parentGetterSupplier.get()).thenReturn(parentGetter);
        when(parentGetter.get("Key")).thenReturn(ValueTimestampHeaders.make("Value", 73L, new RecordHeaders()));
        final ProcessorRecordContext recordContext = new ProcessorRecordContext(42L, 23L, -1, "foo", new RecordHeaders());
        when(context.recordContext()).thenReturn(recordContext);
        doNothing().when(context).setRecordContext(new ProcessorRecordContext(73L, -1L, -1, null, new RecordHeaders()));
        doNothing().when(context).setRecordContext(recordContext);

        final KTableValueGetter<String, String> getter = processFixedKey.view().get();
        getter.init(context);

        final String result = getter.get("Key").value();

        assertThat(result, is("Key->Value!"));
    }

    @Test
    public void shouldUseContextHeadersWhenValueTimestampHeadersIsNull() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, new ExclamationFixedKeyProcessorSupplier(), null);

        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);
        when(parentGetterSupplier.get()).thenReturn(parentGetter);
        when(parentGetter.get("Key")).thenReturn(null);

        final RecordHeaders contextHeaders = new RecordHeaders();
        contextHeaders.add("test-header", "test-value".getBytes());
        final ProcessorRecordContext recordContext = new ProcessorRecordContext(42L, 23L, -1, "foo", contextHeaders);
        when(context.recordContext()).thenReturn(recordContext);
        doNothing().when(context).setRecordContext(new ProcessorRecordContext(-1L, -1L, -1, null, new RecordHeaders()));
        doNothing().when(context).setRecordContext(recordContext);

        final KTableValueGetter<String, String> getter = processFixedKey.view().get();
        getter.init(context);

        final ValueTimestampHeaders<String> result = getter.get("Key");

        assertThat(result.value(), is("Key->null!"));
        assertThat(result.headers(), is(contextHeaders));
    }

    @Test
    public void shouldGetFromStateStoreIfMaterialized() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, new ExclamationFixedKeyProcessorSupplier(), QUERYABLE_NAME);

        when(context.getStateStore(QUERYABLE_NAME)).thenReturn(stateStore);
        when(stateStore.get("Key")).thenReturn(ValueTimestampHeaders.make("something", 0L, new RecordHeaders()));

        final KTableValueGetter<String, String> getter = processFixedKey.view().get();
        getter.init(context);

        final String result = getter.get("Key").value();

        assertThat(result, is("something"));
    }

    @Test
    public void shouldGetStoreNamesFromParentIfNotMaterialized() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, new ExclamationFixedKeyProcessorSupplier(), null);

        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);
        when(parentGetterSupplier.storeNames()).thenReturn(new String[]{"store1", "store2"});

        final String[] storeNames = processFixedKey.view().storeNames();

        assertThat(storeNames, is(new String[]{"store1", "store2"}));
    }

    @Test
    public void shouldGetQueryableStoreNameIfMaterialized() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, new ExclamationFixedKeyProcessorSupplier(), QUERYABLE_NAME);

        final String[] storeNames = processFixedKey.view().storeNames();

        assertThat(storeNames, is(new String[]{QUERYABLE_NAME}));
    }

    @Test
    public void shouldCloseProcessorOnProcessorClose() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, mockSupplier, null);

        when(mockSupplier.get()).thenReturn(fixedKeyProcessor);

        final Processor<String, Change<String>, String, Change<String>> processor = processFixedKey.get();
        processor.close();

        verify(fixedKeyProcessor).close();
    }

    @Test
    public void shouldCloseProcessorOnGetterClose() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, mockSupplier, null);

        when(mockSupplier.get()).thenReturn(fixedKeyProcessor);
        when(parentGetterSupplier.get()).thenReturn(parentGetter);
        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);

        final KTableValueGetter<String, String> getter = processFixedKey.view().get();
        getter.close();

        verify(fixedKeyProcessor).close();
    }

    @Test
    public void shouldCloseParentGetterClose() {
        final KTableProcessFixedKey<String, String, String> processFixedKey =
            new KTableProcessFixedKey<>(parent, mockSupplier, null);

        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);
        when(mockSupplier.get()).thenReturn(fixedKeyProcessor);
        when(parentGetterSupplier.get()).thenReturn(parentGetter);
        doNothing().when(parentGetter).close();

        final KTableValueGetter<String, String> getter = processFixedKey.view().get();
        getter.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldProcessValuesWithKey(final boolean withHeaders) {
        builder
            .addStateStore(storeBuilder(STORE_NAME))
            .addStateStore(storeBuilder(OTHER_STORE_NAME))
            .table(INPUT_TOPIC, CONSUMED)
            .processFixedKey(
                new ExclamationFixedKeyProcessorSupplier(STORE_NAME, OTHER_STORE_NAME),
                STORE_NAME, OTHER_STORE_NAME)
            .toStream()
            .process(capture);

        driver = new TopologyTestDriver(builder.build(), props(withHeaders));
        final TestInputTopic<String, String> inputTopic =
            driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

        inputTopic.pipeInput("A", "a", 5L);
        inputTopic.pipeInput("B", "b", 10L);
        inputTopic.pipeInput("D", null, 15L);

        assertThat(output(), hasItems(
            new KeyValueTimestamp<>("A", "A->a!", 5),
            new KeyValueTimestamp<>("B", "B->b!", 10),
            new KeyValueTimestamp<>("D", "D->null!", 15)
        ));
        assertNull(driver.getKeyValueStore(QUERYABLE_NAME), "Store should not be materialized");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldProcessValuesWithKeyAndMaterialize(final boolean withHeaders) {
        builder
            .addStateStore(storeBuilder(STORE_NAME))
            .table(INPUT_TOPIC, CONSUMED)
            .processFixedKey(
                new ExclamationFixedKeyProcessorSupplier(STORE_NAME, QUERYABLE_NAME),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(QUERYABLE_NAME)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String()),
                STORE_NAME)
            .toStream()
            .process(capture);

        driver = new TopologyTestDriver(builder.build(), props(withHeaders));
        final TestInputTopic<String, String> inputTopic =
            driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

        inputTopic.pipeInput("A", "a", 5L);
        inputTopic.pipeInput("B", "b", 10L);
        inputTopic.pipeInput("C", null, 15L);

        assertThat(output(), hasItems(
            new KeyValueTimestamp<>("A", "A->a!", 5),
            new KeyValueTimestamp<>("B", "B->b!", 10),
            new KeyValueTimestamp<>("C", "C->null!", 15)
        ));

        {
            final KeyValueStore<String, String> keyValueStore = driver.getKeyValueStore(QUERYABLE_NAME);
            assertThat(keyValueStore.get("A"), is("A->a!"));
            assertThat(keyValueStore.get("B"), is("B->b!"));
            assertThat(keyValueStore.get("C"), is("C->null!"));
        }
        {
            final KeyValueStore<String, ValueAndTimestamp<String>> keyValueStore = driver.getTimestampedKeyValueStore(QUERYABLE_NAME);
            assertThat(keyValueStore.get("A"), is(ValueAndTimestamp.make("A->a!", 5L)));
            assertThat(keyValueStore.get("B"), is(ValueAndTimestamp.make("B->b!", 10L)));
            assertThat(keyValueStore.get("C"), is(ValueAndTimestamp.make("C->null!", 15L)));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldCalculateCorrectOldValuesIfMaterializedEvenIfStateful(final boolean withHeaders) {
        builder
            .table(INPUT_TOPIC, CONSUMED)
            .processFixedKey(
                new StatefulFixedKeyProcessorSupplier(),
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(QUERYABLE_NAME)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer()))
            .groupBy(toForceSendingOfOldValues(), Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR)
            .mapValues(mapBackToStrings())
            .toStream()
            .process(capture);

        driver = new TopologyTestDriver(builder.build(), props(withHeaders));
        final TestInputTopic<String, String> inputTopic =
            driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

        inputTopic.pipeInput("A", "ignored", 5L);
        inputTopic.pipeInput("A", "ignored1", 15L);
        inputTopic.pipeInput("A", "ignored2", 10L);

        assertThat(output(), equalTo(Arrays.asList(
            new KeyValueTimestamp<>("A", "1", 5),
            new KeyValueTimestamp<>("A", "2", 15),
            new KeyValueTimestamp<>("A", "3", 15)
        )));

        final KeyValueStore<String, Integer> keyValueStore = driver.getKeyValueStore(QUERYABLE_NAME);
        assertThat(keyValueStore.get("A"), is(3));
        assertThat(driver.getAllStateStores().keySet(),
            equalTo(Set.of(QUERYABLE_NAME, "KTABLE-AGGREGATE-STATE-STORE-0000000005")));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldCalculateCorrectOldValuesIfNotStatefulEvenIfNotMaterialized(final boolean withHeaders) {
        builder
            .table(INPUT_TOPIC, CONSUMED)
            .processFixedKey(new StatelessFixedKeyProcessorSupplier())
            .groupBy(toForceSendingOfOldValues(), Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR)
            .mapValues(mapBackToStrings())
            .toStream()
            .process(capture);

        driver = new TopologyTestDriver(builder.build(), props(withHeaders));
        final TestInputTopic<String, String> inputTopic =
            driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

        inputTopic.pipeInput("A", "a", 5L);
        inputTopic.pipeInput("A", "aa", 15L);
        inputTopic.pipeInput("A", "aaa", 10);

        assertThat(output(), equalTo(Arrays.asList(
            new KeyValueTimestamp<>("A", "1", 5),
            new KeyValueTimestamp<>("A", "2", 15),
            new KeyValueTimestamp<>("A", "3", 15)
        )));
        assertThat(driver.getAllStateStores().keySet(),
            equalTo(Set.of("inputTopic-STATE-STORE-0000000000", "KTABLE-AGGREGATE-STATE-STORE-0000000005")));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldCalculateCorrectOldValuesIfNotStatefulEvenNotMaterializedNoQueryableName(final boolean withHeaders) {
        builder
            .table(INPUT_TOPIC, CONSUMED)
            .processFixedKey(
                new StatelessFixedKeyProcessorSupplier(),
                Materialized.with(Serdes.String(), Serdes.Integer()))
            .groupBy(toForceSendingOfOldValues(), Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR)
            .mapValues(mapBackToStrings())
            .toStream()
            .process(capture);

        driver = new TopologyTestDriver(builder.build(), props(withHeaders));
        final TestInputTopic<String, String> inputTopic =
            driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

        inputTopic.pipeInput("A", "a", 5L);
        inputTopic.pipeInput("A", "aa", 15L);
        inputTopic.pipeInput("A", "aaa", 10);

        assertThat(output(), equalTo(Arrays.asList(
            new KeyValueTimestamp<>("A", "1", 5),
            new KeyValueTimestamp<>("A", "2", 15),
            new KeyValueTimestamp<>("A", "3", 15)
        )));
        assertThat(driver.getAllStateStores().keySet(),
            equalTo(Set.of("inputTopic-STATE-STORE-0000000000", "KTABLE-AGGREGATE-STATE-STORE-0000000005")));
    }

    private ArrayList<KeyValueTimestamp<String, String>> output() {
        return capture.capturedProcessors(1).get(0).processed();
    }

    private static KeyValueMapper<String, Integer, KeyValue<String, Integer>> toForceSendingOfOldValues() {
        return KeyValue::new;
    }

    private static ValueMapper<Integer, String> mapBackToStrings() {
        return Object::toString;
    }

    private static StoreBuilder<KeyValueStore<Long, Long>> storeBuilder(final String storeName) {
        return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long());
    }

    public static Properties props(final boolean withHeaders) {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        return props;
    }

    private static void throwIfStoresNotAvailable(final FixedKeyProcessorContext<?, ?> context,
                                                  final List<String> expectedStoredNames) {
        final List<String> missing = new ArrayList<>();
        for (final String storeName : expectedStoredNames) {
            if (context.getStateStore(storeName) == null) {
                missing.add(storeName);
            }
        }
        if (!missing.isEmpty()) {
            throw new AssertionError("State stores are not accessible: " + missing);
        }
    }

    public static class ExclamationFixedKeyProcessorSupplier
        implements FixedKeyProcessorSupplier<Object, String, String> {

        private final List<String> expectedStoredNames;

        ExclamationFixedKeyProcessorSupplier(final String... expectedStoreNames) {
            this.expectedStoredNames = Arrays.asList(expectedStoreNames);
        }

        @Override
        public FixedKeyProcessor<Object, String, String> get() {
            return new ExclamationFixedKeyProcessor(expectedStoredNames);
        }
    }

    public static class ExclamationFixedKeyProcessor implements FixedKeyProcessor<Object, String, String> {
        private final List<String> expectedStoredNames;
        private FixedKeyProcessorContext<Object, String> context;

        ExclamationFixedKeyProcessor(final List<String> expectedStoredNames) {
            this.expectedStoredNames = expectedStoredNames;
        }

        @Override
        public void init(final FixedKeyProcessorContext<Object, String> context) {
            this.context = context;
            throwIfStoresNotAvailable(context, expectedStoredNames);
        }

        @Override
        public void process(final FixedKeyRecord<Object, String> record) {
            context.forward(record.withValue(record.key() + "->" + record.value() + "!"));
        }

        @Override
        public void close() {}
    }

    private static class NullFixedKeyProcessorSupplier
        implements FixedKeyProcessorSupplier<String, String, String> {
        @Override
        public FixedKeyProcessor<String, String, String> get() {
            return null;
        }
    }

    private static class StatefulFixedKeyProcessorSupplier
        implements FixedKeyProcessorSupplier<String, String, Integer> {
        @Override
        public FixedKeyProcessor<String, String, Integer> get() {
            return new StatefulFixedKeyProcessor();
        }
    }

    private static class StatefulFixedKeyProcessor implements FixedKeyProcessor<String, String, Integer> {
        private int counter;
        private FixedKeyProcessorContext<String, Integer> context;

        @Override
        public void init(final FixedKeyProcessorContext<String, Integer> context) {
            this.context = context;
        }

        @Override
        public void process(final FixedKeyRecord<String, String> record) {
            context.forward(record.withValue(++counter));
        }

        @Override
        public void close() {}
    }

    private static class StatelessFixedKeyProcessorSupplier
        implements FixedKeyProcessorSupplier<String, String, Integer> {
        @Override
        public FixedKeyProcessor<String, String, Integer> get() {
            return new StatelessFixedKeyProcessor();
        }
    }

    private static class StatelessFixedKeyProcessor implements FixedKeyProcessor<String, String, Integer> {
        private FixedKeyProcessorContext<String, Integer> context;

        @Override
        public void init(final FixedKeyProcessorContext<String, Integer> context) {
            this.context = context;
        }

        @Override
        public void process(final FixedKeyRecord<String, String> record) {
            final String value = record.value();
            context.forward(record.withValue(value == null ? null : value.length()));
        }

        @Override
        public void close() {}
    }
}
