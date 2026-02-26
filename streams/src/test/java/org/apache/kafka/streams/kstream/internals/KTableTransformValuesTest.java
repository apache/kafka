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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.KeyValueTimestampHeaders;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.NoOpValueTransformerWithKeySupplier;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
public class KTableTransformValuesTest {
    private static final String QUERYABLE_NAME = "queryable-store";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String STORE_NAME = "someStore";
    private static final String OTHER_STORE_NAME = "otherStore";

    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());

    private TopologyTestDriver driver;
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
    private TimestampedKeyValueStore<String, String> stateStore;
    @Mock
    private ValueTransformerWithKeySupplier<String, String, String> mockSupplier;
    @Mock
    private ValueTransformerWithKey<String, String, String> transformer;

    @AfterEach
    public void cleanup() {
        if (driver != null) {
            driver.close();
            driver = null;
        }
    }

    @BeforeEach
    public void setUp() {
        builder = new StreamsBuilder();
    }

    /**
     * Provides test parameters for different store formats.
     */
    private static Stream<Arguments> storeFormats() {
        return Stream.of(
            Arguments.of("default"),
            Arguments.of("headers")
        );
    }

    private Properties getProps(final String storeFormat) {
        final Properties properties = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
        properties.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        properties.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, storeFormat);
        return properties;
    }

    private static Headers makeHeaders(final String key, final String value) {
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }

    @Test
    public void shouldThrowOnGetIfSupplierReturnsNull() {
        final KTableTransformValues<String, String, String> transformer =
            new KTableTransformValues<>(parent, new NullSupplier(), QUERYABLE_NAME);

        try {
            transformer.get();
            fail("NPE expected");
        } catch (final NullPointerException expected) {
            // expected
        }
    }

    @Test
    public void shouldThrowOnViewGetIfSupplierReturnsNull() {
        final KTableValueGetterSupplier<String, String> view =
            new KTableTransformValues<>(parent, new NullSupplier(), null).view();

        try {
            view.get();
            fail("NPE expected");
        } catch (final NullPointerException expected) {
            // expected
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldInitializeTransformerWithForwardDisabledProcessorContext() {
        final NoOpValueTransformerWithKeySupplier<String, String> transformer = new NoOpValueTransformerWithKeySupplier<>();
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, transformer, null);
        final Processor<String, Change<String>, String, Change<String>> processor = transformValues.get();

        processor.init(context);

        assertThat(transformer.context, isA((Class) ForwardingDisabledProcessorContext.class));
    }

    @Test
    public void shouldNotSendOldValuesByDefault() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

        final Processor<String, Change<String>, String, Change<String>> processor = transformValues.get();
        processor.init(context);

        doNothing().when(context).forward(new Record<>("Key", new Change<>("Key->newValue!", null), 0));

        processor.process(new Record<>("Key", new Change<>("newValue", "oldValue"), 0));
    }

    @Test
    public void shouldSendOldValuesIfConfigured() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

        when(parent.enableSendingOldValues(true)).thenReturn(true);

        transformValues.enableSendingOldValues(true);
        final Processor<String, Change<String>, String, Change<String>> processor = transformValues.get();
        processor.init(context);

        doNothing().when(context).forward(new Record<>("Key", new Change<>("Key->newValue!", "Key->oldValue!"), 0));

        processor.process(new Record<>("Key", new Change<>("newValue", "oldValue"), 0));
    }

    @Test
    public void shouldNotSetSendOldValuesOnParentIfMaterialized() {
        new KTableTransformValues<>(parent, new NoOpValueTransformerWithKeySupplier<>(), QUERYABLE_NAME).enableSendingOldValues(true);

        verify(parent, never()).enableSendingOldValues(anyBoolean());
    }

    @Test
    public void shouldSetSendOldValuesOnParentIfNotMaterialized() {
        when(parent.enableSendingOldValues(true)).thenReturn(true);

        new KTableTransformValues<>(parent, new NoOpValueTransformerWithKeySupplier<>(), null).enableSendingOldValues(true);
    }

    @Test
    public void shouldTransformOnGetIfNotMaterialized() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);
        when(parentGetterSupplier.get()).thenReturn(parentGetter);
        when(parentGetter.get("Key")).thenReturn(ValueAndTimestamp.make("Value", 73L));
        final ProcessorRecordContext recordContext = new ProcessorRecordContext(
            42L,
            23L,
            -1,
            "foo",
            new RecordHeaders()
        );
        when(context.recordContext()).thenReturn(recordContext);
        doNothing().when(context).setRecordContext(new ProcessorRecordContext(
            73L,
            -1L,
            -1,
            null,
            new RecordHeaders()
        ));
        doNothing().when(context).setRecordContext(recordContext);

        final KTableValueGetter<String, String> getter = transformValues.view().get();
        getter.init(context);

        final String result = getter.get("Key").value();

        assertThat(result, is("Key->Value!"));
    }

    @Test
    public void shouldGetFromStateStoreIfMaterialized() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), QUERYABLE_NAME);

        when(context.getStateStore(QUERYABLE_NAME)).thenReturn(stateStore);
        when(stateStore.get("Key")).thenReturn(ValueAndTimestamp.make("something", 0L));

        final KTableValueGetter<String, String> getter = transformValues.view().get();
        getter.init(context);

        final String result = getter.get("Key").value();

        assertThat(result, is("something"));
    }

    @Test
    public void shouldGetStoreNamesFromParentIfNotMaterialized() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);
        when(parentGetterSupplier.storeNames()).thenReturn(new String[]{"store1", "store2"});

        final String[] storeNames = transformValues.view().storeNames();

        assertThat(storeNames, is(new String[]{"store1", "store2"}));
    }

    @Test
    public void shouldGetQueryableStoreNameIfMaterialized() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), QUERYABLE_NAME);

        final String[] storeNames = transformValues.view().storeNames();

        assertThat(storeNames, is(new String[]{QUERYABLE_NAME}));
    }

    @Test
    public void shouldCloseTransformerOnProcessorClose() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, mockSupplier, null);

        when(mockSupplier.get()).thenReturn(transformer);
        doNothing().when(transformer).close();

        final Processor<String, Change<String>, String, Change<String>> processor = transformValues.get();
        processor.close();
    }

    @Test
    public void shouldCloseTransformerOnGetterClose() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, mockSupplier, null);

        when(mockSupplier.get()).thenReturn(transformer);
        when(parentGetterSupplier.get()).thenReturn(parentGetter);
        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);

        doNothing().when(transformer).close();

        final KTableValueGetter<String, String> getter = transformValues.view().get();
        getter.close();
    }

    @Test
    public void shouldCloseParentGetterClose() {
        final KTableTransformValues<String, String, String> transformValues =
            new KTableTransformValues<>(parent, mockSupplier, null);

        when(parent.valueGetterSupplier()).thenReturn(parentGetterSupplier);
        when(mockSupplier.get()).thenReturn(transformer);
        when(parentGetterSupplier.get()).thenReturn(parentGetter);
        doNothing().when(parentGetter).close();

        final KTableValueGetter<String, String> getter = transformValues.view().get();
        getter.close();
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldTransformValuesWithKey(final String storeFormat) {
        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersD = makeHeaders("key", "D");

        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        builder
            .addStateStore(storeBuilder(STORE_NAME))
            .addStateStore(storeBuilder(OTHER_STORE_NAME))
            .table(INPUT_TOPIC, CONSUMED)
            .transformValues(
                new ExclamationValueTransformerSupplier(STORE_NAME, OTHER_STORE_NAME),
                STORE_NAME, OTHER_STORE_NAME)
            .toStream()
            .process(supplier);

        driver = new TopologyTestDriver(builder.build(), getProps(storeFormat));
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        inputTopic.pipeInput(new TestRecord<>("A", "a", headersA, 5L));
        inputTopic.pipeInput(new TestRecord<>("B", "b", headersB, 10L));
        inputTopic.pipeInput(new TestRecord<>("D", null, headersD, 15L));

        if (storeFormat.equals("default")) {
            supplier.theCapturedProcessor().checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", "A->a!", 5),
                    new KeyValueTimestamp<>("B", "B->b!", 10),
                    new KeyValueTimestamp<>("D", "D->null!", 15));
        } else if (storeFormat.equals("headers")) {
            supplier.theCapturedProcessor().checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", "A->a!", 5, headersA),
                    new KeyValueTimestampHeaders<>("B", "B->b!", 10, headersB),
                    new KeyValueTimestampHeaders<>("D", "D->null!", 15, headersD));
        }
        assertNull(driver.getKeyValueStore(QUERYABLE_NAME), "Store should not be materialized");
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldTransformValuesWithKeyAndMaterialize(final String storeFormat) {
        final Headers headersA = makeHeaders("key", "A");
        final Headers headersB = makeHeaders("key", "B");
        final Headers headersC = makeHeaders("key", "C");

        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        builder
            .addStateStore(storeBuilder(STORE_NAME))
            .table(INPUT_TOPIC, CONSUMED)
            .transformValues(
                new ExclamationValueTransformerSupplier(STORE_NAME, QUERYABLE_NAME),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(QUERYABLE_NAME)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String()),
                STORE_NAME)
            .toStream()
            .process(supplier);

        driver = new TopologyTestDriver(builder.build(), getProps(storeFormat));
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTopic.pipeInput(new TestRecord<>("A", "a", headersA, 5L));
        inputTopic.pipeInput(new TestRecord<>("B", "b", headersB, 10L));
        inputTopic.pipeInput(new TestRecord<>("C", null, headersC, 15L));

        if (storeFormat.equals("default")) {
            supplier.theCapturedProcessor().checkAndClearProcessResult(
                    new KeyValueTimestamp<>("A", "A->a!", 5),
                    new KeyValueTimestamp<>("B", "B->b!", 10),
                    new KeyValueTimestamp<>("C", "C->null!", 15));

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
        } else if (storeFormat.equals("headers")) {
            supplier.theCapturedProcessor().checkAndClearProcessResultWithHeaders(
                    new KeyValueTimestampHeaders<>("A", "A->a!", 5, headersA),
                    new KeyValueTimestampHeaders<>("B", "B->b!", 10, headersB),
                    new KeyValueTimestampHeaders<>("C", "C->null!", 15, headersC));
        }
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldCalculateCorrectOldValuesIfMaterializedEvenIfStateful(final String storeFormat) {
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        builder
            .table(INPUT_TOPIC, CONSUMED)
            .transformValues(
                new StatefulTransformerSupplier(),
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(QUERYABLE_NAME)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer()))
            .groupBy(toForceSendingOfOldValues(), Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR)
            .mapValues(mapBackToStrings())
            .toStream()
            .process(supplier);

        driver = new TopologyTestDriver(builder.build(), getProps(storeFormat));
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        inputTopic.pipeInput("A", "ignored", 5L);
        inputTopic.pipeInput("A", "ignored1", 15L);
        inputTopic.pipeInput("A", "ignored2", 10L);

        supplier.theCapturedProcessor().checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", "1", 5),
                new KeyValueTimestamp<>("A", "2", 15),
                new KeyValueTimestamp<>("A", "3", 15));

        if (storeFormat.equals("default")) {
            final KeyValueStore<String, Integer> keyValueStore = driver.getKeyValueStore(QUERYABLE_NAME);
            assertThat(keyValueStore.get("A"), is(3));
        }
        assertThat(driver.getAllStateStores().keySet(),
            equalTo(Set.of(QUERYABLE_NAME, "KTABLE-AGGREGATE-STATE-STORE-0000000005")));
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldCalculateCorrectOldValuesIfNotStatefulEvenIfNotMaterialized(final String storeFormat) {
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        builder
            .table(INPUT_TOPIC, CONSUMED)
            .transformValues(new StatelessTransformerSupplier())
            .groupBy(toForceSendingOfOldValues(), Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR)
            .mapValues(mapBackToStrings())
            .toStream()
            .process(supplier);

        driver = new TopologyTestDriver(builder.build(), getProps(storeFormat));
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        inputTopic.pipeInput("A", "a", 5L);
        inputTopic.pipeInput("A", "aa", 15L);
        inputTopic.pipeInput("A", "aaa", 10);

        supplier.theCapturedProcessor().checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", "1", 5),
                new KeyValueTimestamp<>("A", "2", 15),
                new KeyValueTimestamp<>("A", "3", 15));
        assertThat(driver.getAllStateStores().keySet(),
            equalTo(Set.of("inputTopic-STATE-STORE-0000000000", "KTABLE-AGGREGATE-STATE-STORE-0000000005")));
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldCalculateCorrectOldValuesIfNotStatefulEvenNotMaterializedNoQueryableName(final String storeFormat) {
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        builder
            .table(INPUT_TOPIC, CONSUMED)
            .transformValues(new StatelessTransformerSupplier(),
                Materialized.with(Serdes.String(), Serdes.Integer())
            )
            .groupBy(toForceSendingOfOldValues(), Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(MockReducer.INTEGER_ADDER, MockReducer.INTEGER_SUBTRACTOR)
            .mapValues(mapBackToStrings())
            .toStream()
            .process(supplier);

        driver = new TopologyTestDriver(builder.build(), getProps(storeFormat));
        final TestInputTopic<String, String> inputTopic =
            driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        inputTopic.pipeInput("A", "a", 5L);
        inputTopic.pipeInput("A", "aa", 15L);
        inputTopic.pipeInput("A", "aaa", 10);

        supplier.theCapturedProcessor().checkAndClearProcessResult(
                new KeyValueTimestamp<>("A", "1", 5),
                new KeyValueTimestamp<>("A", "2", 15),
                new KeyValueTimestamp<>("A", "3", 15));
        assertThat(driver.getAllStateStores().keySet(),
            equalTo(Set.of("inputTopic-STATE-STORE-0000000000", "KTABLE-AGGREGATE-STATE-STORE-0000000005")));
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

    private static void throwIfStoresNotAvailable(final ProcessorContext context,
                                                  final List<String> expectedStoredNames) {
        final List<String> missing = new ArrayList<>();

        for (final String storedName : expectedStoredNames) {
            if (context.getStateStore(storedName) == null) {
                missing.add(storedName);
            }
        }

        if (!missing.isEmpty()) {
            throw new AssertionError("State stores are not accessible: " + missing);
        }
    }

    public static class ExclamationValueTransformerSupplier implements ValueTransformerWithKeySupplier<Object, String, String> {
        private final List<String> expectedStoredNames;

        ExclamationValueTransformerSupplier(final String... expectedStoreNames) {
            this.expectedStoredNames = Arrays.asList(expectedStoreNames);
        }

        @Override
        public ExclamationValueTransformer get() {
            return new ExclamationValueTransformer(expectedStoredNames);
        }
    }

    public static class ExclamationValueTransformer implements ValueTransformerWithKey<Object, String, String> {
        private final List<String> expectedStoredNames;

        ExclamationValueTransformer(final List<String> expectedStoredNames) {
            this.expectedStoredNames = expectedStoredNames;
        }

        @Override
        public void init(final ProcessorContext context) {
            throwIfStoresNotAvailable(context, expectedStoredNames);
        }

        @Override
        public String transform(final Object readOnlyKey, final String value) {
            return readOnlyKey.toString() + "->" + value + "!";
        }

        @Override
        public void close() {}
    }

    private static class NullSupplier implements ValueTransformerWithKeySupplier<String, String, String> {
        @Override
        public ValueTransformerWithKey<String, String, String> get() {
            return null;
        }
    }

    private static class StatefulTransformerSupplier implements ValueTransformerWithKeySupplier<String, String, Integer> {
        @Override
        public ValueTransformerWithKey<String, String, Integer> get() {
            return new StatefulTransformer();
        }
    }

    private static class StatefulTransformer implements ValueTransformerWithKey<String, String, Integer> {
        private int counter;

        @Override
        public void init(final ProcessorContext context) {}

        @Override
        public Integer transform(final String readOnlyKey, final String value) {
            return ++counter;
        }

        @Override
        public void close() {}
    }

    private static class StatelessTransformerSupplier implements ValueTransformerWithKeySupplier<String, String, Integer> {
        @Override
        public ValueTransformerWithKey<String, String, Integer> get() {
            return new StatelessTransformer();
        }
    }

    private static class StatelessTransformer implements ValueTransformerWithKey<String, String, Integer> {
        @Override
        public void init(final ProcessorContext context) {}

        @Override
        public Integer transform(final String readOnlyKey, final String value) {
            return value == null ? null : value.length();
        }

        @Override
        public void close() {}
    }
}
