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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.NoOpInternalValueTransformer;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(EasyMockRunner.class)
public class KTableTransformValuesTest {
    private static final String QUERYABLE_NAME = "queryable-store";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String STORE_NAME = "someStore";
    private static final String OTHER_STORE_NAME = "otherStore";

    private static final Consumed<String, String> CONSUMED = Consumed.with(Serdes.String(), Serdes.String());

    private final ConsumerRecordFactory<String, String> recordFactory
            = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    private TopologyTestDriver driver;
    private MockProcessorSupplier<String, String> capture;
    private StreamsBuilder builder;
    @Mock(MockType.NICE)
    private KTableImpl<String, String, String> parent;
    @Mock(MockType.NICE)
    private InternalProcessorContext context;
    @Mock(MockType.NICE)
    private KTableValueGetterSupplier<String, String> parentGetterSupplier;
    @Mock(MockType.NICE)
    private KTableValueGetter<String, String> parentGetter;
    @Mock(MockType.NICE)
    private KeyValueStore<String, String> stateStore;
    @Mock(MockType.NICE)
    private ValueTransformerWithKeySupplier<String, String, String> mockSupplier;
    @Mock(MockType.NICE)
    private ValueTransformerWithKey<String, String, String> transformer;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Before
    public void setUp() {
        capture = new MockProcessorSupplier<>();
        builder = new StreamsBuilder();
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowOnGetIfSupplierReturnsNull() {
        new KTableTransformValues<>(parent, new NullSupplier(), QUERYABLE_NAME).get();
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowOnViewGetIfSupplierReturnsNull() {
        new KTableTransformValues<>(parent, new NullSupplier(), null).view().get();
    }

    @Test
    public void shouldInitializeTransformerWithForwardDisabledProcessorContext() {
        final NoOpInternalValueTransformer<String, String> transformer = new NoOpInternalValueTransformer<>();
        final KTableTransformValues<String, String, String> transformValues = new KTableTransformValues<>(parent, transformer, null);
        final Processor<String, Change<String>> processor = transformValues.get();

        processor.init(context);

        assertThat(transformer.context, isA((Class) ForwardingDisabledProcessorContext.class));
    }

    @Test
    public void shouldNotSendOldValuesByDefault() {
        final KTableTransformValues<String, String, String> transformValues =
                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

        final Processor<String, Change<String>> processor = transformValues.get();
        processor.init(context);

        context.forward("Key", new Change<>("Key->newValue!", null));
        expectLastCall();
        replay(context);

        processor.process("Key", new Change<>("newValue", "oldValue"));

        verify(context);
    }

    @Test
    public void shouldSendOldValuesIfConfigured() {
        final KTableTransformValues<String, String, String> transformValues =
                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

        transformValues.enableSendingOldValues();
        final Processor<String, Change<String>> processor = transformValues.get();
        processor.init(context);

        context.forward("Key", new Change<>("Key->newValue!", "Key->oldValue!"));
        expectLastCall();
        replay(context);

        processor.process("Key", new Change<>("newValue", "oldValue"));

        verify(context);
    }

    @Test
    public void shouldSetSendOldValuesOnParent() {
        parent.enableSendingOldValues();
        expectLastCall();
        replay(parent);

        new KTableTransformValues<>(parent, new NoOpInternalValueTransformer<>(), QUERYABLE_NAME).enableSendingOldValues();

        verify(parent);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldTransformOnGetIfNotMaterialized() {
        final KTableTransformValues<String, String, String> transformValues =
                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

        expect(parent.valueGetterSupplier()).andReturn(parentGetterSupplier);
        expect(parentGetterSupplier.get()).andReturn(parentGetter);
        expect(parentGetter.get("Key")).andReturn("Value");
        replay(parent, parentGetterSupplier, parentGetter);

        final KTableValueGetter<String, String> getter = transformValues.view().get();
        getter.init(context);

        final String result = getter.get("Key");

        assertThat(result, is("Key->Value!"));
    }

    @Test
    public void shouldGetFromStateStoreIfMaterialized() {
        final KTableTransformValues<String, String, String> transformValues =
                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), QUERYABLE_NAME);

        expect(context.getStateStore(QUERYABLE_NAME)).andReturn(stateStore);
        expect(stateStore.get("Key")).andReturn("something");
        replay(context, stateStore);

        final KTableValueGetter<String, String> getter = transformValues.view().get();
        getter.init(context);

        final String result = getter.get("Key");

        assertThat(result, is("something"));
    }

    @Test
    public void shouldGetStoreNamesFromParentIfNotMaterialized() {
        final KTableTransformValues<String, String, String> transformValues =
                new KTableTransformValues<>(parent, new ExclamationValueTransformerSupplier(), null);

        expect(parent.valueGetterSupplier()).andReturn(parentGetterSupplier);
        expect(parentGetterSupplier.storeNames()).andReturn(new String[]{"store1", "store2"});
        replay(parent, parentGetterSupplier);

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

        expect(mockSupplier.get()).andReturn(transformer);
        transformer.close();
        expectLastCall();
        replay(mockSupplier, transformer);

        final Processor<String, Change<String>> processor = transformValues.get();
        processor.close();

        verify(transformer);
    }

    @Test
    public void shouldTransformValuesWithKey() {
        builder.addStateStore(storeBuilder(STORE_NAME))
                .addStateStore(storeBuilder(OTHER_STORE_NAME))
                .table(INPUT_TOPIC, CONSUMED)
                .transformValues(
                        new ExclamationValueTransformerSupplier(STORE_NAME, OTHER_STORE_NAME),
                        STORE_NAME, OTHER_STORE_NAME)
                .toStream()
                .process(capture);

        driver = new TopologyTestDriver(builder.build(), props());

        driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "a", 0L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC, "B", "b", 0L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC, "D", null, 0L));

        assertThat(output(), hasItems("A:A->a!", "B:B->b!", "D:null"));
    }

    @Test
    public void shouldTransformValuesWithKeyAndMaterialize() {
        builder.addStateStore(storeBuilder(STORE_NAME))
                .table(INPUT_TOPIC, CONSUMED)
                .transformValues(
                        new ExclamationValueTransformerSupplier(STORE_NAME, QUERYABLE_NAME),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(QUERYABLE_NAME)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()),
                        STORE_NAME)
                .toStream()
                .process(capture);

        driver = new TopologyTestDriver(builder.build(), props());

        driver.pipeInput(recordFactory.create(INPUT_TOPIC, "A", "a", 0L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC, "B", "b", 0L));
        driver.pipeInput(recordFactory.create(INPUT_TOPIC, "C", null, 0L));

        assertThat(output(), hasItems("A:A->a!", "B:B->b!", "C:null"));

        final KeyValueStore<String, String> keyValueStore = driver.getKeyValueStore(QUERYABLE_NAME);
        assertThat(keyValueStore.get("A"), is("A->a!"));
        assertThat(keyValueStore.get("B"), is("B->b!"));
        assertThat(keyValueStore.get("C"), is(nullValue()));
    }

    private ArrayList<String> output() {
        return capture.capturedProcessors(1).get(0).processed;
    }

    private static StoreBuilder<KeyValueStore<Long, Long>> storeBuilder(final String storeName) {
        return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long());
    }

    public static Properties props() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-transform-values-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        return props;
    }

    private static void throwIfStoresNotAvailable(final ProcessorContext context,
                                                  final List<String> expectedStoredNames) {
        final List<String> missing = new ArrayList<>();

        for (String storedName : expectedStoredNames) {
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
        public void close() {
        }
    }

    private static class NullSupplier implements ValueTransformerWithKeySupplier<String, String, String> {
        @Override
        public ValueTransformerWithKey<String, String, String> get() {
            return null;
        }
    }
}