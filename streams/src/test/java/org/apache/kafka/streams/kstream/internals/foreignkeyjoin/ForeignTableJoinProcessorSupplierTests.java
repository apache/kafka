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
import org.apache.kafka.streams.processor.TaskId;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ResponseJoinProcessorSupplierTest.getDroppedRecordsRateMetric;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ResponseJoinProcessorSupplierTest.getDroppedRecordsTotalMetric;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class ForeignTableJoinProcessorSupplierTests {

    private static final Supplier<String> PK_SERDE_TOPIC_SUPPLIER = () -> "pk-topic";
    private static final CombinedKeySchema<String, String> COMBINED_KEY_SCHEMA = new CombinedKeySchema<>(
        () -> "fk-topic",
        Serdes.String(),
        PK_SERDE_TOPIC_SUPPLIER,
        Serdes.String()
    );

    private MockInternalProcessorContext<String, SubscriptionResponseWrapper<String>> context = null;
    private TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>> stateStore = null;
    private Processor<String, Change<String>, String, SubscriptionResponseWrapper<String>> processor = null;
    private File stateDir;

    @BeforeEach
    public void setUp() {
        stateDir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
        context = new MockInternalProcessorContext<>(props, new TaskId(0, 0), stateDir);

        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<String>>> storeBuilder = storeBuilder();
        processor = new ForeignTableJoinProcessorSupplier<String, String, String>(
            StoreBuilderWrapper.wrapStoreBuilder(storeBuilder()),
            COMBINED_KEY_SCHEMA
        ).get();
        stateStore = storeBuilder.build();
        context.addStateStore(stateStore);
        stateStore.init(context, stateStore);
        processor.init(context);
    }

    @AfterEach
    public void cleanUp() throws IOException {
        if (stateStore != null) {
            stateStore.close();
        }
        Utils.delete(stateDir);
    }

    private final String pk1 = "pk1";
    private final String pk2 = "pk2";
    private final String fk1 = "fk1";

    private final long[] hash = new long[]{1L, 2L};

    @Test
    public void shouldPropagateRightRecordForEachMatchingPrimaryKey() {
        putInStore(fk1, pk1);
        putInStore(fk1, pk2);

        final Record<String, Change<String>> record = new Record<>(fk1, new Change<>("new_value", null), 0);

        processor.process(record);

        assertThat(context.forwarded().size(), is(2));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>(pk1, new SubscriptionResponseWrapper<>(hash, "new_value", null), 0))
        );
        assertThat(
            context.forwarded().get(1).record(),
            is(new Record<>(pk2, new SubscriptionResponseWrapper<>(hash, "new_value", null), 0))
        );
    }

    @Test
    public void shouldPropagateNothingIfNoMatchingPrimaryKey() {
        final String fk2 = "fk2";
        putInStore(fk1, pk1);

        final Record<String, Change<String>> record = new Record<>(fk2, new Change<>("new_value", null), 0);

        processor.process(record);

        assertThat(context.forwarded(), empty());
    }

    @Test
    public void shouldPropagateTombstoneRightRecordForEachMatchingPrimaryKey() {
        putInStore(fk1, pk1);
        putInStore(fk1, pk2);

        final Record<String, Change<String>> record = new Record<>(fk1, new Change<>(null, "new_value"), 0);

        processor.process(record);

        assertThat(context.forwarded().size(), is(2));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>(pk1, new SubscriptionResponseWrapper<>(hash, null, null), 0))
        );
        assertThat(
            context.forwarded().get(1).record(),
            is(new Record<>(pk2, new SubscriptionResponseWrapper<>(hash, null, null), 0))
        );
    }

    @Test
    public void shouldNotMatchForeignKeysHavingThisFKAsPrefix() {
        final String fk = "fk";
        putInStore(fk1, pk1);
        putInStore(fk, pk2);

        final Record<String, Change<String>> record = new Record<>(fk, new Change<>("new_value", null), 0);

        processor.process(record);

        assertThat(context.forwarded().size(), is(1));
        assertThat(
            context.forwarded().get(0).record(),
            is(new Record<>(pk2, new SubscriptionResponseWrapper<>(hash, "new_value", null), 0))
        );
    }

    @Test
    public void shouldIgnoreRecordWithNullKey() {
        putInStore(fk1, pk1);

        final Record<String, Change<String>> record = new Record<>(null, new Change<>("new_value", null), 0);

        processor.process(record);

        assertThat(context.forwarded(), empty());

        // test dropped-records sensors
        Assertions.assertEquals(1.0, getDroppedRecordsTotalMetric(context));
        Assertions.assertNotEquals(0.0, getDroppedRecordsRateMetric(context));
    }

    private void putInStore(final String fk, final String pk) {
        final SubscriptionWrapper<String> oldWrapper = new SubscriptionWrapper<>(
            hash,
            SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE,
            pk,
            SubscriptionWrapper.VERSION_0,
            null
        );
        final ValueAndTimestamp<SubscriptionWrapper<String>> oldValue = ValueAndTimestamp.make(oldWrapper, 0);

        final Bytes key = COMBINED_KEY_SCHEMA.toBytes(fk, pk);
        stateStore.put(key, oldValue);
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
