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
package org.apache.kafka.streams.state.internals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.KeyFirstSessionKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedSessionKeySchemas.TimeFirstSessionKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.SegmentedBytesStore.KeySchema;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RocksDBTimeOrderedWindowSegmentedBytesStoreTest
    extends AbstractDualSchemaRocksDBSegmentedBytesStoreTest<Segment> {

    private final static String METRICS_SCOPE = "metrics-scope";

    private enum SchemaType {
        WindowSchemaWithIndex,
        WindowSchemaWithoutIndex,
        SessionSchemaWithIndex,
        SessionSchemaWithoutIndex
    }

    private boolean hasIndex;
    private SchemaType schemaType;

    private RocksDBTransactionalMechanism txnMechanism;

    @Parameterized.Parameters(name = "{0} {1} {2}")
    public static Collection<Object[]> data() {
        final Object[][] schemaTypes = new Object[][] {
            {SchemaType.WindowSchemaWithIndex, true},
            {SchemaType.WindowSchemaWithoutIndex, false},
            {SchemaType.SessionSchemaWithIndex, true},
            {SchemaType.SessionSchemaWithoutIndex, false}};

        final List<Object[]> data = new ArrayList<>();
        for (final Object[] obj : schemaTypes) {
            data.add(new Object[] {obj[0], obj[1], RocksDBTransactionalMechanism.SECONDARY_STORE});
            data.add(new Object[] {obj[0], obj[1], null});
        }
        return data;
    }

    public RocksDBTimeOrderedWindowSegmentedBytesStoreTest(
        final SchemaType schemaType,
        final boolean hasIndex,
        final RocksDBTransactionalMechanism txnMechanism) {
        this.schemaType = schemaType;
        this.hasIndex = hasIndex;
        this.txnMechanism = txnMechanism;
    }


    AbstractDualSchemaRocksDBSegmentedBytesStore<Segment> getBytesStore() {
        switch (schemaType) {
            case WindowSchemaWithIndex:
            case WindowSchemaWithoutIndex:
                return new RocksDBTimeOrderedWindowSegmentedBytesStore(
                    storeName,
                    METRICS_SCOPE,
                    retention,
                    segmentInterval,
                    hasIndex,
                    txnMechanism
                );
            case SessionSchemaWithIndex:
            case SessionSchemaWithoutIndex:
                return new RocksDBTimeOrderedSessionSegmentedBytesStore(
                    storeName,
                    METRICS_SCOPE,
                    retention,
                    segmentInterval,
                    hasIndex,
                    txnMechanism
                );
            default:
                throw new IllegalStateException("Unknown SchemaType: " + schemaType);
        }
    }

    @Override
    KeyValueSegments newSegments() {
        return new KeyValueSegments(storeName, METRICS_SCOPE, retention, segmentInterval, txnMechanism);
    }

    @Override
    KeySchema getBaseSchema() {
        switch (schemaType) {
            case WindowSchemaWithIndex:
            case WindowSchemaWithoutIndex:
                return new TimeFirstWindowKeySchema();
            case SessionSchemaWithIndex:
            case SessionSchemaWithoutIndex:
                return new TimeFirstSessionKeySchema();
            default:
                throw new IllegalStateException("Unknown SchemaType: " + schemaType);
        }
    }

    @Override
    KeySchema getIndexSchema() {
        if (!hasIndex) {
            return null;
        }
        switch (schemaType) {
            case WindowSchemaWithIndex:
                return new KeyFirstWindowKeySchema();
            case SessionSchemaWithIndex:
                return new KeyFirstSessionKeySchema();
            default:
                throw new IllegalStateException("Unknown SchemaType: " + schemaType);
        }
    }

}
