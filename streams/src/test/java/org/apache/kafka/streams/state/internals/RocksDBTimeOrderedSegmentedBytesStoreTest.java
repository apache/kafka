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

import static java.util.Arrays.asList;

import java.util.Collection;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.SegmentedBytesStore.KeySchema;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public class RocksDBTimeOrderedSegmentedBytesStoreTest
    extends AbstractDualSchemaRocksDBSegmentedBytesStoreTest<KeyValueSegment> {

    private final static String METRICS_SCOPE = "metrics-scope";

    @Parameter
    public String name;

    @Parameter(1)
    public boolean hasIndex;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> getKeySchema() {
        return asList(new Object[][] {
            {"WindowSchemaWithIndex", true},
            {"WindowSchemaWithoutIndex", false}
        });
    }

    AbstractDualSchemaRocksDBSegmentedBytesStore<KeyValueSegment> getBytesStore() {
        return new RocksDBTimeOrderedSegmentedBytesStore(
            storeName,
            METRICS_SCOPE,
            retention,
            segmentInterval,
            hasIndex
        );
    }

    @Override
    KeyValueSegments newSegments() {
        return new KeyValueSegments(storeName, METRICS_SCOPE, retention, segmentInterval);
    }

    @Override
    KeySchema getBaseSchema() {
        return new TimeFirstWindowKeySchema();
    }

    @Override
    KeySchema getIndexSchema() {
        return hasIndex ? new KeyFirstWindowKeySchema() : null;
    }

}
