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


import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class RocksDBStoreTest {

    private RocksDBStore<String, Long> rocksDBStore;
    private MockProcessorContext context;
    private File dir;

    @Before
    public void before() {
        rocksDBStore = new RocksDBStore<String, Long>("rocksdb-store", Serdes.String(), Serdes.Long());
        dir = TestUtils.tempDirectory();
        context = new MockProcessorContext(dir,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));

    }

    @After
    public void close() {
        rocksDBStore.close();
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnOpeningReadOnlyDir() throws IOException {
        final File tmpDir = TestUtils.tempDirectory();
        MockProcessorContext tmpContext = new MockProcessorContext(tmpDir,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));
        tmpDir.setReadOnly();

        rocksDBStore.openDB(tmpContext);
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExeptionOnPutDeletedDir() throws IOException {
        rocksDBStore.init(context, rocksDBStore);
        Utils.delete(dir);
        rocksDBStore.put("anyKey", 2L);
        rocksDBStore.flush();
    }

}
