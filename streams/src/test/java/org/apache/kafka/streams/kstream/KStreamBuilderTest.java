/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamBuilderTest {

    private KStreamTestDriver driver = null;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test(expected = TopologyBuilderException.class)
    public void testFrom() {
        final KStreamBuilder builder = new KStreamBuilder();

        builder.stream("topic-1", "topic-2");

        builder.addSource(KStreamImpl.SOURCE_NAME + "0000000000", "topic-3");
    }

    @Test
    public void testNewName() {
        KStreamBuilder builder = new KStreamBuilder();

        assertEquals("X-0000000000", builder.newName("X-"));
        assertEquals("Y-0000000001", builder.newName("Y-"));
        assertEquals("Z-0000000002", builder.newName("Z-"));

        builder = new KStreamBuilder();

        assertEquals("X-0000000000", builder.newName("X-"));
        assertEquals("Y-0000000001", builder.newName("Y-"));
        assertEquals("Z-0000000002", builder.newName("Z-"));
    }

    @Test
    public void testMerge() {
        String topic1 = "topic-1";
        String topic2 = "topic-2";

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source1 = builder.stream(topic1);
        KStream<String, String> source2 = builder.stream(topic2);
        KStream<String, String> merged = builder.merge(source1, source2);

        MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        merged.process(processorSupplier);

        driver = new KStreamTestDriver(builder);
        driver.setTime(0L);

        driver.process(topic1, "A", "aa");
        driver.process(topic2, "B", "bb");
        driver.process(topic2, "C", "cc");
        driver.process(topic1, "D", "dd");

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd"), processorSupplier.processed);
    }

}
