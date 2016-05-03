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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class KTableAggregateTest {

    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver = null;
    private File stateDir = null;

    @After
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testAggBasic() throws Exception {
        final KStreamBuilder builder = new KStreamBuilder();
        String topic1 = "topic1";

        KTable<String, String> table1 = builder.table(stringSerde, stringSerde, topic1);
        KTable<String, String> table2 = table1.groupBy(MockKeyValueMapper.<String, String>NoOpKeyValueMapper(),
                stringSerde,
                stringSerde
        ).aggregate(MockInitializer.STRING_INIT,
                MockAggregator.STRING_ADDER,
                MockAggregator.STRING_REMOVER,
                stringSerde,
                "topic1-Canonized");

        MockProcessorSupplier<String, String> proc2 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);

        driver = new KStreamTestDriver(builder, stateDir);

        driver.process(topic1, "A", "1");
        driver.process(topic1, "B", "2");
        driver.process(topic1, "A", "3");
        driver.process(topic1, "B", "4");
        driver.process(topic1, "C", "5");
        driver.process(topic1, "D", "6");
        driver.process(topic1, "B", "7");
        driver.process(topic1, "C", "8");

        assertEquals(Utils.mkList(
                "A:0+1",
                "B:0+2",
                "A:0+1+3", "A:0+1+3-1",
                "B:0+2+4", "B:0+2+4-2",
                "C:0+5",
                "D:0+6",
                "B:0+2+4-2+7", "B:0+2+4-2+7-4",
                "C:0+5+8", "C:0+5+8-5"), proc2.processed);
    }
}
