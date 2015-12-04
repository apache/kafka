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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KTableSourceTest {

    private final Serializer<String> strSerializer = new StringSerializer();
    private final Deserializer<String> strDeserializer = new StringDeserializer();

    @Test
    public void testKTable() {
        final KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";

        KTable<String, String> table1 = builder.table(strSerializer, strSerializer, strDeserializer, strDeserializer, topic1);

        MockProcessorSupplier<String, String> proc1 = new MockProcessorSupplier<>();
        table1.toStream().process(proc1);

        KStreamTestDriver driver = new KStreamTestDriver(builder);

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 2);
        driver.process(topic1, "C", 3);
        driver.process(topic1, "D", 4);
        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);

        assertEquals(Utils.mkList("A:1", "B:2", "C:3", "D:4", "A:null", "B:null"), proc1.processed);
    }

    @Test
    public void testValueGetter() throws IOException {
        File stateDir = Files.createTempDirectory("test").toFile();
        try {
            final KStreamBuilder builder = new KStreamBuilder();

            String topic1 = "topic1";

            KTableImpl<String, String, String> table1 = (KTableImpl<String, String, String>)
                    builder.table(strSerializer, strSerializer, strDeserializer, strDeserializer, topic1);

            KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();

            KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir, null, null, null, null);

            KTableValueGetter<String, String> getter1 = getterSupplier1.get();
            getter1.init(driver.context());

            driver.process(topic1, "A", "01");
            driver.process(topic1, "B", "01");
            driver.process(topic1, "C", "01");

            assertEquals("01", getter1.get("A"));
            assertEquals("01", getter1.get("B"));
            assertEquals("01", getter1.get("C"));

            driver.process(topic1, "A", "02");
            driver.process(topic1, "B", "02");

            assertEquals("02", getter1.get("A"));
            assertEquals("02", getter1.get("B"));
            assertEquals("01", getter1.get("C"));

            driver.process(topic1, "A", "03");

            assertEquals("03", getter1.get("A"));
            assertEquals("02", getter1.get("B"));
            assertEquals("01", getter1.get("C"));

            driver.process(topic1, "A", null);
            driver.process(topic1, "B", null);

            assertNull(getter1.get("A"));
            assertNull(getter1.get("B"));
            assertEquals("01", getter1.get("C"));

        } finally {
            Utils.delete(stateDir);
        }
    }

}
