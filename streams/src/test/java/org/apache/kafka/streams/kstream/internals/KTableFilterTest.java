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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KTableFilterTest {

    private final Serializer<String> strSerializer = new StringSerializer();
    private final Deserializer<String> strDeserializer = new StringDeserializer();
    private final Serializer<Integer> intSerializer = new IntegerSerializer();
    private final Deserializer<Integer> intDeserializer = new IntegerDeserializer();

    @Test
    public void testKTable() {
        final KStreamBuilder builder = new KStreamBuilder();

        String topic1 = "topic1";

        KTable<String, Integer> table1 = builder.table(strSerializer, intSerializer, strDeserializer, intDeserializer, topic1);

        KTable<String, Integer> table2 = table1.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return (value % 2) == 0;
            }
        });

        MockProcessorSupplier<String, Integer> proc2 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);

        KStreamTestDriver driver = new KStreamTestDriver(builder);

        driver.process(topic1, "A", 1);
        driver.process(topic1, "B", 2);
        driver.process(topic1, "C", 3);
        driver.process(topic1, "D", 4);
        driver.process(topic1, "A", null);
        driver.process(topic1, "B", null);


        assertEquals(Utils.mkList("A:null", "B:2", "C:null", "D:4", "A:null", "B:null"), proc2.processed);
    }

    @Test
    public void testValueGetter() throws IOException {
        File stateDir = Files.createTempDirectory("test").toFile();
        try {
            final KStreamBuilder builder = new KStreamBuilder();

            String topic1 = "topic1";

            KTableImpl<String, Integer, Integer> table1 =
                    (KTableImpl<String, Integer, Integer>) builder.table(strSerializer, intSerializer, strDeserializer, intDeserializer, topic1);
            KTableImpl<String, Integer, Integer> table2 = (KTableImpl<String, Integer, Integer>) table1.filter(
                    new Predicate<String, Integer>() {
                        @Override
                        public boolean test(String key, Integer value) {
                            return (value % 2) == 0;
                        }
                    });

            KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();

            KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir, null, null, null, null);

            KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            getter2.init(driver.context());

            driver.process(topic1, "A", 1);
            driver.process(topic1, "B", 1);
            driver.process(topic1, "C", 1);

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

            driver.process(topic1, "A", 2);
            driver.process(topic1, "B", 2);

            assertEquals(new Integer(2), getter2.get("A"));
            assertEquals(new Integer(2), getter2.get("B"));
            assertNull(getter2.get("C"));

            driver.process(topic1, "A", 3);

            assertNull(getter2.get("A"));
            assertEquals(new Integer(2), getter2.get("B"));
            assertNull(getter2.get("C"));

            driver.process(topic1, "A", null);
            driver.process(topic1, "B", null);

            assertNull(getter2.get("A"));
            assertNull(getter2.get("B"));
            assertNull(getter2.get("C"));

        } finally {
            Utils.delete(stateDir);
        }
    }

}
