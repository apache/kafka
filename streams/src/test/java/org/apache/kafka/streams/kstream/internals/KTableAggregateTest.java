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
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.NoOpKeyValueMapper;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class KTableAggregateTest {

    private final Serializer<String> strSerializer = new StringSerializer();
    private final Deserializer<String> strDeserializer = new StringDeserializer();

    private class StringCanonizer implements Aggregator<String, String, String> {

        @Override
        public String initialValue(String aggKey) {
            return "0";
        }

        @Override
        public String add(String aggKey, String value, String aggregate) {
            return aggregate + "+" + value;
        }

        @Override
        public String remove(String aggKey, String value, String aggregate) {
            return aggregate + "-" + value;
        }
    }

    @Test
    public void testAggBasic() throws Exception {
        final File baseDir = Files.createTempDirectory("test").toFile();

        try {
            final KStreamBuilder builder = new KStreamBuilder();

            builder.register(String.class, new StringSerializer(), new StringDeserializer());

            String topic1 = "topic1";

            KTable<String, String> table1 = builder.table(String.class, String.class, topic1);
            KTable<String, String> table2 = table1.aggregate(new StringCanonizer(),
                    new NoOpKeyValueMapper<String, String>() { }, // capture types by creating an anonymous subclass
                    "topic1-Canonized");

            MockProcessorSupplier<String, String> proc2 = new MockProcessorSupplier<>();
            table2.toStream().process(proc2);

            KStreamTestDriver driver = new KStreamTestDriver(builder, baseDir);

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

        } finally {
            Utils.delete(baseDir);
        }
    }
}
