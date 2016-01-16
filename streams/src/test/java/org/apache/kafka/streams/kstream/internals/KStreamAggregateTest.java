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
import org.apache.kafka.streams.kstream.AggregatorSupplier;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class KStreamAggregateTest {

    private final Serializer<String> strSerializer = new StringSerializer();
    private final Deserializer<String> strDeserializer = new StringDeserializer();

    private class StringCanonizeSupplier implements AggregatorSupplier<String, String, String> {

        private class StringCanonizer implements Aggregator<String, String, String> {

            @Override
            public String initialValue() {
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

            @Override
            public String merge(String aggr1, String aggr2) {
                return "(" + aggr1 + ") + (" + aggr2 + ")";
            }
        }

        @Override
        public Aggregator<String, String, String> get() {
            return new StringCanonizer();
        }
    }

    @Test
    public void testAggBasic() throws Exception {
        final File baseDir = Files.createTempDirectory("test").toFile();

        try {
            final KStreamBuilder builder = new KStreamBuilder();
            String topic1 = "topic1";

            KStream<String, String> stream1 = builder.stream(strDeserializer, strDeserializer, topic1);
            KTable<Windowed<String>, String> table2 = stream1.aggregateByKey(new StringCanonizeSupplier(),
                    HoppingWindows.of("topic1-Canonized").with(10L).every(5L),
                    strSerializer,
                    strSerializer,
                    strDeserializer,
                    strDeserializer);

            MockProcessorSupplier<Windowed<String>, String> proc2 = new MockProcessorSupplier<>();
            table2.toStream().process(proc2);

            KStreamTestDriver driver = new KStreamTestDriver(builder, baseDir);

            driver.setTime(0L);
            driver.process(topic1, "A", "1");
            driver.setTime(1L);
            driver.process(topic1, "B", "2");
            driver.setTime(2L);
            driver.process(topic1, "C", "3");
            driver.setTime(3L);
            driver.process(topic1, "D", "4");
            driver.setTime(4L);
            driver.process(topic1, "A", "1");

            driver.setTime(5L);
            driver.process(topic1, "A", "1");
            driver.setTime(6L);
            driver.process(topic1, "B", "2");
            driver.setTime(7L);
            driver.process(topic1, "D", "4");
            driver.setTime(8L);
            driver.process(topic1, "B", "2");
            driver.setTime(9L);
            driver.process(topic1, "C", "3");

            driver.setTime(10L);
            driver.process(topic1, "A", "1");
            driver.setTime(11L);
            driver.process(topic1, "B", "2");
            driver.setTime(12L);
            driver.process(topic1, "D", "4");
            driver.setTime(13L);
            driver.process(topic1, "B", "2");
            driver.setTime(14L);
            driver.process(topic1, "C", "3");

            assertEquals(Utils.mkList(
                    "[A@0]:0+1",
                    "[B@0]:0+2",
                    "[C@0]:0+3",
                    "[D@0]:0+4",
                    "[A@0]:0+1+1",

                    "[A@0]:0+1+1+1", "[A@5]:0+1",
                    "[B@0]:0+2+2", "[B@5]:0+2",
                    "[D@0]:0+4+4", "[D@5]:0+4",
                    "[B@0]:0+2+2+2", "[B@5]:0+2+2",
                    "[C@0]:0+3+3", "[C@5]:0+3",

                    "[A@5]:0+1+1", "[A@10]:0+1",
                    "[B@5]:0+2+2+2", "[B@10]:0+2",
                    "[D@5]:0+4+4", "[D@10]:0+4",
                    "[B@5]:0+2+2+2+2", "[B@10]:0+2+2",
                    "[C@5]:0+3+3", "[C@10]:0+3"), proc2.processed);

        } finally {
            Utils.delete(baseDir);
        }
    }
}
