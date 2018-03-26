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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class KStreamWindowAggregateTest {

    final private Serde<String> strSerde = Serdes.String();
    private File stateDir = null;
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testAggBasic() {
        final StreamsBuilder builder = new StreamsBuilder();
        String topic1 = "topic1";

        KStream<String, String> stream1 = builder.stream(topic1, Consumed.with(strSerde, strSerde));
        KTable<Windowed<String>, String> table2 =
            stream1.groupByKey(Serialized.with(strSerde, strSerde))
                .aggregate(MockInitializer.STRING_INIT,
                           MockAggregator.TOSTRING_ADDER,
                           TimeWindows.of(10).advanceBy(5),
                           strSerde, "topic1-Canonized");

        MockProcessorSupplier<Windowed<String>, String> proc2 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);

        driver.setUp(builder, stateDir);

        setRecordContext(0, topic1);
        driver.process(topic1, "A", "1");
        driver.flushState();
        setRecordContext(1, topic1);
        driver.process(topic1, "B", "2");
        driver.flushState();
        setRecordContext(2, topic1);
        driver.process(topic1, "C", "3");
        driver.flushState();
        setRecordContext(3, topic1);
        driver.process(topic1, "D", "4");
        driver.flushState();
        setRecordContext(4, topic1);
        driver.process(topic1, "A", "1");
        driver.flushState();

        setRecordContext(5, topic1);
        driver.process(topic1, "A", "1");
        driver.flushState();
        setRecordContext(6, topic1);
        driver.process(topic1, "B", "2");
        driver.flushState();
        setRecordContext(7, topic1);
        driver.process(topic1, "D", "4");
        driver.flushState();
        setRecordContext(8, topic1);
        driver.process(topic1, "B", "2");
        driver.flushState();
        setRecordContext(9, topic1);
        driver.process(topic1, "C", "3");
        driver.flushState();
        setRecordContext(10, topic1);
        driver.process(topic1, "A", "1");
        driver.flushState();
        setRecordContext(11, topic1);
        driver.process(topic1, "B", "2");
        driver.flushState();
        setRecordContext(12, topic1);
        driver.flushState();
        driver.process(topic1, "D", "4");
        driver.flushState();
        setRecordContext(13, topic1);
        driver.process(topic1, "B", "2");
        driver.flushState();
        setRecordContext(14, topic1);
        driver.process(topic1, "C", "3");
        driver.flushState();


        assertEquals(Utils.mkList(
                "[A@0/10]:0+1",
                "[B@0/10]:0+2",
                "[C@0/10]:0+3",
                "[D@0/10]:0+4",
                "[A@0/10]:0+1+1",

                "[A@0/10]:0+1+1+1", "[A@5/15]:0+1",
                "[B@0/10]:0+2+2",   "[B@5/15]:0+2",
                "[D@0/10]:0+4+4",   "[D@5/15]:0+4",
                "[B@0/10]:0+2+2+2", "[B@5/15]:0+2+2",
                "[C@0/10]:0+3+3",   "[C@5/15]:0+3",

                "[A@5/15]:0+1+1",     "[A@10/20]:0+1",
                "[B@5/15]:0+2+2+2",   "[B@10/20]:0+2",
                "[D@5/15]:0+4+4",     "[D@10/20]:0+4",
                "[B@5/15]:0+2+2+2+2", "[B@10/20]:0+2+2",
                "[C@5/15]:0+3+3",     "[C@10/20]:0+3"), proc2.processed);
    }

    private void setRecordContext(final long time, final String topic) {
        ((MockProcessorContext) driver.context()).setRecordContext(new ProcessorRecordContext(time, 0, 0, topic));
    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        String topic1 = "topic1";
        String topic2 = "topic2";

        KStream<String, String> stream1 = builder.stream(topic1, Consumed.with(strSerde, strSerde));
        KTable<Windowed<String>, String> table1 =
            stream1.groupByKey(Serialized.with(strSerde, strSerde))
                .aggregate(MockInitializer.STRING_INIT,
                           MockAggregator.TOSTRING_ADDER,
                           TimeWindows.of(10).advanceBy(5),
                           strSerde, "topic1-Canonized");

        MockProcessorSupplier<Windowed<String>, String> proc1 = new MockProcessorSupplier<>();
        table1.toStream().process(proc1);

        KStream<String, String> stream2 = builder.stream(topic2, Consumed.with(strSerde, strSerde));
        KTable<Windowed<String>, String> table2 =
            stream2.groupByKey(Serialized.with(strSerde, strSerde))
                .aggregate(MockInitializer.STRING_INIT,
                           MockAggregator.TOSTRING_ADDER,
                           TimeWindows.of(10).advanceBy(5),
                           strSerde, "topic2-Canonized");

        MockProcessorSupplier<Windowed<String>, String> proc2 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);


        MockProcessorSupplier<Windowed<String>, String> proc3 = new MockProcessorSupplier<>();
        table1.join(table2, new ValueJoiner<String, String, String>() {
            @Override
            public String apply(String p1, String p2) {
                return p1 + "%" + p2;
            }
        }).toStream().process(proc3);

        driver.setUp(builder, stateDir);

        setRecordContext(0, topic1);
        driver.process(topic1, "A", "1");
        driver.flushState();
        setRecordContext(1, topic1);
        driver.process(topic1, "B", "2");
        driver.flushState();
        setRecordContext(2, topic1);
        driver.process(topic1, "C", "3");
        driver.flushState();
        setRecordContext(3, topic1);
        driver.process(topic1, "D", "4");
        driver.flushState();
        setRecordContext(4, topic1);
        driver.process(topic1, "A", "1");
        driver.flushState();

        proc1.checkAndClearProcessResult(
                "[A@0/10]:0+1",
                "[B@0/10]:0+2",
                "[C@0/10]:0+3",
                "[D@0/10]:0+4",
                "[A@0/10]:0+1+1"
        );
        proc2.checkAndClearProcessResult();
        proc3.checkAndClearProcessResult();

        setRecordContext(5, topic1);
        driver.process(topic1, "A", "1");
        driver.flushState();
        setRecordContext(6, topic1);
        driver.process(topic1, "B", "2");
        driver.flushState();
        setRecordContext(7, topic1);
        driver.process(topic1, "D", "4");
        driver.flushState();
        setRecordContext(8, topic1);
        driver.process(topic1, "B", "2");
        driver.flushState();
        setRecordContext(9, topic1);
        driver.process(topic1, "C", "3");
        driver.flushState();

        proc1.checkAndClearProcessResult(
                "[A@0/10]:0+1+1+1", "[A@5/15]:0+1",
                "[B@0/10]:0+2+2",   "[B@5/15]:0+2",
                "[D@0/10]:0+4+4",   "[D@5/15]:0+4",
                "[B@0/10]:0+2+2+2", "[B@5/15]:0+2+2",
                "[C@0/10]:0+3+3",   "[C@5/15]:0+3"
        );
        proc2.checkAndClearProcessResult();
        proc3.checkAndClearProcessResult();

        setRecordContext(0, topic1);
        driver.process(topic2, "A", "a");
        driver.flushState();
        setRecordContext(1, topic1);
        driver.process(topic2, "B", "b");
        driver.flushState();
        setRecordContext(2, topic1);
        driver.process(topic2, "C", "c");
        driver.flushState();
        setRecordContext(3, topic1);
        driver.process(topic2, "D", "d");
        driver.flushState();
        setRecordContext(4, topic1);
        driver.process(topic2, "A", "a");
        driver.flushState();

        proc1.checkAndClearProcessResult();
        proc2.checkAndClearProcessResult(
                "[A@0/10]:0+a",
                "[B@0/10]:0+b",
                "[C@0/10]:0+c",
                "[D@0/10]:0+d",
                "[A@0/10]:0+a+a"
        );
        proc3.checkAndClearProcessResult(
                "[A@0/10]:0+1+1+1%0+a",
                "[B@0/10]:0+2+2+2%0+b",
                "[C@0/10]:0+3+3%0+c",
                "[D@0/10]:0+4+4%0+d",
                "[A@0/10]:0+1+1+1%0+a+a");

        setRecordContext(5, topic1);
        driver.process(topic2, "A", "a");
        driver.flushState();
        setRecordContext(6, topic1);
        driver.process(topic2, "B", "b");
        driver.flushState();
        setRecordContext(7, topic1);
        driver.process(topic2, "D", "d");
        driver.flushState();
        setRecordContext(8, topic1);
        driver.process(topic2, "B", "b");
        driver.flushState();
        setRecordContext(9, topic1);
        driver.process(topic2, "C", "c");
        driver.flushState();
        proc1.checkAndClearProcessResult();
        proc2.checkAndClearProcessResult(
                "[A@0/10]:0+a+a+a", "[A@5/15]:0+a",
                "[B@0/10]:0+b+b",   "[B@5/15]:0+b",
                "[D@0/10]:0+d+d",   "[D@5/15]:0+d",
                "[B@0/10]:0+b+b+b", "[B@5/15]:0+b+b",
                "[C@0/10]:0+c+c",   "[C@5/15]:0+c"
        );
        proc3.checkAndClearProcessResult(
                "[A@0/10]:0+1+1+1%0+a+a+a", "[A@5/15]:0+1%0+a",
                "[B@0/10]:0+2+2+2%0+b+b",   "[B@5/15]:0+2+2%0+b",
                "[D@0/10]:0+4+4%0+d+d",     "[D@5/15]:0+4%0+d",
                "[B@0/10]:0+2+2+2%0+b+b+b", "[B@5/15]:0+2+2%0+b+b",
                "[C@0/10]:0+3+3%0+c+c",     "[C@5/15]:0+3%0+c"
        );
    }

}
