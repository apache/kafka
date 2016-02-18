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

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KTableKTableOuterJoinTest {

    private String topic1 = "topic1";
    private String topic2 = "topic2";

    private IntegerSerializer keySerializer = new IntegerSerializer();
    private StringSerializer valSerializer = new StringSerializer();
    private IntegerDeserializer keyDeserializer = new IntegerDeserializer();
    private StringDeserializer valDeserializer = new StringDeserializer();

    private ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
        @Override
        public String apply(String value1, String value2) {
            return value1 + "+" + value2;
        }
    };

    private static class JoinedKeyValue extends KeyValue<Integer, String> {
        public JoinedKeyValue(Integer key, String value) {
            super(key, value);
        }
    }

    @Test
    public void testJoin() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {

            KStreamBuilder builder = new KStreamBuilder();

            final int[] expectedKeys = new int[]{0, 1, 2, 3};

            KTable<Integer, String> table1;
            KTable<Integer, String> table2;
            KTable<Integer, String> joined;
            MockProcessorSupplier<Integer, String> processor;

            processor = new MockProcessorSupplier<>();
            table1 = builder.table(keySerializer, valSerializer, keyDeserializer, valDeserializer, topic1);
            table2 = builder.table(keySerializer, valSerializer, keyDeserializer, valDeserializer, topic2);
            joined = table1.outerJoin(table2, joiner);
            joined.toStream().process(processor);

            Collection<Set<String>> copartitionGroups = builder.copartitionGroups();

            assertEquals(1, copartitionGroups.size());
            assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

            KTableValueGetterSupplier<Integer, String> getterSupplier = ((KTableImpl<Integer, String, String>) joined).valueGetterSupplier();

            KStreamTestDriver driver = new KStreamTestDriver(builder, baseDir);
            driver.setTime(0L);

            KTableValueGetter<Integer, String> getter = getterSupplier.get();
            getter.init(driver.context());

            // push two items to the primary stream. the other table is empty

            for (int i = 0; i < 2; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            processor.checkAndClearResult("0:X0+null", "1:X1+null");
            checkJoinedValues(getter, kv(0, "X0+null"), kv(1, "X1+null"), kv(2, null), kv(3, null));

            // push two items to the other stream. this should produce two items.

            for (int i = 0; i < 2; i++) {
                driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
            }

            processor.checkAndClearResult("0:X0+Y0", "1:X1+Y1");
            checkJoinedValues(getter, kv(0, "X0+Y0"), kv(1, "X1+Y1"), kv(2, null), kv(3, null));

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            processor.checkAndClearResult("0:X0+Y0", "1:X1+Y1", "2:X2+null", "3:X3+null");
            checkJoinedValues(getter, kv(0, "X0+Y0"), kv(1, "X1+Y1"), kv(2, "X2+null"), kv(3, "X3+null"));

            // push all items to the other stream. this should produce four items.
            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic2, expectedKeys[i], "YY" + expectedKeys[i]);
            }

            processor.checkAndClearResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");
            checkJoinedValues(getter, kv(0, "X0+YY0"), kv(1, "X1+YY1"), kv(2, "X2+YY2"), kv(3, "X3+YY3"));

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            processor.checkAndClearResult("0:X0+YY0", "1:X1+YY1", "2:X2+YY2", "3:X3+YY3");
            checkJoinedValues(getter, kv(0, "X0+YY0"), kv(1, "X1+YY1"), kv(2, "X2+YY2"), kv(3, "X3+YY3"));

            // push two items with null to the other stream as deletes. this should produce two item.

            for (int i = 0; i < 2; i++) {
                driver.process(topic2, expectedKeys[i], null);
            }

            processor.checkAndClearResult("0:X0+null", "1:X1+null");
            checkJoinedValues(getter, kv(0, "X0+null"), kv(1, "X1+null"), kv(2, "X2+YY2"), kv(3, "X3+YY3"));

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
            }

            processor.checkAndClearResult("0:XX0+null", "1:XX1+null", "2:XX2+YY2", "3:XX3+YY3");
            checkJoinedValues(getter, kv(0, "XX0+null"), kv(1, "XX1+null"), kv(2, "XX2+YY2"), kv(3, "XX3+YY3"));

            // push middle two items to the primary stream with null. this should produce two items.

            for (int i = 1; i < 3; i++) {
                driver.process(topic1, expectedKeys[i], null);
            }

            processor.checkAndClearResult("1:null", "2:null+YY2");
            checkJoinedValues(getter, kv(0, "XX0+null"), kv(1, null), kv(2, "null+YY2"), kv(3, "XX3+YY3"));

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testNotSendingOldValue() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {

            KStreamBuilder builder = new KStreamBuilder();

            final int[] expectedKeys = new int[]{0, 1, 2, 3};

            KTable<Integer, String> table1;
            KTable<Integer, String> table2;
            KTable<Integer, String> joined;
            MockProcessorSupplier<Integer, String> proc;

            table1 = builder.table(keySerializer, valSerializer, keyDeserializer, valDeserializer, topic1);
            table2 = builder.table(keySerializer, valSerializer, keyDeserializer, valDeserializer, topic2);
            joined = table1.outerJoin(table2, joiner);

            proc = new MockProcessorSupplier<>();
            builder.addProcessor("proc", proc, ((KTableImpl<?, ?, ?>) joined).name);

            KStreamTestDriver driver = new KStreamTestDriver(builder, baseDir);
            driver.setTime(0L);

            assertFalse(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
            assertFalse(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
            assertFalse(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());

            // push two items to the primary stream. the other table is empty

            for (int i = 0; i < 2; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+null<-null)", "1:(X1+null<-null)");

            // push two items to the other stream. this should produce two items.

            for (int i = 0; i < 2; i++) {
                driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+Y0<-null)", "1:(X1+Y1<-null)");

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+Y0<-null)", "1:(X1+Y1<-null)", "2:(X2+null<-null)", "3:(X3+null<-null)");

            // push all items to the other stream. this should produce four items.
            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic2, expectedKeys[i], "YY" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+YY0<-null)", "1:(X1+YY1<-null)", "2:(X2+YY2<-null)", "3:(X3+YY3<-null)");

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+YY0<-null)", "1:(X1+YY1<-null)", "2:(X2+YY2<-null)", "3:(X3+YY3<-null)");

            // push two items with null to the other stream as deletes. this should produce two item.

            for (int i = 0; i < 2; i++) {
                driver.process(topic2, expectedKeys[i], null);
            }

            proc.checkAndClearResult("0:(X0+null<-null)", "1:(X1+null<-null)");

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(XX0+null<-null)", "1:(XX1+null<-null)", "2:(XX2+YY2<-null)", "3:(XX3+YY3<-null)");

            // push middle two items to the primary stream with null. this should produce two items.

            for (int i = 1; i < 3; i++) {
                driver.process(topic1, expectedKeys[i], null);
            }

            proc.checkAndClearResult("1:(null<-null)", "2:(null+YY2<-null)");

        } finally {
            Utils.delete(baseDir);
        }
    }

    @Test
    public void testSendingOldValue() throws Exception {
        File baseDir = Files.createTempDirectory("test").toFile();
        try {

            KStreamBuilder builder = new KStreamBuilder();

            final int[] expectedKeys = new int[]{0, 1, 2, 3};

            KTable<Integer, String> table1;
            KTable<Integer, String> table2;
            KTable<Integer, String> joined;
            MockProcessorSupplier<Integer, String> proc;

            table1 = builder.table(keySerializer, valSerializer, keyDeserializer, valDeserializer, topic1);
            table2 = builder.table(keySerializer, valSerializer, keyDeserializer, valDeserializer, topic2);
            joined = table1.outerJoin(table2, joiner);

            ((KTableImpl<?, ?, ?>) joined).enableSendingOldValues();

            proc = new MockProcessorSupplier<>();
            builder.addProcessor("proc", proc, ((KTableImpl<?, ?, ?>) joined).name);

            KStreamTestDriver driver = new KStreamTestDriver(builder, baseDir);
            driver.setTime(0L);

            assertTrue(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
            assertTrue(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
            assertTrue(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());

            // push two items to the primary stream. the other table is empty

            for (int i = 0; i < 2; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+null<-null)", "1:(X1+null<-null)");

            // push two items to the other stream. this should produce two items.

            for (int i = 0; i < 2; i++) {
                driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+Y0<-X0+null)", "1:(X1+Y1<-X1+null)");

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+Y0<-X0+Y0)", "1:(X1+Y1<-X1+Y1)", "2:(X2+null<-null)", "3:(X3+null<-null)");

            // push all items to the other stream. this should produce four items.
            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic2, expectedKeys[i], "YY" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+YY0<-X0+Y0)", "1:(X1+YY1<-X1+Y1)", "2:(X2+YY2<-X2+null)", "3:(X3+YY3<-X3+null)");

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(X0+YY0<-X0+YY0)", "1:(X1+YY1<-X1+YY1)", "2:(X2+YY2<-X2+YY2)", "3:(X3+YY3<-X3+YY3)");

            // push two items with null to the other stream as deletes. this should produce two item.

            for (int i = 0; i < 2; i++) {
                driver.process(topic2, expectedKeys[i], null);
            }

            proc.checkAndClearResult("0:(X0+null<-X0+YY0)", "1:(X1+null<-X1+YY1)");

            // push all four items to the primary stream. this should produce four items.

            for (int i = 0; i < expectedKeys.length; i++) {
                driver.process(topic1, expectedKeys[i], "XX" + expectedKeys[i]);
            }

            proc.checkAndClearResult("0:(XX0+null<-X0+null)", "1:(XX1+null<-X1+null)", "2:(XX2+YY2<-X2+YY2)", "3:(XX3+YY3<-X3+YY3)");

            // push middle two items to the primary stream with null. this should produce two items.

            for (int i = 1; i < 3; i++) {
                driver.process(topic1, expectedKeys[i], null);
            }

            proc.checkAndClearResult("1:(null<-XX1+null)", "2:(null+YY2<-XX2+YY2)");

        } finally {
            Utils.delete(baseDir);
        }
    }

    private JoinedKeyValue kv(Integer key, String value) {
        return new JoinedKeyValue(key, value);
    }

    private void checkJoinedValues(KTableValueGetter<Integer, String> getter, JoinedKeyValue... expected) {
        for (JoinedKeyValue kv : expected) {
            String value = getter.get(kv.key);
            if (kv.value == null) {
                assertNull(value);
            } else {
                assertEquals(kv.value, value);
            }
        }
    }
}
