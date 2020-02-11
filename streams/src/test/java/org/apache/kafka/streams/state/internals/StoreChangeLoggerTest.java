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


import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class StoreChangeLoggerTest {

    private final String topic = "topic";

    private final MockRecordCollector collector = new MockRecordCollector();
    private final InternalMockProcessorContext context = new InternalMockProcessorContext(
        StateSerdes.withBuiltinTypes(topic, Integer.class, String.class),
        collector);

    private final StoreChangeLogger<Integer, String> changeLogger =
        new StoreChangeLogger<>(topic, context, StateSerdes.withBuiltinTypes(topic, Integer.class, String.class));

    @Test
    public void testAddRemove() {
        context.setTime(1);
        changeLogger.logChange(0, "zero");
        context.setTime(5);
        changeLogger.logChange(1, "one");
        changeLogger.logChange(2, "two");
        changeLogger.logChange(3, "three", 42L);
        context.setTime(9);
        changeLogger.logChange(0, null);

        assertThat(collector.collected().size(), equalTo(5));
        assertThat(collector.collected().get(0).key(), equalTo(0));
        assertThat(collector.collected().get(0).value(), equalTo("zero"));
        assertThat(collector.collected().get(0).timestamp(), equalTo(1L));
        assertThat(collector.collected().get(1).key(), equalTo(1));
        assertThat(collector.collected().get(1).value(), equalTo("one"));
        assertThat(collector.collected().get(1).timestamp(), equalTo(5L));
        assertThat(collector.collected().get(2).key(), equalTo(2));
        assertThat(collector.collected().get(2).value(), equalTo("two"));
        assertThat(collector.collected().get(2).timestamp(), equalTo(5L));
        assertThat(collector.collected().get(3).key(), equalTo(3));
        assertThat(collector.collected().get(3).value(), equalTo("three"));
        assertThat(collector.collected().get(3).timestamp(), equalTo(42L));
        assertThat(collector.collected().get(4).key(), equalTo(0));
        assertThat(collector.collected().get(4).value(), nullValue());
        assertThat(collector.collected().get(4).timestamp(), equalTo(9L));
    }

    @Test
    public void shouldNotSendRecordHeadersToChangelogTopic() {
        context.headers().add(new RecordHeader("key", "value".getBytes()));
        changeLogger.logChange(0, "zero", 42L);

        assertThat(collector.collected().size(), equalTo(1));
        assertThat(collector.collected().get(0).key(), equalTo(0));
        assertThat(collector.collected().get(0).value(), equalTo("zero"));
        assertThat(collector.collected().get(0).timestamp(), equalTo(42L));
        assertThat(collector.collected().get(0).headers().toArray(), equalTo(Record.EMPTY_HEADERS));
    }
}
