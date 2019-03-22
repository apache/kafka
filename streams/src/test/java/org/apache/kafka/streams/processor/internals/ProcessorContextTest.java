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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class ProcessorContextTest {
    private ProcessorContext context;

    @Before
    public void prepare() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("add-id");
        expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
        replay(streamsConfig);

        context = new ProcessorContextImpl(
            mock(TaskId.class),
            mock(StreamTask.class),
            streamsConfig,
            mock(RecordCollector.class),
            mock(ProcessorStateManager.class),
            mock(StreamsMetricsImpl.class),
            mock(ThreadCache.class)
        );
    }

    @Test
    public void shouldNotAllowToScheduleZeroMillisecondPunctuation() {
        try {
            context.schedule(Duration.ofMillis(0L), null, null);
            fail("Should have thrown IllegalArgumentException");
        } catch (final IllegalArgumentException expected) {
            assertThat(expected.getMessage(), equalTo("The minimum supported scheduling interval is 1 millisecond."));
        }
    }

    @Test
    public void shouldNotAllowToScheduleSubMillisecondPunctuation() {
        try {
            context.schedule(Duration.ofNanos(999_999L), null, null);
            fail("Should have thrown IllegalArgumentException");
        } catch (final IllegalArgumentException expected) {
            assertThat(expected.getMessage(), equalTo("The minimum supported scheduling interval is 1 millisecond."));
        }
    }
}
