/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class WallclockTimestampExtractorTest {

    @Test
    public void extractSystemTimestamp() {
        final TimestampExtractor extractor = new WallclockTimestampExtractor();

        final long before = System.currentTimeMillis();
        final long timestamp = extractor.extract(new ConsumerRecord<>("anyTopic", 0, 0, null, null), 42);
        final long after = System.currentTimeMillis();

        assertThat(timestamp, is(new InBetween(before, after)));
    }

    private static class InBetween extends BaseMatcher<Long> {
        private final long before;
        private final long after;

        public InBetween(long before, long after) {
            this.before = before;
            this.after = after;
        }

        @Override
        public boolean matches(Object item) {
            final long timestamp = (Long) item;
            return before <= timestamp && timestamp <= after;
        }

        @Override
        public void describeMismatch(Object item, Description mismatchDescription) {}

        @Override
        public void describeTo(Description description) {}
    }

}
