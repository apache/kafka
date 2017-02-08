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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CheckpointerTest {

    private static final int CHECKPOINT_INTERVAL = 1000;
    private final MockTime time = new MockTime();
    private final CheckpointableStub checkpointable = new CheckpointableStub();
    private final Checkpointer checkpointer = new Checkpointer(time, checkpointable, CHECKPOINT_INTERVAL);

    @Test
    public void shouldNotCheckpointIfCheckpointIntervalHasntElapsed() throws Exception {
        checkpointer.checkpoint(Collections.<TopicPartition, Long>emptyMap());
        assertThat(checkpointable.callCount, equalTo(0));
    }

    @Test
    public void shouldCheckpointIfIntervalHasElapsed() throws Exception {
        time.sleep(CHECKPOINT_INTERVAL);
        checkpointer.checkpoint(Collections.<TopicPartition, Long>emptyMap());
        assertThat(checkpointable.callCount, equalTo(1));
    }

    @Test
    public void shouldCheckpointEveryTimeTheIntervalHasElapsed() throws Exception {
        time.sleep(CHECKPOINT_INTERVAL);
        checkpointer.checkpoint(Collections.<TopicPartition, Long>emptyMap());
        time.sleep(CHECKPOINT_INTERVAL);
        checkpointer.checkpoint(Collections.<TopicPartition, Long>emptyMap());
        assertThat(checkpointable.callCount, equalTo(2));
    }

    @Test
    public void shouldNotCheckpointBetweenIntervals() throws Exception {
        time.sleep(CHECKPOINT_INTERVAL);
        checkpointer.checkpoint(Collections.<TopicPartition, Long>emptyMap());
        time.sleep(CHECKPOINT_INTERVAL / 2);
        checkpointer.checkpoint(Collections.<TopicPartition, Long>emptyMap());
        assertThat(checkpointable.callCount, equalTo(1));
    }
    
    class CheckpointableStub implements Checkpointable {
        int callCount = 0;
        @Override
        public void checkpoint(final Map<TopicPartition, Long> offsets) {
            callCount++;
        }
    }

}