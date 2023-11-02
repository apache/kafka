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

import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RepartitionTopicConfigTest {

    @Test
    public void shouldThrowAnExceptionWhenSettingNumberOfPartitionsIfTheyAreEnforced() {
        final String name = "my-topic";
        final RepartitionTopicConfig repartitionTopicConfig = new RepartitionTopicConfig(name,
                                                                                         Collections.emptyMap(),
                                                                                         10,
                                                                                         true);

        final UnsupportedOperationException ex = assertThrows(
            UnsupportedOperationException.class,
            () -> repartitionTopicConfig.setNumberOfPartitions(2)
        );

        assertEquals(String.format("number of partitions are enforced on topic " +
                                   "%s and can't be altered.", name), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenSettingNumberOfPartitionsIfTheyAreNotEnforced() {
        final String name = "my-topic";
        final RepartitionTopicConfig repartitionTopicConfig = new RepartitionTopicConfig(name,
                                                                                         Collections.emptyMap(),
                                                                                         10,
                                                                                         false);

        repartitionTopicConfig.setNumberOfPartitions(4);

        assertEquals(repartitionTopicConfig.numberOfPartitions(), Optional.of(4));
    }
}