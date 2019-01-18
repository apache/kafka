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

package org.apache.kafka.streams.kstream;


import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class KafkaStreamsBrancherTest {
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void correctConsumersAreCalled() {
        Predicate p1 = Mockito.mock(Predicate.class);
        Predicate p2 = Mockito.mock(Predicate.class);
        KStream input = Mockito.mock(KStream.class);
        KStream[] result = new KStream[]{Mockito.mock(KStream.class),
                Mockito.mock(KStream.class), Mockito.mock(KStream.class)};
        Mockito.when(input.branch(Mockito.eq(p1), Mockito.eq(p2), Mockito.any()))
                .thenReturn(result);
        AtomicInteger invocations = new AtomicInteger(0);
        assertSame(input, new KafkaStreamBrancher()
                .branch(
                        p1,
                        ks -> {
                            assertSame(result[0], ks);
                            assertEquals(0, invocations.getAndIncrement());
                        })
                .defaultBranch(ks -> {
                    assertSame(result[2], ks);
                    assertEquals(2, invocations.getAndIncrement());
                })
                .branch(p2,
                        ks -> {
                            assertSame(result[1], ks);
                            assertEquals(1, invocations.getAndIncrement());
                        })
                .onTopOf(input));

        assertEquals(3, invocations.get());
    }
}
