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

package org.apache.kafka.metadata.placement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

public class CanarySpecTest {
    @Test
    public void testCanaryPodName() {
        CanarySpec canarySpec = new CanarySpec("pod1", 0.02);
        Assertions.assertEquals("pod1", canarySpec.toMap().keySet().iterator().next());
    }

    @Test
    public void testEmptyCanaryPodName() {
        CanarySpec canarySpec = new CanarySpec("", 0.02);
        Assertions.assertTrue(canarySpec.toMap().keySet().iterator().next().isEmpty());
    }

    @Test
    public void testZeroCanaryPercentage() {
        CanarySpec canarySpec = new CanarySpec("pod1", 0.0);
        Assertions.assertEquals(CanarySpec.ALWAYS_FALSE, canarySpec.toMap().values().iterator().next());
    }

    @Test
    public void testLargeCanaryPercentage() {
        CanarySpec canarySpec = new CanarySpec("pod1", 1.1);
        Assertions.assertEquals(CanarySpec.ALWAYS_FALSE, canarySpec.toMap().values().iterator().next());
    }

    @Test
    public void testCanaryPercentageOne() {
        CanarySpec canarySpec = new CanarySpec("pod1", 1.0);
        Assertions.assertTrue(canarySpec.toMap().values().iterator().next().test(1));
    }

    @Test
    public void testCanaryPercentageDefault() {
        CanarySpec canarySpec = new CanarySpec("pod1", 0.02);
        Predicate<Integer> predicate = canarySpec.toMap().values().iterator().next();
        for (int i = 0; i < 49; ++i) {
            Assertions.assertFalse(predicate.test(i));
        }
        Assertions.assertTrue(predicate.test(49));
        for (int i = 50; i < 99; ++i) {
            Assertions.assertFalse(predicate.test(i));
        }
        Assertions.assertTrue(predicate.test(99));
    }

    @Test
    public void testCanaryPercentageCustom() {
        CanarySpec canarySpec = new CanarySpec("pod1", 0.03125);
        Predicate<Integer> predicate = canarySpec.toMap().values().iterator().next();
        Assertions.assertFalse(predicate.test(0));
        Assertions.assertTrue(predicate.test(31));
        Assertions.assertTrue(predicate.test(63));
        Assertions.assertTrue(predicate.test(95));
    }
}
