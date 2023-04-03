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

package org.apache.kafka.pcoll.pcollections;

import org.junit.jupiter.api.Test;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PHashMapSetWrapperFactoryTest {
    private static final PCollectionsHashMapSetWrapperFactory FACTORY = new PCollectionsHashMapSetWrapperFactory();

    @Test
    public void testEmptyMap() {
        assertEquals(HashTreePMap.empty(), FACTORY.emptyMap().underlying());
    }

    @Test
    public void testSingletonMap() {
        assertEquals(HashTreePMap.singleton(1, 2), FACTORY.singletonMap(1, 2).underlying());
    }

    @Test
    public void testEmptySet() {
        assertEquals(HashTreePSet.empty(), FACTORY.emptySet().underlying());
    }

    @Test
    public void testSingletonSet() {
        assertEquals(HashTreePSet.singleton(1), FACTORY.singletonSet(1).underlying());
    }
}
