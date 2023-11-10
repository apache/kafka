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
package org.apache.kafka.streams.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.streams.state.VersionedRecord;
import org.junit.Test;

public class VersionedKeyQueryTest {
    @Test
    public void shouldThrowNPEWithNullKey() {
        final Exception exception = assertThrows(NullPointerException.class, () -> VersionedKeyQuery.withKey(null));
        assertEquals("key cannot be null.", exception.getMessage());
    }

    @Test
    public void shouldThrowNPEWithNullAsOftimestamp() {
        final VersionedKeyQuery<Integer, VersionedRecord<Integer>> query = VersionedKeyQuery.withKey(1);
        final Exception exception = assertThrows(NullPointerException.class, () -> query.asOf(null));
        assertEquals("asOf timestamp cannot be null.", exception.getMessage());
    }
}
