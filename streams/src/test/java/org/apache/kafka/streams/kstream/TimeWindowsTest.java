/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.kstream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TimeWindowsTest {

    @Test
    public void shouldHaveSaneEqualsAndHashCode() {
        long arbitrarySize = 10L;
        TimeWindows w1 = TimeWindows.of("w1", arbitrarySize);
        TimeWindows w2 = TimeWindows.of("w2", w1.size);

        // Reflexive
        assertTrue(w1.equals(w1));
        assertTrue(w1.hashCode() == w1.hashCode());

        // Symmetric
        assertTrue(w1.equals(w2));
        assertTrue(w1.hashCode() == w2.hashCode());
        assertTrue(w2.hashCode() == w1.hashCode());

        // Transitive
        TimeWindows w3 = TimeWindows.of("w3", w2.size);
        assertTrue(w2.equals(w3));
        assertTrue(w2.hashCode() == w3.hashCode());
        assertTrue(w1.equals(w3));
        assertTrue(w1.hashCode() == w3.hashCode());

        // Inequality scenarios
        assertFalse("must be false for null", w1.equals(null));
        assertFalse("must be false for different window types", w1.equals(UnlimitedWindows.of("irrelevant")));
        assertFalse("must be false for different types", w1.equals(new Object()));

        TimeWindows differentWindowSize = TimeWindows.of("differentWindowSize", w1.size + 1);
        assertFalse("must be false when window sizes are different", w1.equals(differentWindowSize));

        TimeWindows differentHopSize = w1.shiftedBy(w1.hop - 1);
        assertFalse("must be false when hop sizes are different", w1.equals(differentHopSize));
    }

}