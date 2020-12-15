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

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class MaterializedTest {

    @Test
    public void shouldAllowValidTopicNamesAsStoreName() {
        Materialized.as("valid-name");
        Materialized.as("valid.name");
        Materialized.as("valid_name");
    }

    @Test
    public void shouldNotAllowInvalidTopicNames() {
        final String invalidName = "not:valid";
        final TopologyException e = assertThrows(TopologyException.class,
            () -> Materialized.as(invalidName));

        assertEquals(e.getMessage(), "Invalid topology: Name \"" + invalidName +
            "\" is illegal, it contains a character other than " + "ASCII alphanumerics, '.', '_' and '-'");
    }

    @Test
    public void shouldThrowNullPointerIfWindowBytesStoreSupplierIsNull() {
        final NullPointerException e = assertThrows(NullPointerException.class,
            () -> Materialized.as((WindowBytesStoreSupplier) null));

        assertEquals(e.getMessage(), "supplier can't be null");
    }

    @Test
    public void shouldThrowNullPointerIfKeyValueBytesStoreSupplierIsNull() {
        final NullPointerException e = assertThrows(NullPointerException.class,
            () -> Materialized.as((KeyValueBytesStoreSupplier) null));

        assertEquals(e.getMessage(), "supplier can't be null");
    }

    @Test
    public void shouldThrowNullPointerIfSessionBytesStoreSupplierIsNull() {
        final NullPointerException e = assertThrows(NullPointerException.class,
            () -> Materialized.as((SessionBytesStoreSupplier) null));

        assertEquals(e.getMessage(), "supplier can't be null");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfRetentionIsNegative() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> Materialized.as("valid-name").withRetention(Duration.of(-1, ChronoUnit.DAYS)));

        assertEquals(e.getMessage(), "Retention must not be negative.");
    }

    @Test
    public void shouldThrowTopologyExceptionIfStoreNameExceedsMaxAllowedLength() {
        final StringBuffer invalidStoreNameBuffer = new StringBuffer();
        final int maxNameLength = 249;

        for (int i = 0; i < maxNameLength + 1; i++) {
            invalidStoreNameBuffer.append('a');
        }

        final String invalidStoreName = invalidStoreNameBuffer.toString();

        final TopologyException e = assertThrows(TopologyException.class,
            () -> Materialized.as(invalidStoreName));
        assertEquals(e.getMessage(), "Invalid topology: Name is illegal, it can't be longer than " + maxNameLength +
                " characters, name: " + invalidStoreName);
    }
}