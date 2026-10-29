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
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

        assertEquals("Invalid topology: Name \"" + invalidName +
            "\" is illegal, it contains a character other than " + "ASCII alphanumerics, '.', '_' and '-'", e.getMessage());
    }

    @Test
    public void shouldThrowNullPointerIfWindowBytesStoreSupplierIsNull() {
        final NullPointerException e = assertThrows(NullPointerException.class,
            () -> Materialized.as((WindowBytesStoreSupplier) null));

        assertEquals("supplier can't be null", e.getMessage());
    }

    @Test
    public void shouldThrowNullPointerIfKeyValueBytesStoreSupplierIsNull() {
        final NullPointerException e = assertThrows(NullPointerException.class,
            () -> Materialized.as((KeyValueBytesStoreSupplier) null));

        assertEquals("supplier can't be null", e.getMessage());
    }

    @Test
    public void shouldThrowNullPointerIfStoreTypeIsNull() {
        final NullPointerException e = assertThrows(NullPointerException.class,
            () -> Materialized.as((Materialized.StoreType) null));

        assertEquals("store type can't be null", e.getMessage());
    }

    @Test
    public void shouldThrowNullPointerIfSessionBytesStoreSupplierIsNull() {
        final NullPointerException e = assertThrows(NullPointerException.class,
            () -> Materialized.as((SessionBytesStoreSupplier) null));

        assertEquals("supplier can't be null", e.getMessage());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfRetentionIsNegative() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> Materialized.as("valid-name").withRetention(Duration.of(-1, ChronoUnit.DAYS)));

        assertEquals("Retention must not be negative.", e.getMessage());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfStoreSupplierAndStoreTypeBothSet() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Materialized.as(Stores.persistentKeyValueStore("test")).withStoreType(Materialized.StoreType.ROCKS_DB));

        assertEquals("Cannot set store type when store supplier is pre-configured.", e.getMessage());
    }

    @Test
    public void shouldThrowTopologyExceptionIfStoreNameExceedsMaxAllowedLength() {
        final int maxNameLength = 249;
        final String invalidStoreName = "a".repeat(maxNameLength + 1);

        final TopologyException e = assertThrows(TopologyException.class,
            () -> Materialized.as(invalidStoreName));
        assertEquals("Invalid topology: Name is illegal, it can't be longer than " + maxNameLength +
                " characters, name: " + invalidStoreName, e.getMessage());
    }
}
