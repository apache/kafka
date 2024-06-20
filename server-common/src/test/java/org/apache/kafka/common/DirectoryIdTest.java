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
package org.apache.kafka.common;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectoryIdTest {
    @Test
    void testUnassignedIsReserved() {
        assertTrue(DirectoryId.reserved(DirectoryId.UNASSIGNED));
    }

    @Test
    void testLostIsReserved() {
        assertTrue(DirectoryId.reserved(DirectoryId.LOST));
    }

    @Test
    void testMigratingIsReserved() {
        assertTrue(DirectoryId.reserved(DirectoryId.MIGRATING));
    }

    @Test
    void testCreateAssignmentMap() {
        assertThrows(IllegalArgumentException.class, () ->
                DirectoryId.createAssignmentMap(new int[]{1, 2}, DirectoryId.unassignedArray(3)));
        assertEquals(
            new HashMap<Integer, Uuid>() {{
                    put(1, Uuid.fromString("upjfkCrUR9GNn1i94ip1wg"));
                    put(2, Uuid.fromString("bCF3l0RIQjOKhUqgbivHZA"));
                    put(3, Uuid.fromString("Fg3mFhcVQlqCWRk4dZazxw"));
                    put(4, Uuid.fromString("bv9TEYi4TqOm52hLmrxT5w"));
                }},
            DirectoryId.createAssignmentMap(
                    new int[] {1, 2, 3, 4},
                    new Uuid[] {
                            Uuid.fromString("upjfkCrUR9GNn1i94ip1wg"),
                            Uuid.fromString("bCF3l0RIQjOKhUqgbivHZA"),
                            Uuid.fromString("Fg3mFhcVQlqCWRk4dZazxw"),
                            Uuid.fromString("bv9TEYi4TqOm52hLmrxT5w")
                    })
        );
    }

    @Test
    void testIsOnline() {
        // Given
        List<Uuid> sortedDirs = Arrays.asList(
                Uuid.fromString("imQKg2cXTVe8OUFNa3R9bg"),
                Uuid.fromString("Mwy5wxTDQxmsZwGzjsaX7w"),
                Uuid.fromString("s8rHMluuSDCnxt3FmKwiyw")
        );
        sortedDirs.sort(Uuid::compareTo);
        List<Uuid> emptySortedDirs = Collections.emptyList();

        // When/Then
        assertTrue(DirectoryId.isOnline(Uuid.fromString("imQKg2cXTVe8OUFNa3R9bg"), sortedDirs));
        assertTrue(DirectoryId.isOnline(Uuid.fromString("Mwy5wxTDQxmsZwGzjsaX7w"), sortedDirs));
        assertTrue(DirectoryId.isOnline(Uuid.fromString("s8rHMluuSDCnxt3FmKwiyw"), sortedDirs));
        assertTrue(DirectoryId.isOnline(DirectoryId.MIGRATING, sortedDirs));
        assertTrue(DirectoryId.isOnline(DirectoryId.UNASSIGNED, sortedDirs));
        assertFalse(DirectoryId.isOnline(DirectoryId.LOST, sortedDirs));
        assertFalse(DirectoryId.isOnline(Uuid.fromString("AMYchbMtS6yhtsXbca7DQg"), sortedDirs));
        assertTrue(DirectoryId.isOnline(Uuid.randomUuid(), emptySortedDirs));
    }
}
