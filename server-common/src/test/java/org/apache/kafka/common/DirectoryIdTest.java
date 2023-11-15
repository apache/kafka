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
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void testCreateDirectoriesFrom() {
        assertThrows(IllegalArgumentException.class, () -> DirectoryId.createDirectoriesFrom(
                new int[] {1},
                new Uuid[] {DirectoryId.UNASSIGNED, DirectoryId.LOST},
                Arrays.asList(2, 3)
        ));
        assertEquals(
            Arrays.asList(
                Uuid.fromString("YXY0bQYEQmmyOQ6ZDfGgSQ"),
                Uuid.fromString("5SZij3DRQgaFbvzR9KooLg"),
                DirectoryId.UNASSIGNED
            ),
            DirectoryId.createDirectoriesFrom(
                new int[] {1, 2, 3},
                new Uuid[] {
                        Uuid.fromString("MgVK5KSwTxe65eYATaoQrg"),
                        Uuid.fromString("YXY0bQYEQmmyOQ6ZDfGgSQ"),
                        Uuid.fromString("5SZij3DRQgaFbvzR9KooLg")
                },
                Arrays.asList(2, 3, 4)
            )
        );
        assertEquals(
                Arrays.asList(
                        DirectoryId.UNASSIGNED,
                        DirectoryId.UNASSIGNED,
                        DirectoryId.UNASSIGNED
                ),
                DirectoryId.createDirectoriesFrom(
                        new int[] {1, 2},
                        new Uuid[] {
                            DirectoryId.UNASSIGNED,
                            DirectoryId.UNASSIGNED
                        },
                        Arrays.asList(1, 2, 3)
                )
        );
    }

    @Test
    void testCreateAssignmentMap() {
        assertThrows(IllegalArgumentException.class,
                () -> DirectoryId.createAssignmentMap(new int[]{1, 2}, DirectoryId.unassignedArray(3)));
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
}
