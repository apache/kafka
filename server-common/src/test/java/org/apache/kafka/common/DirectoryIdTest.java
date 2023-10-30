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
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectoryIdTest {

    @Test
    void testReserved() {
        Set<Long> seen = new HashSet<>(100);
        for (DirectoryId reservedId : DirectoryId.RESERVED) {
            assertEquals(0L, reservedId.getMostSignificantBits(), "Unexpected reserved msb value");
            long lsb = reservedId.getLeastSignificantBits();
            assertTrue(lsb >= 0 && lsb < 100L, "Unexpected reserved lsb value");
            assertTrue(seen.add(lsb), "Duplicate reserved value");
        }
        assertEquals(100, DirectoryId.RESERVED.size());
    }

    @Test
    void testToArray() {
        assertNull(DirectoryId.toArray(null));
        assertArrayEquals(
                new DirectoryId[]{
                    DirectoryId.MIGRATING, DirectoryId.fromString("UXyU9i5ARn6W00ON2taeWA")
                },
                DirectoryId.toArray(Arrays.asList(
                    DirectoryId.MIGRATING, DirectoryId.fromString("UXyU9i5ARn6W00ON2taeWA")
                ))
        );
    }

    @Test
    void testToList() {
        assertNull(DirectoryId.toList(null));
        assertEquals(
            Arrays.asList(
                DirectoryId.MIGRATING, DirectoryId.fromString("UXyU9i5ARn6W00ON2taeWA")
            ),
            DirectoryId.toList(new DirectoryId[]{
                DirectoryId.MIGRATING, DirectoryId.fromString("UXyU9i5ARn6W00ON2taeWA")
            })
        );
    }

    @Test
    void testCreateDirectoriesFrom() {
        assertThrows(IllegalArgumentException.class, () -> DirectoryId.createDirectoriesFrom(
                new int[] {1},
                new DirectoryId[] {DirectoryId.UNASSIGNED, DirectoryId.LOST},
                Arrays.asList(2, 3)
        ));
        assertEquals(
            Arrays.asList(
                DirectoryId.fromString("YXY0bQYEQmmyOQ6ZDfGgSQ"),
                DirectoryId.fromString("5SZij3DRQgaFbvzR9KooLg"),
                DirectoryId.UNASSIGNED
            ),
            DirectoryId.createDirectoriesFrom(
                new int[] {1, 2, 3},
                new DirectoryId[] {
                        DirectoryId.fromString("MgVK5KSwTxe65eYATaoQrg"),
                        DirectoryId.fromString("YXY0bQYEQmmyOQ6ZDfGgSQ"),
                        DirectoryId.fromString("5SZij3DRQgaFbvzR9KooLg")
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
                        new DirectoryId[] {
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
            new HashMap<Integer, DirectoryId>() {{
                    put(1, DirectoryId.fromString("upjfkCrUR9GNn1i94ip1wg"));
                    put(2, DirectoryId.fromString("bCF3l0RIQjOKhUqgbivHZA"));
                    put(3, DirectoryId.fromString("Fg3mFhcVQlqCWRk4dZazxw"));
                    put(4, DirectoryId.fromString("bv9TEYi4TqOm52hLmrxT5w"));
                }},
            DirectoryId.createAssignmentMap(
                    new int[] {1, 2, 3, 4},
                    new DirectoryId[] {
                            DirectoryId.fromString("upjfkCrUR9GNn1i94ip1wg"),
                            DirectoryId.fromString("bCF3l0RIQjOKhUqgbivHZA"),
                            DirectoryId.fromString("Fg3mFhcVQlqCWRk4dZazxw"),
                            DirectoryId.fromString("bv9TEYi4TqOm52hLmrxT5w")
                    })
        );
    }
}
