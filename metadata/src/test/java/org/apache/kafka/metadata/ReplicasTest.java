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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class ReplicasTest {
    @Test
    public void testToList() {
        assertEquals(Arrays.asList(1, 2, 3, 4), Replicas.toList(new int[] {1, 2, 3, 4}));
        assertEquals(Arrays.asList(), Replicas.toList(Replicas.NONE));
        assertEquals(Arrays.asList(2), Replicas.toList(new int[] {2}));
    }

    @Test
    public void testToArray() {
        assertArrayEquals(new int[] {3, 2, 1}, Replicas.toArray(Arrays.asList(3, 2, 1)));
        assertArrayEquals(new int[] {}, Replicas.toArray(Arrays.asList()));
        assertArrayEquals(new int[] {2}, Replicas.toArray(Arrays.asList(2)));
    }

    @Test
    public void testClone() {
        assertArrayEquals(new int[]{3, 2, 1}, Replicas.clone(new int[]{3, 2, 1}));
        assertArrayEquals(new int[]{}, Replicas.clone(new int[]{}));
        assertArrayEquals(new int[]{2}, Replicas.clone(new int[]{2}));
    }

    @Test
    public void testValidateIsr() {
        assertTrue(Replicas.validateIsr(Replicas.withUnknownDirs(new int[] {}), new int[] {}));
        assertTrue(Replicas.validateIsr(Replicas.withUnknownDirs(new int[] {1, 2, 3}), new int[] {}));
        assertTrue(Replicas.validateIsr(Replicas.withUnknownDirs(new int[] {1, 2, 3}), new int[] {1, 2, 3}));
        assertTrue(Replicas.validateIsr(Replicas.withUnknownDirs(new int[] {3, 1, 2}), new int[] {2, 1}));
        assertFalse(Replicas.validateIsr(Replicas.withUnknownDirs(new int[] {3, 1, 2}), new int[] {4, 1}));
        assertFalse(Replicas.validateIsr(Replicas.withUnknownDirs(new int[] {1, 2, 4}), new int[] {4, 4}));
    }

    @Test
    public void testContains() {
        assertTrue(Replicas.contains(new int[] {3, 0, 1}, 0));
        assertFalse(Replicas.contains(new int[] {}, 0));
        assertTrue(Replicas.contains(new int[] {1}, 1));
    }

    @Test
    public void testCopyWithout() {
        assertArrayEquals(new int[] {}, Replicas.copyWithout(new int[] {}, 0));
        assertArrayEquals(new int[] {}, Replicas.copyWithout(new int[] {1}, 1));
        assertArrayEquals(new int[] {1, 3}, Replicas.copyWithout(new int[] {1, 2, 3}, 2));
        assertArrayEquals(new int[] {4, 1}, Replicas.copyWithout(new int[] {4, 2, 2, 1}, 2));
    }

    @Test
    public void testCopyWithout2() {
        assertArrayEquals(new int[] {}, Replicas.copyWithout(new int[] {}, new int[] {}));
        assertArrayEquals(new int[] {}, Replicas.copyWithout(new int[] {1}, new int[] {1}));
        assertArrayEquals(new int[] {1, 3},
            Replicas.copyWithout(new int[] {1, 2, 3}, new int[]{2, 4}));
        assertArrayEquals(new int[] {4},
            Replicas.copyWithout(new int[] {4, 2, 2, 1}, new int[]{2, 1}));
    }

    @Test
    public void testCopyWith() {
        assertArrayEquals(new int[] {-1}, Replicas.copyWith(new int[] {}, -1));
        assertArrayEquals(new int[] {1, 2, 3, 4}, Replicas.copyWith(new int[] {1, 2, 3}, 4));
    }

    @Test
    public void testToSet() {
        assertEquals(Collections.emptySet(), Replicas.toSet(new int[] {}));
        assertEquals(new HashSet<>(Arrays.asList(3, 1, 5)),
            Replicas.toSet(new int[] {1, 3, 5}));
        assertEquals(new HashSet<>(Arrays.asList(1, 2, 10)),
            Replicas.toSet(new int[] {1, 1, 2, 10, 10}));
    }

    @Test
    public void testContains2() {
        assertTrue(Replicas.contains(Collections.emptyList(), Replicas.NONE));
        assertFalse(Replicas.contains(Collections.emptyList(), new int[] {1}));
        assertTrue(Replicas.contains(Arrays.asList(1, 2, 3), new int[] {3, 2, 1}));
        assertTrue(Replicas.contains(Arrays.asList(1, 2, 3, 4), new int[] {3}));
        assertTrue(Replicas.contains(Arrays.asList(1, 2, 3, 4), new int[] {3, 1}));
        assertFalse(Replicas.contains(Arrays.asList(1, 2, 3, 4), new int[] {3, 1, 7}));
        assertTrue(Replicas.contains(Arrays.asList(1, 2, 3, 4), new int[] {}));
    }

    static Replica[] replicas(Object... brokerIdsAndDirUuids) {
        if (brokerIdsAndDirUuids.length % 2 != 0) throw new IllegalArgumentException("odd arg count");
        Replica[] replicas = new Replica[brokerIdsAndDirUuids.length / 2];
        for (int i = 0; i < replicas.length; i++) {
            int brokerId = (int) brokerIdsAndDirUuids[i * 2];
            Object uuidObj = brokerIdsAndDirUuids[i * 2 + 1];
            Uuid uuid = uuidObj instanceof Uuid ? (Uuid) uuidObj : Uuid.fromString((String) uuidObj);
            replicas[i] = new Replica(brokerId, uuid);
        }
        return replicas;
    }

    @Test
    public void testUpdate() {
        assertArrayEquals(replicas(), Replicas.update(replicas(), replicas()));
        assertArrayEquals(replicas(), Replicas.update(replicas(
                4, "8ahIcaOIQgKyHKEkloD5mA"
        ), replicas()));
        assertArrayEquals(replicas(
                5, "n1bUyxRKSGCgnJOliMAmcg"
        ), Replicas.update(replicas(), replicas(
                5, "n1bUyxRKSGCgnJOliMAmcg"
        )));
        assertArrayEquals(
                replicas(
                        1, "y5lrQJLhQhq8ZSnGZij96Q",
                        3, "UJgXkoc7TomwzVJ8Jk4wow",
                        4, "8ahIcaOIQgKyHKEkloD5mA",
                        5, Uuid.OFFLINE_DIR,
                        6, Uuid.UNKNOWN_DIR
                ),
                Replicas.update(
                        replicas(
                                1, Uuid.UNKNOWN_DIR,
                                2, "y6FNZwzBTxyGHQXPI7y8FQ",
                                4, "8ahIcaOIQgKyHKEkloD5mA",
                                5, "n1bUyxRKSGCgnJOliMAmcg"
                        ),
                        replicas(
                                1, "y5lrQJLhQhq8ZSnGZij96Q",
                                3, "UJgXkoc7TomwzVJ8Jk4wow",
                                4, Uuid.UNKNOWN_DIR,
                                5, Uuid.OFFLINE_DIR,
                                6, Uuid.UNKNOWN_DIR
                        )
                )
        );
    }

    @Test
    void testBrokerIds() {
        assertArrayEquals(new int[0], Replicas.brokerIds(replicas()));
        assertArrayEquals(
                new int[] {8, 4, 2},
                Replicas.brokerIds(replicas(
                        8, "ZczWN1enTzieF6WiCuxWuA",
                        4, Uuid.OFFLINE_DIR,
                        2, Uuid.UNKNOWN_DIR))
        );
        assertEquals(Collections.emptyList(), Replicas.brokerIdsList(replicas()));
        assertEquals(
                Arrays.asList(1, 7, 3),
                Replicas.brokerIdsList(replicas(
                        1, Uuid.UNKNOWN_DIR,
                        7, "9wxk8mlUTcSDeezs176uQQ",
                        3, "abjErKbhTZ63lLFRM1mwOA"))
        );
        assertEquals(Collections.emptyList(), Replicas.brokerIdsList(Collections.emptyList()));
        assertEquals(
                Arrays.asList(9, 3, 5),
                Replicas.brokerIdsList(Arrays.asList(replicas(
                        9, "ljHCEJKORMKc550c2wdjcQ",
                        3, "dw7mdSeiSFaAMd9seSssqw",
                        5, Uuid.SELECTED_DIR)))
        );
    }

    @Test
    void testWithUnkownDirs() {
        assertArrayEquals(replicas(), Replicas.withUnknownDirs(new int[0]));
        assertArrayEquals(replicas(
                7, Uuid.UNKNOWN_DIR,
                3, Uuid.UNKNOWN_DIR,
                6, Uuid.UNKNOWN_DIR
        ), Replicas.withUnknownDirs(new int[] {7, 3, 6}));
        assertArrayEquals(replicas(), Replicas.withUnknownDirs(Collections.emptyList()));
        assertArrayEquals(replicas(
                9, Uuid.UNKNOWN_DIR,
                4, Uuid.UNKNOWN_DIR,
                1, Uuid.UNKNOWN_DIR
        ), Replicas.withUnknownDirs(Arrays.asList(9, 4, 1)));
    }

    @Test
    void testFromRecord() {
        assertArrayEquals(replicas(), Replicas.fromRecord(new PartitionRecord()));
        assertArrayEquals(
                replicas(
                        4, Uuid.UNKNOWN_DIR,
                        3, Uuid.UNKNOWN_DIR,
                        2, Uuid.UNKNOWN_DIR
                ),
                Replicas.fromRecord(
                        new PartitionRecord().
                                setReplicas(Arrays.asList(4, 3, 2))
                ));
        assertArrayEquals(replicas(), Replicas.fromRecord(new PartitionRecord()));
        assertArrayEquals(
                replicas(
                        4, "MGbbRZEeSKK7ii5sz0WgLA",
                        3, Uuid.OFFLINE_DIR,
                        2, "qJrCGfUEQgGOcS61esUg8w"
                ),
                Replicas.fromRecord(
                        new PartitionRecord().
                                setAssignment(Arrays.asList(
                                        new PartitionRecord.ReplicaAssignment().
                                                setBroker(4).
                                                setDirectory(Uuid.fromString("MGbbRZEeSKK7ii5sz0WgLA")),
                                        new PartitionRecord.ReplicaAssignment().
                                                setBroker(3).
                                                setDirectory(Uuid.OFFLINE_DIR),
                                        new PartitionRecord.ReplicaAssignment().
                                                setBroker(2).
                                                setDirectory(Uuid.fromString("qJrCGfUEQgGOcS61esUg8w"))
                                ))
                ));
        assertArrayEquals(replicas(), Replicas.fromRecord(new PartitionChangeRecord()));
        assertArrayEquals(
                replicas(
                        4, Uuid.UNKNOWN_DIR,
                        3, Uuid.UNKNOWN_DIR,
                        2, Uuid.UNKNOWN_DIR
                ),
                Replicas.fromRecord(
                        new PartitionChangeRecord().
                                setReplicas(Arrays.asList(4, 3, 2))
                ));
        assertArrayEquals(replicas(), Replicas.fromRecord(new PartitionChangeRecord()));
        assertArrayEquals(
                replicas(
                        4, "MGbbRZEeSKK7ii5sz0WgLA",
                        3, Uuid.OFFLINE_DIR,
                        2, "qJrCGfUEQgGOcS61esUg8w"
                ),
                Replicas.fromRecord(
                        new PartitionChangeRecord().
                                setAssignment(Arrays.asList(
                                        new PartitionChangeRecord.ReplicaAssignment().
                                                setBroker(4).
                                                setDirectory(Uuid.fromString("MGbbRZEeSKK7ii5sz0WgLA")),
                                        new PartitionChangeRecord.ReplicaAssignment().
                                                setBroker(3).
                                                setDirectory(Uuid.OFFLINE_DIR),
                                        new PartitionChangeRecord.ReplicaAssignment().
                                                setBroker(2).
                                                setDirectory(Uuid.fromString("qJrCGfUEQgGOcS61esUg8w"))
                                ))
                ));
    }

    @Test
    void testToRecordReplicaAssignment() {
        assertEquals(
                Collections.emptyList(),
                Replicas.toPartitionRecordReplicaAssignment(replicas())
        );
        assertEquals(
                Arrays.asList(
                        new PartitionRecord.ReplicaAssignment().
                                setBroker(4).
                                setDirectory(Uuid.fromString("MGbbRZEeSKK7ii5sz0WgLA")),
                        new PartitionRecord.ReplicaAssignment().
                                setBroker(3).
                                setDirectory(Uuid.OFFLINE_DIR),
                        new PartitionRecord.ReplicaAssignment().
                                setBroker(2).
                                setDirectory(Uuid.fromString("qJrCGfUEQgGOcS61esUg8w"))
                ),
                Replicas.toPartitionRecordReplicaAssignment(replicas(
                        4, "MGbbRZEeSKK7ii5sz0WgLA",
                        3, Uuid.OFFLINE_DIR,
                        2, "qJrCGfUEQgGOcS61esUg8w"
                ))
        );
        assertEquals(
                Collections.emptyList(),
                Replicas.toPartitionChangeRecordReplicaAssignment(replicas())
        );
        assertEquals(
                Arrays.asList(
                        new PartitionChangeRecord.ReplicaAssignment().
                                setBroker(4).
                                setDirectory(Uuid.fromString("MGbbRZEeSKK7ii5sz0WgLA")),
                        new PartitionChangeRecord.ReplicaAssignment().
                                setBroker(3).
                                setDirectory(Uuid.OFFLINE_DIR),
                        new PartitionChangeRecord.ReplicaAssignment().
                                setBroker(2).
                                setDirectory(Uuid.fromString("qJrCGfUEQgGOcS61esUg8w"))
                ),
                Replicas.toPartitionChangeRecordReplicaAssignment(replicas(
                        4, "MGbbRZEeSKK7ii5sz0WgLA",
                        3, Uuid.OFFLINE_DIR,
                        2, "qJrCGfUEQgGOcS61esUg8w"
                ))
        );
    }
}
