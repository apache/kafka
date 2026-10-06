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

package org.apache.kafka.image;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.metadata.RemoveUserScramCredentialRecord;
import org.apache.kafka.metadata.ScramCredentialData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_256;
import static org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_512;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for {@link ScramDelta#apply()}.
 *
 * <p>Absent REMOVE records must not detach the per-mechanism user map before later UPSERTs
 * in the same batch are applied.
 */
@Timeout(value = 40)
public class ScramDeltaTest {

    @Test
    public void testApplyWithNoChangesReturnsBaseImage() {
        ScramImage base = image(SCRAM_SHA_256, "user-admin");
        assertEquals(base, new ScramDelta(base).apply());
    }

    @Test
    public void testAbsentRemovesBeforeUpsertPreservesUsers() {
        ScramDelta delta = new ScramDelta(ScramImage.EMPTY);
        replayRemove(delta, "producer");
        replayRemove(delta, "consumer");
        replayUpsert(delta, SCRAM_SHA_256, "user-admin");
        replayRemove(delta, "update-user");
        replayRemove(delta, "ext-user");
        replayUpsert(delta, SCRAM_SHA_512, "cluster-admin");

        ScramImage result = delta.apply();

        assertUserNames(result, SCRAM_SHA_256, "user-admin");
        assertUserNames(result, SCRAM_SHA_512, "cluster-admin");
    }

    @Test
    public void testAbsentRemovesOnlyLeaveImageEmpty() {
        ScramDelta delta = new ScramDelta(ScramImage.EMPTY);
        replayRemove(delta, "producer");
        replayRemove(delta, "consumer");

        assertTrue(delta.apply().isEmpty());
    }

    @Test
    public void testRemoveLastExistingUserClearsImage() {
        ScramImage base = image(SCRAM_SHA_256, "user-admin");
        ScramDelta delta = new ScramDelta(base);
        replayRemove(delta, "user-admin");

        assertTrue(delta.apply().isEmpty());
    }

    private static ScramImage image(ScramMechanism mechanism, String... userNames) {
        Map<String, ScramCredentialData> users = Arrays.stream(userNames)
            .collect(Collectors.toMap(name -> name, ScramDeltaTest::credential));
        return new ScramImage(Map.of(mechanism, users));
    }

    private static ScramCredentialData credential(String userName) {
        int seed = userName.hashCode();
        byte[] bytes = new byte[] {
            (byte) seed,
            (byte) (seed >> 8),
            (byte) (seed >> 16)
        };
        return new ScramCredentialData(bytes, bytes, bytes, 4096);
    }

    private static void replayUpsert(ScramDelta delta, ScramMechanism mechanism, String userName) {
        ScramCredentialData data = credential(userName);
        delta.replay(data.toRecord(userName, mechanism));
    }

    private static void replayRemove(ScramDelta delta, String userName) {
        delta.replay(new RemoveUserScramCredentialRecord()
            .setName(userName)
            .setMechanism(ScramMechanism.SCRAM_SHA_256.type()));
    }

    private static void assertUserNames(ScramImage image, ScramMechanism mechanism, String... userNames) {
        assertEquals(
            Arrays.stream(userNames).collect(Collectors.toSet()),
            image.mechanisms().get(mechanism).keySet()
        );
    }
}
