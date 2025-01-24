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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 40)
public class BrokerRegistrationTrackerTest {
    static final Uuid INCARNATION_ID = Uuid.fromString("jyjLbk31Tpa53pFrU9Y-Ng");

    static final Uuid A = Uuid.fromString("Ahw3vXfnThqeZbb7HD1w6Q");

    static final Uuid B = Uuid.fromString("BjOacT0OTNqIvUWIlKhahg");

    static final Uuid C = Uuid.fromString("CVHi_iv2Rvy5_1rtPdasfg");

    static class BrokerRegistrationTrackerTestContext {
        AtomicInteger numCalls = new AtomicInteger(0);
        BrokerRegistrationTracker tracker = new BrokerRegistrationTracker(1, () -> numCalls.incrementAndGet());

        MetadataImage image = MetadataImage.EMPTY;

        void onMetadataUpdate(MetadataDelta delta) {
            MetadataProvenance provenance = new MetadataProvenance(0, 0, 0, true);
            image = delta.apply(provenance);
            LogDeltaManifest manifest = new LogDeltaManifest.Builder().
                provenance(provenance).
                leaderAndEpoch(LeaderAndEpoch.UNKNOWN).
                numBatches(1).
                elapsedNs(1).
                numBytes(1).
                build();
            tracker.onMetadataUpdate(delta, image, manifest);
        }

        MetadataDelta newDelta() {
            return new MetadataDelta.Builder().
                setImage(image).
                build();
        }
    }

    @Test
    public void testTrackerName() {
        BrokerRegistrationTrackerTestContext ctx  = new BrokerRegistrationTrackerTestContext();
        assertEquals("BrokerRegistrationTracker(id=1)", ctx.tracker.name());
    }

    @Test
    public void testMetadataVersionUpdateWithoutRegistrationDoesNothing() {
        BrokerRegistrationTrackerTestContext ctx  = new BrokerRegistrationTrackerTestContext();
        MetadataDelta delta = ctx.newDelta();
        delta.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.IBP_3_7_IV2.featureLevel()));
        ctx.onMetadataUpdate(delta);
        assertEquals(0, ctx.numCalls.get());
    }

    @Test
    public void testBrokerUpdateWithoutNewMvDoesNothing() {
        BrokerRegistrationTrackerTestContext ctx  = new BrokerRegistrationTrackerTestContext();
        MetadataDelta delta = ctx.newDelta();
        delta.replay(new RegisterBrokerRecord().
            setBrokerId(1).
            setIncarnationId(INCARNATION_ID).
            setLogDirs(Arrays.asList(A, B, C)));
        ctx.onMetadataUpdate(delta);
        assertEquals(0, ctx.numCalls.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBrokerUpdateWithNewMv(boolean jbodMv) {
        BrokerRegistrationTrackerTestContext ctx  = new BrokerRegistrationTrackerTestContext();
        MetadataDelta delta = ctx.newDelta();
        delta.replay(new RegisterBrokerRecord().
            setBrokerId(1).
            setIncarnationId(INCARNATION_ID).
            setLogDirs(Collections.emptyList()));
        delta.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(jbodMv ? MetadataVersion.IBP_3_7_IV2.featureLevel() :
                MetadataVersion.IBP_3_7_IV1.featureLevel()));
        ctx.onMetadataUpdate(delta);
        if (jbodMv) {
            assertEquals(1, ctx.numCalls.get());
        } else {
            assertEquals(0, ctx.numCalls.get());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBrokerUpdateWithNewMvWithTwoDeltas(boolean jbodMv) {
        BrokerRegistrationTrackerTestContext ctx  = new BrokerRegistrationTrackerTestContext();
        MetadataDelta delta = ctx.newDelta();
        delta.replay(new RegisterBrokerRecord().
            setBrokerId(1).
            setIncarnationId(INCARNATION_ID).
            setLogDirs(Collections.emptyList()));
        ctx.onMetadataUpdate(delta);
        // No calls are made because MetadataVersion is 3.0-IV1 initially
        assertEquals(0, ctx.numCalls.get());

        delta = ctx.newDelta();
        delta.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(jbodMv ? MetadataVersion.IBP_3_7_IV2.featureLevel() :
                MetadataVersion.IBP_3_7_IV1.featureLevel()));
        ctx.onMetadataUpdate(delta);
        if (jbodMv) {
            assertEquals(1, ctx.numCalls.get());
        } else {
            assertEquals(0, ctx.numCalls.get());
        }
    }
}
