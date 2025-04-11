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

package org.apache.kafka.server.common;

import org.apache.kafka.common.record.ControlRecordUtils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class KRaftVersionTest {
    @Test
    public void testFeatureLevel() {
        for (int i = 0; i < KRaftVersion.values().length; i++) {
            assertEquals(i, KRaftVersion.values()[i].featureLevel());
        }
    }

    @Test
    public void testQuorumStateVersion() {
        for (int i = 0; i < KRaftVersion.values().length; i++) {
            assertEquals(i, KRaftVersion.values()[i].quorumStateVersion());
        }
    }

    @Test
    public void testFromFeatureLevel() {
        for (int i = 0; i < KRaftVersion.values().length; i++) {
            assertEquals(KRaftVersion.values()[i], KRaftVersion.fromFeatureLevel((short) i));
        }
    }

    @Test
    public void testBootstrapMetadataVersion() {
        for (int i = 0; i < KRaftVersion.values().length; i++) {
            MetadataVersion metadataVersion = KRaftVersion.values()[i].bootstrapMetadataVersion();
            switch (i) {
                case 0:
                    assertEquals(MetadataVersion.MINIMUM_VERSION, metadataVersion);
                    break;
                case 1:
                    assertEquals(MetadataVersion.IBP_3_9_IV0, metadataVersion);
                    break;
                default:
                    throw new RuntimeException("Unsupported value " + i);
            }
        }
    }

    @Test
    public void testKraftVersionRecordVersion() {
        for (KRaftVersion kraftVersion : KRaftVersion.values()) {
            switch (kraftVersion) {
                case KRAFT_VERSION_0:
                    assertThrows(
                        IllegalStateException.class,
                        () -> kraftVersion.kraftVersionRecordVersion()
                    );
                    break;

                case KRAFT_VERSION_1:
                    assertEquals(
                        ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION,
                        kraftVersion.kraftVersionRecordVersion()
                    );
                    break;

                default:
                    throw new RuntimeException("Unsupported value " + kraftVersion);
            }
        }
    }

    @Test
    public void tesVotersRecordVersion() {
        for (KRaftVersion kraftVersion : KRaftVersion.values()) {
            switch (kraftVersion) {
                case KRAFT_VERSION_0:
                    assertThrows(
                        IllegalStateException.class,
                        () -> kraftVersion.votersRecordVersion()
                    );
                    break;

                case KRAFT_VERSION_1:
                    assertEquals(
                        ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION,
                        kraftVersion.votersRecordVersion()
                    );
                    break;

                default:
                    throw new RuntimeException("Unsupported value " + kraftVersion);
            }
        }
    }
}
