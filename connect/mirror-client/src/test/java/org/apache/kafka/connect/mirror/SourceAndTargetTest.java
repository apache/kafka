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
package org.apache.kafka.connect.mirror;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class SourceAndTargetTest {
    @Test
    public void testEquals() {
        SourceAndTarget sourceAndTarget = new SourceAndTarget("source", "target");
        SourceAndTarget sourceAndTarget2 = new SourceAndTarget("source", "target");
        SourceAndTarget sourceAndTarget3 = new SourceAndTarget("error-source", "target");
        assertEquals(sourceAndTarget, sourceAndTarget2);
        assertNotEquals(sourceAndTarget, sourceAndTarget3);

        class FakeSourceAndTarget {
            private final String source;
            private final String target;

            public FakeSourceAndTarget(String source, String target) {
                this.source = source;
                this.target = target;
            }

            @Override
            public String toString() {
                return source + "->" + target;
            }
        }
        FakeSourceAndTarget fakeSourceAndTarget = new FakeSourceAndTarget("source", "target");
        assertNotEquals(sourceAndTarget, fakeSourceAndTarget);
    }
}
