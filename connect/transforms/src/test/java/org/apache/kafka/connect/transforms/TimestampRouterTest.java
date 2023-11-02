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
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimestampRouterTest {
    private TimestampRouter<SourceRecord> xform;
    @BeforeEach
    public void setup() {
        xform = new TimestampRouter<>();
        xform.configure(Collections.emptyMap()); // defaults
    }

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void defaultConfiguration() {
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, null,
                1483425001864L
        );
        assertEquals("test-20170103", xform.apply(record).topic());
    }

    @Test
    public void testTimestampRouterVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xform.version());
    }

}
