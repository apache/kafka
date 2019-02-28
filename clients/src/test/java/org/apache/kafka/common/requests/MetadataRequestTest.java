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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetadataRequestTest {

    @Test
    public void testEmptyMeansAllTopicsV0() {
        Struct rawRequest = new Struct(MetadataRequest.schemaVersions()[0]);
        rawRequest.set("topics", new Object[0]);
        MetadataRequest parsedRequest = new MetadataRequest(rawRequest, (short) 0);
        assertTrue(parsedRequest.isAllTopics());
        assertNull(parsedRequest.topics());
    }

    @Test
    public void testEmptyMeansEmptyForVersionsAboveV0() {
        for (int i = 1; i < MetadataRequest.schemaVersions().length; i++) {
            Schema schema = MetadataRequest.schemaVersions()[i];
            Struct rawRequest = new Struct(schema);
            rawRequest.set("topics", new Object[0]);
            if (rawRequest.hasField("allow_auto_topic_creation"))
                rawRequest.set("allow_auto_topic_creation", true);
            MetadataRequest parsedRequest = new MetadataRequest(rawRequest, (short) i);
            assertFalse(parsedRequest.isAllTopics());
            assertEquals(Collections.emptyList(), parsedRequest.topics());
        }
    }

}
