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
package org.apache.kafka.connect.runtime.rest.entities;

import org.apache.kafka.connect.util.ConnectorTaskId;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorInfoTest {

    // A bare ObjectMapper, matching what RestClient and the EmbeddedConnect test fixture use, so these tests exercise
    // the same (de)serialization behaviour as the follower -> leader forwarding path.
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String NAME = "source-1";
    private static final Map<String, String> CONFIG = Map.of("connector.class", "FileStreamSource");
    private static final List<ConnectorTaskId> TASKS = List.of(new ConnectorTaskId(NAME, 0));

    @Test
    public void testConvenienceConstructorLeavesOffsetsStatusNull() {
        ConnectorInfo info = new ConnectorInfo(NAME, CONFIG, TASKS, ConnectorType.SOURCE);
        assertNull(info.offsetsStatus());
    }

    @Test
    public void testOffsetsStatusOmittedWhenNull() throws Exception {
        // Endpoints other than POST /connectors never set an offsets status, so their response bodies must be
        // byte-for-byte unchanged by the addition of this field.
        String serialized = OBJECT_MAPPER.writeValueAsString(new ConnectorInfo(NAME, CONFIG, TASKS, ConnectorType.SOURCE));

        assertFalse(serialized.contains("offsets_status"),
            "offsets_status must be omitted when null: " + serialized);
        assertTrue(serialized.contains("\"name\""));
        assertTrue(serialized.contains("\"config\""));
        assertTrue(serialized.contains("\"tasks\""));
        assertTrue(serialized.contains("\"type\""));
    }

    @Test
    public void testOffsetsStatusPresentWhenSet() throws Exception {
        String status = "The offsets for this connector have been set successfully";
        String serialized = OBJECT_MAPPER.writeValueAsString(
            new ConnectorInfo(NAME, CONFIG, TASKS, ConnectorType.SOURCE, status));

        assertTrue(serialized.contains("\"offsets_status\""), serialized);
        assertTrue(serialized.contains(status), serialized);
    }

    @Test
    public void testRoundTripWithOffsetsStatus() throws Exception {
        // Deserialization matters because a worker that is not the leader forwards POST /connectors and parses the
        // leader's response body into this type with a bare ObjectMapper.
        ConnectorInfo original = new ConnectorInfo(NAME, CONFIG, TASKS, ConnectorType.SOURCE, "offsets set");

        String serialized = OBJECT_MAPPER.writeValueAsString(original);
        ConnectorInfo roundTripped = OBJECT_MAPPER.readValue(serialized, ConnectorInfo.class);

        assertEquals(original, roundTripped);
        assertEquals("offsets set", roundTripped.offsetsStatus());
    }

    @Test
    public void testRoundTripWithoutOffsetsStatus() throws Exception {
        ConnectorInfo original = new ConnectorInfo(NAME, CONFIG, TASKS, ConnectorType.SOURCE);

        String serialized = OBJECT_MAPPER.writeValueAsString(original);
        ConnectorInfo roundTripped = OBJECT_MAPPER.readValue(serialized, ConnectorInfo.class);

        assertEquals(original, roundTripped);
        assertNull(roundTripped.offsetsStatus());
    }

    @Test
    public void testDeserializeLegacyBodyWithoutOffsetsStatus() throws Exception {
        // A response produced by an older worker has no offsets_status field at all. Parsing it must succeed, since a
        // new follower may forward a request to an old leader during a rolling upgrade.
        String json = "{\"name\": \"source-1\", \"config\": {\"connector.class\": \"FileStreamSource\"},"
            + " \"tasks\": [{\"connector\": \"source-1\", \"task\": 0}], \"type\": \"source\"}";

        ConnectorInfo info = OBJECT_MAPPER.readValue(json, ConnectorInfo.class);

        assertEquals(NAME, info.name());
        assertEquals(TASKS, info.tasks());
        assertNull(info.offsetsStatus());
    }

    @Test
    public void testWithOffsetsStatus() {
        ConnectorInfo info = new ConnectorInfo(NAME, CONFIG, TASKS, ConnectorType.SOURCE);
        ConnectorInfo withStatus = info.withOffsetsStatus("offsets set");

        assertNull(info.offsetsStatus(), "withOffsetsStatus must not mutate the original");
        assertEquals("offsets set", withStatus.offsetsStatus());
        assertEquals(info.name(), withStatus.name());
        assertEquals(info.config(), withStatus.config());
        assertEquals(info.tasks(), withStatus.tasks());
        assertEquals(info.type(), withStatus.type());
    }
}
