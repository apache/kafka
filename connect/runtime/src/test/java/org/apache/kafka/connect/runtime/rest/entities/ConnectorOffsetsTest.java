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

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ConnectorOffsetsTest {

    @Test
    public void testConnectorOffsetsToMap() {
        // Using arbitrary partition and offset formats here to demonstrate that source connector offsets don't
        // follow a standard pattern
        Map<String, Object> partition1 = new HashMap<>();
        partition1.put("partitionKey1", "partitionValue");
        partition1.put("k", 123);
        Map<String, Object> offset1 = new HashMap<>();
        offset1.put("offset", 3.14);
        ConnectorOffset connectorOffset1 = new ConnectorOffset(partition1, offset1);

        Map<String, Object> partition2 = new HashMap<>();
        partition2.put("partitionKey1", true);
        Map<String, Object> offset2 = new HashMap<>();
        offset2.put("offset", new byte[]{0x00, 0x1A});
        ConnectorOffset connectorOffset2 = new ConnectorOffset(partition2, offset2);

        ConnectorOffsets connectorOffsets = new ConnectorOffsets(Arrays.asList(connectorOffset1, connectorOffset2));
        Map<Map<String, ?>, Map<String, ?>> connectorOffsetsMap = connectorOffsets.toMap();
        assertEquals(2, connectorOffsetsMap.size());
        assertEquals(offset1, connectorOffsetsMap.get(partition1));
        assertEquals(offset2, connectorOffsetsMap.get(partition2));
    }
}
