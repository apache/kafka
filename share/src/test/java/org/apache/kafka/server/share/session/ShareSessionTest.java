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
package org.apache.kafka.server.share.session;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShareSessionTest {

    @Test
    public void testPartitionsToLogString() {
        Uuid uuid1 = Uuid.randomUuid();
        Uuid uuid2 = Uuid.randomUuid();
        List<TopicIdPartition> partitions = Arrays.asList(
            new TopicIdPartition(uuid1, 0, "foo"),
            new TopicIdPartition(uuid2, 1, "bar"));

        String response = ShareSession.partitionsToLogString(partitions, false);
        assertEquals("2 partition(s)", response);

        response = ShareSession.partitionsToLogString(partitions, true);
        assertEquals(String.format("( [%s:foo-0, %s:bar-1] )", uuid1, uuid2), response);
    }

    @Test
    public void testPartitionsToLogStringEmpty() {
        String response = ShareSession.partitionsToLogString(Collections.emptyList(), false);
        assertEquals("0 partition(s)", response);

        response = ShareSession.partitionsToLogString(Collections.emptyList(), true);
        assertEquals("( [] )", response);
    }
}
