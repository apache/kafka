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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WriteTxnMarkersResponseTest {

    private static final long PRODUCER_ID_ONE = 1L;
    private static final long PRODUCER_ID_TWO = 2L;

    private static final TopicPartition TP_1 = new TopicPartition("topic", 1);
    private static final TopicPartition TP_2 = new TopicPartition("topic", 2);

    private static final Errors PID_ONE_ERROR = Errors.UNKNOWN_PRODUCER_ID;
    private static final Errors PID_TWO_ERROR = Errors.INVALID_PRODUCER_EPOCH;

    private static Map<Long, Map<TopicPartition, Errors>> errorMap;

    @BeforeEach
    public void setUp() {
        errorMap = new HashMap<>();
        errorMap.put(PRODUCER_ID_ONE, Collections.singletonMap(TP_1, PID_ONE_ERROR));
        errorMap.put(PRODUCER_ID_TWO, Collections.singletonMap(TP_2, PID_TWO_ERROR));
    }

    @Test
    public void testConstructor() {
        Map<Errors, Integer> expectedErrorCounts = new HashMap<>();
        expectedErrorCounts.put(Errors.UNKNOWN_PRODUCER_ID, 1);
        expectedErrorCounts.put(Errors.INVALID_PRODUCER_EPOCH, 1);
        WriteTxnMarkersResponse response = new WriteTxnMarkersResponse(errorMap);
        assertEquals(expectedErrorCounts, response.errorCounts());
        assertEquals(Collections.singletonMap(TP_1, PID_ONE_ERROR), response.errorsByProducerId().get(PRODUCER_ID_ONE));
        assertEquals(Collections.singletonMap(TP_2, PID_TWO_ERROR), response.errorsByProducerId().get(PRODUCER_ID_TWO));
    }
}
