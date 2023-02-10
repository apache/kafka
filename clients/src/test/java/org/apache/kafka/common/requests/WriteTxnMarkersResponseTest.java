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

    private static long producerIdOne = 1L;
    private static long producerIdTwo = 2L;

    private static TopicPartition tp1 = new TopicPartition("topic", 1);
    private static TopicPartition tp2 = new TopicPartition("topic", 2);

    private static Errors pidOneError = Errors.UNKNOWN_PRODUCER_ID;
    private static Errors pidTwoError = Errors.INVALID_PRODUCER_EPOCH;

    private static Map<Long, Map<TopicPartition, Errors>> errorMap;

    @BeforeEach
    public void setUp() {
        errorMap = new HashMap<>();
        errorMap.put(producerIdOne, Collections.singletonMap(tp1, pidOneError));
        errorMap.put(producerIdTwo, Collections.singletonMap(tp2, pidTwoError));
    }

    @Test
    public void testConstructor() {
        Map<Errors, Integer> expectedErrorCounts = new HashMap<>();
        expectedErrorCounts.put(Errors.UNKNOWN_PRODUCER_ID, 1);
        expectedErrorCounts.put(Errors.INVALID_PRODUCER_EPOCH, 1);
        WriteTxnMarkersResponse response = new WriteTxnMarkersResponse(errorMap);
        assertEquals(expectedErrorCounts, response.errorCounts());
        assertEquals(Collections.singletonMap(tp1, pidOneError), response.errorsByProducerId().get(producerIdOne));
        assertEquals(Collections.singletonMap(tp2, pidTwoError), response.errorsByProducerId().get(producerIdTwo));
    }
}
