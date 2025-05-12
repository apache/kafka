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
package org.apache.kafka.raft;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Random;

import static org.apache.kafka.raft.KafkaRaftClient.RETRY_BACKOFF_BASE_MS;
import static org.apache.kafka.raft.RaftUtil.binaryExponentialElectionBackoffMs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RaftUtilTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
    public void testExponentialBoundOfExponentialElectionBackoffMs(int retries) {
        Random mockedRandom = Mockito.mock(Random.class);
        int electionBackoffMaxMs = 1000;

        // test the bound of the method's first call to random.nextInt
        binaryExponentialElectionBackoffMs(electionBackoffMaxMs, RETRY_BACKOFF_BASE_MS, retries, mockedRandom);
        ArgumentCaptor<Integer> nextIntCaptor = ArgumentCaptor.forClass(Integer.class);
        Mockito.verify(mockedRandom, Mockito.times(2)).nextInt(nextIntCaptor.capture());
        List<Integer> allCapturedBounds = nextIntCaptor.getAllValues();
        int actualBound = allCapturedBounds.get(0);
        int expectedBound = (int) (2 * Math.pow(2, retries - 1));
        // after the 10th retry, the bound of the first call to random.nextInt will remain capped to
        // (RETRY_BACKOFF_BASE_MS * 2 << 10)=2048 to prevent overflow
        if (retries > 10) {
            expectedBound = 2048;
        }
        assertEquals(expectedBound, actualBound, "Incorrect bound for retries=" + retries);
    }

    // test that the return value of the method is capped to QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG + jitter
    // any exponential >= (1000 + jitter)/(RETRY_BACKOFF_BASE_MS)=21 will result in this cap
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 19, 20, 21, 2048})
    public void testExponentialElectionBackoffMsIsCapped(int exponential) {
        Random mockedRandom = Mockito.mock(Random.class);
        int electionBackoffMaxMs = 1000;
        // this is the max bound of the method's first call to random.nextInt
        int firstNextIntMaxBound = 2048;

        int jitterMs = 50;
        Mockito.when(mockedRandom.nextInt(firstNextIntMaxBound)).thenReturn(exponential);
        Mockito.when(mockedRandom.nextInt(RETRY_BACKOFF_BASE_MS)).thenReturn(jitterMs);

        int returnedBackoffMs = binaryExponentialElectionBackoffMs(electionBackoffMaxMs, RETRY_BACKOFF_BASE_MS, 11, mockedRandom);

        // verify nextInt was called on both expected bounds
        ArgumentCaptor<Integer> nextIntCaptor = ArgumentCaptor.forClass(Integer.class);
        Mockito.verify(mockedRandom, Mockito.times(2)).nextInt(nextIntCaptor.capture());
        List<Integer> allCapturedBounds = nextIntCaptor.getAllValues();
        assertEquals(firstNextIntMaxBound, allCapturedBounds.get(0));
        assertEquals(RETRY_BACKOFF_BASE_MS, allCapturedBounds.get(1));

        // finally verify the backoff returned is capped to electionBackoffMaxMs + jitterMs
        int backoffValueCap = electionBackoffMaxMs + jitterMs;
        if (exponential < 20) {
            assertEquals(RETRY_BACKOFF_BASE_MS * (exponential + 1), returnedBackoffMs);
            assertTrue(returnedBackoffMs < backoffValueCap);
        } else {
            assertEquals(backoffValueCap, returnedBackoffMs);
        }
    }
}
