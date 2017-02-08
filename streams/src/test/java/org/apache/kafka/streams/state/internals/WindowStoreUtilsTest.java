/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.StateSerdes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WindowStoreUtilsTest {
    protected StateSerdes<String, String> serdes = new StateSerdes<>("dummy", new Serdes.StringSerde(), new Serdes.StringSerde());

    @Test
    public void testSerialization() throws Exception {
        final String key = "key1";
        final long timestamp = 99L;
        final int seqNum = 3;
        Bytes bytes = WindowStoreUtils.toBinaryKey(key, timestamp, seqNum, serdes);
        final String parsedKey = WindowStoreUtils.keyFromBinaryKey(bytes.get(), serdes);
        final long parsedTs = WindowStoreUtils.timestampFromBinaryKey(bytes.get());
        final int parsedSeqNum = WindowStoreUtils.sequenceNumberFromBinaryKey(bytes.get());
        assertEquals(key, parsedKey);
        assertEquals(timestamp, parsedTs);
        assertEquals(seqNum, parsedSeqNum);
    }
}