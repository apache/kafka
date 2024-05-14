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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BrokerRegistrationRequestTest {
    private static Stream<Arguments> BrokerRegistrationRequestVersions() {
        return IntStream.range(BrokerRegistrationRequestData.LOWEST_SUPPORTED_VERSION,
            BrokerRegistrationRequestData.HIGHEST_SUPPORTED_VERSION + 1).mapToObj(version -> Arguments.of((short) version));
    }

    @ParameterizedTest
    @MethodSource("BrokerRegistrationRequestVersions")
    public void testBasicBuild(short version) {
        Uuid incarnationId = Uuid.randomUuid();
        BrokerRegistrationRequestData data = new BrokerRegistrationRequestData();
        data.setBrokerId(0)
            .setIsMigratingZkBroker(false)
            .setClusterId("test")
            .setFeatures(new BrokerRegistrationRequestData.FeatureCollection())
            .setIncarnationId(incarnationId)
            .setListeners(new BrokerRegistrationRequestData.ListenerCollection())
            .setRack("a")
            .setPreviousBrokerEpoch(1L);
        BrokerRegistrationRequest.Builder builder = new BrokerRegistrationRequest.Builder(data);
        BrokerRegistrationRequest request = builder.build(version);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = request.data().size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(buf);
        request.data().write(byteBufferAccessor, cache, version);

        BrokerRegistrationRequestData data2 = new BrokerRegistrationRequestData();
        buf.flip();
        data2.read(byteBufferAccessor, version);

        assertEquals(0, data2.brokerId(), "Unexpected broker ID in " + data2);
        assertEquals("test", data2.clusterId(), "Unexpected cluster ID in " + data2);
        assertEquals(incarnationId, data2.incarnationId(), "Unexpected incarnation ID in " + data2);
        assertEquals("a", data2.rack(), "Unexpected rack in " + data2);
        if (version >= 3) {
            assertEquals(1, data2.previousBrokerEpoch(), "Unexpected previousBrokerEpoch in " + data2);
        } else {
            assertEquals(-1, data2.previousBrokerEpoch(), "Unexpected previousBrokerEpoch in " + data2);
        }
    }
}
