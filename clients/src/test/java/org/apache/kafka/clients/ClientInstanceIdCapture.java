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
package org.apache.kafka.clients;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;

import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mockStatic;

/**
 * Asserts that a client generates a client instance ID in its constructor and hands it to whatever
 * builds its network client (KIP-1313).
 */
public class ClientInstanceIdCapture {

    private ClientInstanceIdCapture() {
    }

    /**
     * Asserts that the client passes a usable client instance ID to {@link ClientUtils}. The shorter
     * createNetworkClient overload delegates to the longer, so verifying the longer covers every caller.
     */
    public static void assertGenerated(Supplier<? extends AutoCloseable> clientFactory) {
        assertGenerated(ClientUtils.class,
            captor -> () -> ClientUtils.createNetworkClient(any(), any(), any(), captor.capture(), any(), any(),
                any(), any(), any(), anyInt(), anyInt(), any(), any(), any(), any(), any()),
            clientFactory);
    }

    /**
     * Asserts that the client passes a usable client instance ID to {@code factoryClass}. The
     * consumers do not call {@link ClientUtils} on the constructing thread, and a static mock only
     * covers the thread that created it, so they verify the call they do make there instead.
     */
    public static <T> void assertGenerated(Class<T> factoryClass,
                                           Function<ArgumentCaptor<Uuid>, MockedStatic.Verification> verification,
                                           Supplier<? extends AutoCloseable> clientFactory) {
        ArgumentCaptor<Uuid> captor = ArgumentCaptor.forClass(Uuid.class);
        try (MockedStatic<T> mocked = mockStatic(factoryClass, new CallsRealMethods())) {
            AutoCloseable client = clientFactory.get();
            try {
                mocked.verify(verification.apply(captor));
            } finally {
                Utils.closeQuietly(client, "client");
            }
        }
        Uuid clientInstanceId = captor.getValue();
        assertNotNull(clientInstanceId, "no client instance ID was passed to the network client");
        // KIP-1313 does not permit a reserved UUID; Uuid.randomUuid never returns one.
        assertFalse(Uuid.RESERVED.contains(clientInstanceId));
    }
}
