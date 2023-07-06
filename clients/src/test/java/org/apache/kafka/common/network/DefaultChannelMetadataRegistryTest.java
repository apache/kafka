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

package org.apache.kafka.common.network;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultChannelMetadataRegistryTest {
    DefaultChannelMetadataRegistry defaultChannelMetadataRegistry;

    @BeforeEach
    void setup() {
        defaultChannelMetadataRegistry = new DefaultChannelMetadataRegistry();
    }

    @Test
    void testRegisterCipherInformationWithNullCipherInformationValue() {
        assertThrows(NullPointerException.class, () -> defaultChannelMetadataRegistry.registerCipherInformation(null));
    }

    @Test
    void testRegisterCipherInformation() {
        CipherInformation cipherInformation = new CipherInformation("cipher", "protocol");
        defaultChannelMetadataRegistry.registerCipherInformation(cipherInformation);
        assertEquals(cipherInformation, defaultChannelMetadataRegistry.cipherInformation());

        // re-registering the cipher information should override the previous registered value
        CipherInformation cipherInformation2 = new CipherInformation("cipher-2", "protocol-2");
        defaultChannelMetadataRegistry.registerCipherInformation(cipherInformation2);
        assertEquals(cipherInformation2, defaultChannelMetadataRegistry.cipherInformation());

        // invoking defaultChannelMetadataRegistry.close() should set the cipher to null
        defaultChannelMetadataRegistry.close();
        assertNull(defaultChannelMetadataRegistry.cipherInformation());
    }

    @Test
    void testRegisterClientInformation() {
        ClientInformation clientInformation = new ClientInformation("software name", "software version");
        defaultChannelMetadataRegistry.registerClientInformation(clientInformation);
        assertEquals(clientInformation, defaultChannelMetadataRegistry.clientInformation());

        // re-registering the client information should override the previous registered value
        ClientInformation clientInformation2 = new ClientInformation("software name", "software version");
        defaultChannelMetadataRegistry.registerClientInformation(clientInformation2);
        assertEquals(clientInformation2, defaultChannelMetadataRegistry.clientInformation());

        // invoking defaultChannelMetadataRegistry.close() should set the client information to null
        defaultChannelMetadataRegistry.close();
        assertNull(defaultChannelMetadataRegistry.clientInformation());
    }
}