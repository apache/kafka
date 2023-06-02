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

package org.apache.kafka.image.node;

import org.apache.kafka.image.node.printer.MetadataNodeRedactionCriteria.Disabled;
import org.apache.kafka.image.node.printer.NodeStringifier;
import org.apache.kafka.metadata.ScramCredentialData;
import org.apache.kafka.server.util.MockRandom;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ScramCredentialDataNodeTest {
    private static final ScramCredentialData DATA;

    static {
        MockRandom mockRandom = new MockRandom();
        byte[] salt = new byte[16];
        mockRandom.nextBytes(salt);
        byte[] storedKey = new byte[16];
        mockRandom.nextBytes(storedKey);
        byte[] serverKey = new byte[16];
        mockRandom.nextBytes(serverKey);
        DATA = new ScramCredentialData(salt, storedKey, serverKey, 16);
    }

    @Test
    public void testPrintRedacted() {
        NodeStringifier stringifier = new NodeStringifier();
        new ScramCredentialDataNode(DATA).print(stringifier);
        assertEquals("ScramCredentialData(" +
            "salt=[redacted], " +
            "storedKey=[redacted], " +
            "serverKey=[redacted], " +
            "iterations=[redacted])", stringifier.toString());
    }

    @Test
    public void testPrintUnredacted() {
        NodeStringifier stringifier = new NodeStringifier(Disabled.INSTANCE);
        new ScramCredentialDataNode(DATA).print(stringifier);
        assertEquals("ScramCredentialData(" +
            "salt=4f1d6ea31e58c5ad3aaeb3266f55cce6, " +
            "storedKey=3cfa1c3421b512d1d1dfc3355138b4ad, " +
            "serverKey=2d9781209073e8d03aee3cbc63a1d4ca, " +
            "iterations=16)", stringifier.toString());
    }
}
