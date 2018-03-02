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

package org.apache.kafka.trogdor.workload;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;


public class PayloadTest {

    @Test
    public void testDefaultPayload() {
        Payload payload = new Payload();
        assertNull(payload.nextKey());
        byte[] value = payload.nextValue();
        assertEquals(Payload.DEFAULT_MESSAGE_SIZE, value.length);

        // make sure that each time we produce a different value (except if compression rate is 0)
        assertNotEquals(value, payload.nextKey());
    }

    @Test
    public void testMessageSize() {
        final int size = 200;
        Payload payload = new Payload(size);
        byte[] value = payload.nextValue();
        assertEquals(size, value.length);
    }
}
