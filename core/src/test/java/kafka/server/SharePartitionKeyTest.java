/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class SharePartitionKeyTest {

    @Test
    public void testEqualsFunctionality() {
        SharePartitionKey sharePartitionKey_1 = new SharePartitionKey("mock-group-1",
                new Uuid(0L, 1L), 0);
        SharePartitionKey sharePartitionKey_2 = new SharePartitionKey("mock-group-2",
                new Uuid(0L, 1L), 0);
        SharePartitionKey sharePartitionKey_3 = new SharePartitionKey("mock-group-1",
                new Uuid(1L, 1L), 0);
        SharePartitionKey sharePartitionKey_4 = new SharePartitionKey("mock-group-1",
                new Uuid(0L, 1L), 1);
        SharePartitionKey sharePartitionKey_5 = new SharePartitionKey("mock-group-1",
                new Uuid(0L, 0L), 1);
        SharePartitionKey sharePartitionKey_1_copy = new SharePartitionKey("mock-group-1",
                new Uuid(0L, 1L), 0);

        assertEquals(sharePartitionKey_1, sharePartitionKey_1_copy);
        assertNotEquals(sharePartitionKey_1, sharePartitionKey_2);
        assertNotEquals(sharePartitionKey_1, sharePartitionKey_3);
        assertNotEquals(sharePartitionKey_1, sharePartitionKey_4);
        assertNotEquals(sharePartitionKey_1, sharePartitionKey_5);
        assertNotEquals(sharePartitionKey_1, null);
    }
}
