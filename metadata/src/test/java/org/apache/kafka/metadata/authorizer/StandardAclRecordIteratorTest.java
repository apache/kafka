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

package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.apache.kafka.metadata.authorizer.StandardAclWithIdTest.TEST_ACLS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class StandardAclRecordIteratorTest {
    @Test
    public void testIteration() {
        StandardAclRecordIterator iterator =
            new StandardAclRecordIterator(TEST_ACLS.iterator(), 2);
        assertTrue(iterator.hasNext());
        assertEquals(Arrays.asList(
            new ApiMessageAndVersion(TEST_ACLS.get(0).toRecord(), (short) 0),
            new ApiMessageAndVersion(TEST_ACLS.get(1).toRecord(), (short) 0)),
            iterator.next());
        assertEquals(Arrays.asList(
            new ApiMessageAndVersion(TEST_ACLS.get(2).toRecord(), (short) 0),
            new ApiMessageAndVersion(TEST_ACLS.get(3).toRecord(), (short) 0)),
            iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(Arrays.asList(
            new ApiMessageAndVersion(TEST_ACLS.get(4).toRecord(), (short) 0)),
            iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNoSuchElementException() {
        StandardAclRecordIterator iterator =
            new StandardAclRecordIterator(TEST_ACLS.iterator(), 2);
        iterator.next();
        iterator.next();
        iterator.next();
        assertThrows(NoSuchElementException.class, () -> iterator.next());
    }
}